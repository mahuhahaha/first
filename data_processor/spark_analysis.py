import os
os.environ["PYSPARK_PYTHON"] = r"D:\anaconda\envs\study\python.exe"
os.environ["PYSPARK_DRIVER_PYTHON"] = r"D:\anaconda\envs\study\python.exe"

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_unixtime, to_date, hour, explode, col, udf, length, avg
from pyspark.sql.types import ArrayType, StringType
import jieba
import numpy as np
import pickle
from tensorflow.keras.models import load_model
from tensorflow.keras.preprocessing.sequence import pad_sequences
import pkuseg

# 1. 读取MySQL表为DataFrame
def read_danmu_from_mysql(table_name, user, password, database, host='localhost', port=3306):
    spark = SparkSession.builder.appName("ReadMySQL").getOrCreate()
    url = f"jdbc:mysql://{host}:{port}/{database}?useUnicode=true&characterEncoding=UTF-8"
    df = spark.read.format("jdbc") \
        .option("url", url) \
        .option("dbtable", table_name) \
        .option("user", user) \
        .option("password", password) \
        .option("driver", "com.mysql.cj.jdbc.Driver") \
        .load()
    return df

# 2. 统计每集弹幕数量
def count_danmu_per_episode_spark(df):
    result = df.groupBy("episode").count().orderBy("episode")
    return result.toPandas()

# 3. 弹幕时间分布（日）
def danmu_daily_distribution(df):
    df = df.withColumn("date", to_date(from_unixtime(df.create_time)))
    result = df.groupBy("date").count().orderBy("date")
    return result.toPandas()

# 4. 弹幕时间分布（小时）
def danmu_hourly_distribution(df):
    df = df.withColumn("hour", hour(from_unixtime(df.create_time)))
    result = df.groupBy("hour").count().orderBy("hour")
    return result.toPandas()

# 5. 词云（高频词统计）
# --- Spark UDF 分词实现（pkuseg版本） ---
# import jieba
# jieba.initialize()  # jieba预热
# def jieba_cut(text):
#     return list(jieba.cut(text)) if text else []
# jieba_cut_udf = udf(jieba_cut, ArrayType(StringType()))

seg = pkuseg.pkuseg()
def pkuseg_cut(text):
    return seg.cut(text) if text else []
pkuseg_cut_udf = udf(pkuseg_cut, ArrayType(StringType()))

def danmu_wordcloud(df, top_n=100):
    # df = df.withColumn("words", jieba_cut_udf(col("content")))  # jieba版本
    df = df.withColumn("words", pkuseg_cut_udf(col("content")))  # pkuseg版本
    words_df = df.select(explode(col("words")).alias("word"))
    words_df = words_df.filter(words_df.word.isNotNull() & (length(words_df.word) > 1))
    result = words_df.groupBy("word").count().orderBy(col("count").desc()).limit(top_n)
    return result.toPandas()

# 6. 弹幕内容长度分布
def danmu_length_distribution(df):
    df = df.withColumn("content_length", length(df.content))
    result = df.groupBy("episode").agg(avg("content_length").alias("avg_length")).orderBy("episode")
    return result.toPandas()

# 7. 情感分析（批量）
# 加载模型和字典（只加载一次）
model = load_model(r'./ml_models/sentiment_model.h5')
with open(r'./ml_models/word_dictionary.pk', 'rb') as f:
    word_dict = pickle.load(f)

def preprocess(text, input_shape=180):
    words = list(jieba.cut(text))
    x = [word_dict.get(w, 0) for w in words]
    x = pad_sequences([x], maxlen=input_shape, padding='post', value=0)
    return x

def predict_sentiment(text):
    if not text:
        return "未知"
    x = preprocess(text)
    pred = model.predict(x)
    label = np.argmax(pred)
    # 假设0=负面, 1=中性, 2=正面（根据你模型实际标签）
    if label == 0:
        return "正面"
    else:
        return "负面"

sentiment_udf = udf(predict_sentiment, StringType())

def danmu_sentiment_analysis(df):
    df = df.withColumn("sentiment", sentiment_udf(col("content")))
    result = df.groupBy("sentiment").count().orderBy("sentiment")
    return result.toPandas()

# --- Spark 版本的 info 表相关操作 ---
def fetch_anime_list_spark(spark, user, password, database, selected_type='全部', host='localhost', port=3306):
    """
    用 Spark 读取 info 表，获取动漫列表。
    """
    df = read_danmu_from_mysql('info', user, password, database, host, port)
    if selected_type == '全部':
        return df.toPandas().to_dict(orient='records')
    else:
        return df.filter(df.type == selected_type).toPandas().to_dict(orient='records')


def fetch_anime_info_spark(spark, user, password, database, anime_title, host='localhost', port=3306):
    """
    用 Spark 读取 info 表，获取指定动漫的详细信息。
    """
    df = read_danmu_from_mysql('info', user, password, database, host, port)
    result = df.filter(df.title == anime_title).toPandas().to_dict(orient='records')
    return result[0] if result else None


def search_anime_by_keyword_spark(spark, user, password, database, keyword, host='localhost', port=3306):
    """
    用 Spark 读取 info 表，根据关键词模糊搜索动漫。
    """
    df = read_danmu_from_mysql('info', user, password, database, host, port)
    result = df.filter(df.title.contains(keyword)).toPandas().to_dict(orient='records')
    return result

# --- Spark 版本的弹幕表相关操作 ---
def get_episode_list_spark(df):
    """
    获取弹幕表中所有集数列表。
    """
    return [row['episode'] for row in df.select('episode').distinct().orderBy('episode').toPandas().to_dict(orient='records')]


def calculate_average_danmu_length_per_episode_spark(df):
    """
    计算每集弹幕的平均长度。
    """
    from pyspark.sql.functions import avg, length
    df = df.withColumn('content_length', length(df.content))
    result = df.groupBy('episode').agg(avg('content_length').alias('avg_danmu_length')).orderBy('episode')
    return result.toPandas()


def read_danmu_data_by_date_and_episode_spark(df, start_date=None, end_date=None, episode='all'):
    """
    读取弹幕数据，可按日期和集数筛选。
    """
    from pyspark.sql.functions import from_unixtime, to_date
    if start_date and end_date:
        df = df.withColumn('date', to_date(from_unixtime(df.create_time)))
        df = df.filter((df.date >= start_date) & (df.date <= end_date))
    if episode != 'all':
        df = df.filter(df.episode == int(episode))
    return df

# read_danmu_from_mysql(
#     table_name="仙逆",
#     user="root",
#     password="tpy520",
#     database="dan"
# )