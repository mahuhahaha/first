import pickle
import re
import jieba
import mysql.connector
import numpy as np
from keras.src.utils import pad_sequences
from mysql.connector import Error
import pandas as pd
from collections import Counter
from tensorflow.keras.models import load_model
from tensorflow.keras.preprocessing.sequence import pad_sequences
import random
import hashlib
import json
import os

# 数据库连接配置
DB_CONFIG = {
    'host': 'localhost',  # 数据库主机地址
    'user': 'root',  # 数据库用户名
    'password': 'tpy520',  # 数据库密码
    'database': 'dan',  # 数据库名
    'charset': 'utf8mb4'  # 指定字符集为 utf8mb4
}

def count_danmu_per_episode(table_name, episode=None):
    """
    统计每一集的弹幕数量。

    参数:
        table_name (str): 数据库中要查询的表名称。
        episode (str, optional): 需要统计的集数。可以是单集的集数（如 "1"），也可以是 "all" 表示统计所有集数。默认为 None，表示统计所有集数。

    返回:
        dict: 包含每一集的弹幕数量的字典。
    """
    episode = str(episode)
    result_dict = {}
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor(dictionary=True)

        base_query = f"SELECT episode, COUNT(*) AS danmu_count FROM {table_name}"
        if episode is not None and episode.lower() != "all":
            query = f"{base_query} WHERE episode = {episode} GROUP BY episode"
        else:
            query = f"{base_query} GROUP BY episode ORDER BY episode"

        cursor.execute(query)
        results = cursor.fetchall()

        for row in results:
            result_dict[row['episode']] = pd.DataFrame([row])  # 返回 DataFrame

        print(f"表 {table_name} 中每一集的弹幕数量统计完成")
    except Error as e:
        print(f"统计每一集的弹幕数量时出错：{e}")
    finally:
        if conn.is_connected():
            cursor.close()
            conn.close()

    return result_dict


def read_danmu_data_by_date_and_episode(table_name, start_date=None, end_date=None, episode="all"):
    try:
        connection = mysql.connector.connect(**DB_CONFIG)
        cursor = connection.cursor(dictionary=True)

        # 构造查询语句
        if episode == "all":
            if start_date and end_date:
                query = f"SELECT * FROM {table_name} WHERE FROM_UNIXTIME(create_time) BETWEEN %s AND %s"
                cursor.execute(query, (start_date, end_date))
            elif start_date:
                query = f"SELECT * FROM {table_name} WHERE FROM_UNIXTIME(create_time) = %s"
                cursor.execute(query, (start_date,))
            else:
                query = f"SELECT * FROM {table_name}"
                cursor.execute(query)
        else:
            if start_date and end_date:
                query = f"SELECT * FROM {table_name} WHERE episode = %s AND FROM_UNIXTIME(create_time) BETWEEN %s AND %s"
                cursor.execute(query, (episode, start_date, end_date))
            elif start_date:
                query = f"SELECT * FROM {table_name} WHERE episode = %s AND FROM_UNIXTIME(create_time) = %s"
                cursor.execute(query, (episode, start_date))
            else:
                query = f"SELECT * FROM {table_name} WHERE episode = %s"
                cursor.execute(query, (episode,))

        # 获取所有数据
        data = cursor.fetchall()

        # 按集数分组
        grouped_data = {}
        for row in data:
            episode_number = row['episode']
            if episode_number not in grouped_data:
                grouped_data[episode_number] = []
            grouped_data[episode_number].append(row)

        # 将每个集数的数据转换为 DataFrame
        for episode_number in grouped_data.keys():
            grouped_data[episode_number] = pd.DataFrame(grouped_data[episode_number])

        return grouped_data

    except Error as e:
        print(f"Error while reading danmu data: {e}")
        return {}
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()


def calculate_average_danmu_length_per_episode(table_name, episode=None):
    """
    计算每集弹幕的平均长度。

    参数:
        table_name (str): 数据库中要查询的表名称。
        episode (str, optional): 需要计算的集数。可以是单集的集数（如 "1"），也可以是 "all" 表示计算所有集数。默认为 None，表示计算所有集数。

    返回:
        dict: 包含每集弹幕平均长度的字典。
    """
    result_dict = {}
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor(dictionary=True)

        base_query = f"SELECT episode, AVG(CHAR_LENGTH(content)) AS avg_danmu_length FROM {table_name}"
        if episode is not None and episode.lower() != "all":
            episode = str(episode)
            query = f"{base_query} WHERE episode = {episode} GROUP BY episode"
        else:
            query = f"{base_query} GROUP BY episode ORDER BY episode"

        cursor.execute(query)
        results = cursor.fetchall()

        for row in results:
            result_dict[row['episode']] = pd.DataFrame([row])  # 返回 DataFrame

        print(f"表 {table_name} 中每集弹幕的平均长度计算完成")
    except Error as e:
        print(f"计算每集弹幕平均长度时出错：{e}")
    finally:
        if conn.is_connected():
            cursor.close()
            conn.close()

    return result_dict


def get_episode_list_from_db(table_name):
    result_dict = {}
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor(dictionary=True)

        query = f"SELECT DISTINCT episode FROM {table_name} ORDER BY episode"
        cursor.execute(query)
        results = cursor.fetchall()

        for row in results:
            result_dict[row['episode']] = pd.DataFrame()  # 返回空的 DataFrame

        print(f"表 {table_name} 中的集数列表查询完成")
    except Error as e:
        print(f"查询集数列表时出错：{e}")
    finally:
        if conn.is_connected():
            cursor.close()
            conn.close()

    return result_dict


# 数据分析和处理函数
def analyze_danmu_time_distribution_in_video_area_chart(danmu_data, video_duration, time_column='time_offset'):
    """
    分析单个视频中弹幕的时间分布，并返回数据。

    参数:
        danmu_data (pd.DataFrame): 单个视频的弹幕数据。
        video_duration (int): 视频的总时长（单位：分钟）。
        time_column (str): 时间偏移量列的名称，默认为'time_offset'。
    """
    if danmu_data is None or time_column not in danmu_data.columns:
        print(f"数据中缺少列'{time_column}'，无法进行时间分布分析。")
        return None

    time_offsets = danmu_data[time_column]
    time_offsets_in_minutes = time_offsets / 60000  # 1分钟 = 60000毫秒

    time_bins = np.arange(0, 31, 1)  # 从0到30分钟，每分钟一个时间点
    danmu_counts, _ = np.histogram(time_offsets_in_minutes, bins=time_bins)

    df = pd.DataFrame({
        'episodeTime': time_bins[:-1],
        'count': danmu_counts
    })

    return df


def count_cipin(text):
    """
    统计词频并过滤指定词汇和标点符号。

    参数:
        text (str): 要分析的文本。

    返回:
        tuple: 包含过滤后的词列表和词频统计结果。
    """
    # 使用 jieba 进行中文分词
    words = jieba.cut(text, cut_all=False)

    # 过滤指定词汇和标点符号
    stopwords = {
        '的', '是', '和', '了', '在', '我', '有', '不', '人', '都', '一', '一个', '上', '也', '很', '到', '说', '要', '去',
        '你', '会', '着', '没有', '看', '好', '自己', '这', '就', '吧', '我们', '被', '过', '那', '把', '在', '里', '是', '个',
        '对', '上', '他', '但', '它', '而', '所以', '自己', '等', '与', '同', '及', '其', '如', '以', '下', '来',
        '更', '从', '做', '当', '还', '比', '将', '又', '再', '可', '就', '只', '些', '些', '着', '了', '的', '着',
        '这个', '那个', '这么', '那么', '那样', '这样', '一些', '一些', '什么', '时候', '现在', '然后', '就是',
        '因为', '为了', '关于', '于是', '但是', '而且', '如果', '虽然', '只要', '只有', '还是', '还是', '不过',
        '例如', '比如', '即','哈哈','哈哈哈','哈','哈哈哈哈','哈哈哈哈哈'
    }
    filtered_words = [word for word in words if word not in stopwords and not re.match(r'\W', word)]

    # 统计词频
    word_freq = Counter(filtered_words)

    # 将词频统计结果保存到列表
    word_freq_list = [(word, freq) for word, freq in word_freq.items()]

    return word_freq_list


def analyze_time_distribution_in_folder(time_data_dict):
    if not isinstance(time_data_dict, dict):
        print("输入数据必须是一个字典")
        return None

    all_hourly_counts = {hour: 0 for hour in range(24)}

    for episode_number, danmu_data in time_data_dict.items():
        if not isinstance(danmu_data, pd.DataFrame):
            print(f"集数 {episode_number} 的数据不是 DataFrame")
            continue

        if 'create_time' not in danmu_data.columns:
            print(f"集数 {episode_number} 的数据中缺少 'create_time' 列")
            continue

        # 确保 create_time 是 datetime 类型
        danmu_data['create_time'] = pd.to_datetime(danmu_data['create_time'], unit='s', errors='coerce')

        # 检查是否有无效的时间数据
        if danmu_data['create_time'].isnull().any():
            print(f"集数 {episode_number} 中存在无效的时间数据，已忽略")
            danmu_data = danmu_data.dropna(subset=['create_time'])

        # 提取小时信息
        danmu_data['hour'] = danmu_data['create_time'].dt.hour

        # 统计每个小时的弹幕数量
        hourly_counts = danmu_data['hour'].value_counts().sort_index()

        # 累加到总统计中
        for hour in all_hourly_counts:
            all_hourly_counts[hour] += hourly_counts.get(hour, 0)

    # 创建结果 DataFrame
    summary_data = pd.DataFrame(list(all_hourly_counts.items()), columns=['Hour', 'Count'])

    return summary_data


def analyze_time_distribution(time_data_dict):
    """
    分析弹幕时间分布（按小时统计）

    :param time_data_dict: 字典，键为集数编号，值为包含弹幕数据的 DataFrame
    :return: DataFrame，包含每个小时的弹幕数量统计
    """
    if not isinstance(time_data_dict, dict):
        print("输入数据必须是一个字典")
        return None

    all_hourly_counts = {hour: 0 for hour in range(24)}

    for episode_number, danmu_data in time_data_dict.items():
        if not isinstance(danmu_data, pd.DataFrame):
            print(f"集数 {episode_number} 的数据不是 DataFrame")
            continue

        if 'create_time' not in danmu_data.columns:
            print(f"集数 {episode_number} 的数据中缺少 'create_time' 列")
            continue

        # 确保 create_time 是 datetime 类型
        danmu_data['create_time'] = pd.to_datetime(danmu_data['create_time'], unit='s', errors='coerce')

        # 检查是否有无效的时间数据
        if danmu_data['create_time'].isnull().any():
            print(f"集数 {episode_number} 中存在无效的时间数据，已忽略")
            danmu_data = danmu_data.dropna(subset=['create_time'])

        # 提取小时信息
        danmu_data['hour'] = danmu_data['create_time'].dt.hour

        # 统计每个小时的弹幕数量
        hourly_counts = danmu_data['hour'].value_counts().sort_index()

        # 累加到总统计中
        for hour in all_hourly_counts:
            all_hourly_counts[hour] += hourly_counts.get(hour, 0)

    # 创建结果 DataFrame
    summary_data = pd.DataFrame(list(all_hourly_counts.items()), columns=['Hour', 'Count'])

    return summary_data


def analyze_weekly_danmu_distribution(danmu_data_dict):
    if not isinstance(danmu_data_dict, dict):
        print("输入数据必须是一个字典")
        return None

    # 初始化每天的弹幕总数
    daily_totals = {day: 0 for day in range(7)}

    for episode_number, danmu_data in danmu_data_dict.items():
        if not isinstance(danmu_data, pd.DataFrame):
            print(f"集数 {episode_number} 的数据不是 DataFrame")
            continue

        if 'create_time' not in danmu_data.columns:
            print(f"集数 {episode_number} 的数据中缺少 'create_time' 列")
            continue

        # 确保 create_time 是 datetime 类型
        # 明确指定单位为秒 (unit='s')
        danmu_data['create_time'] = pd.to_datetime(danmu_data['create_time'], unit='s', errors='coerce')

        # 检查是否有无效的时间数据
        if danmu_data['create_time'].isnull().any():
            print(f"集数 {episode_number} 中存在无效的时间数据，已忽略")
            danmu_data = danmu_data.dropna(subset=['create_time'])

        # 计算星期几（0=周一, 6=周日）
        danmu_data['day_of_week'] = danmu_data['create_time'].dt.dayofweek

        # 统计每天的弹幕数量
        daily_counts = danmu_data['day_of_week'].value_counts().sort_index()

        # 累加到总统计中
        for day in range(7):
            if day in daily_counts.index:
                daily_totals[day] += daily_counts[day]

    # 映射星期几到中文
    days = ['周一', '周二', '周三', '周四', '周五', '周六', '周日']
    totals = [daily_totals[day] for day in range(7)]

    # 创建结果 DataFrame
    df = pd.DataFrame({
        '星期': days,
        '弹幕总量': totals
    })

    return df

def preprocess_sentiment_danmu_data(df, text_column='content'):
    """
    预处理弹幕数据，提取文本内容。
    参数:
        df (pd.DataFrame): 弹幕数据。
        text_column (str): 弹幕文本内容所在的列名，默认为 'content'。

    返回:
        list: 包含所有弹幕文本的列表。
    """
    if df is None or text_column not in df.columns:
        print(f"数据中缺少列 '{text_column}'，无法进行预处理。")
        return []

    danmu_texts = df[text_column].tolist()
    print(f"成功提取 {len(danmu_texts)} 条弹幕文本")
    return danmu_texts

def get_cache_path(text_hash):
    """获取缓存文件路径"""
    cache_dir = "cache/sentiment"
    os.makedirs(cache_dir, exist_ok=True)
    return os.path.join(cache_dir, f"{text_hash}.json")

def analyze_sentiment_with_cache(danmu_texts, model_path, tokenizer_path, use_cache=True):
    """
    带缓存的情感分析，可以显著提高重复分析的速度
    
    参数:
        danmu_texts (list): 弹幕文本列表
        model_path (str): 模型文件路径
        tokenizer_path (str): 词汇表文件路径
        use_cache (bool): 是否使用缓存，默认True
    
    返回:
        dict: 情感分析结果
    """
    if not use_cache:
        return analyze_sentiment(danmu_texts, model_path, tokenizer_path)
    
    # 计算文本哈希（基于文本内容和长度）
    text_content = "".join(danmu_texts)
    text_hash = hashlib.md5((text_content + str(len(danmu_texts))).encode()).hexdigest()
    
    # 检查缓存
    cache_path = get_cache_path(text_hash)
    if os.path.exists(cache_path):
        try:
            with open(cache_path, 'r', encoding='utf-8') as f:
                cached_result = json.load(f)
            print("使用缓存的情感分析结果")
            return cached_result
        except Exception as e:
            print(f"读取缓存失败: {e}")
    
    # 执行情感分析
    result = analyze_sentiment(danmu_texts, model_path, tokenizer_path)
    
    # 保存到缓存
    try:
        with open(cache_path, 'w', encoding='utf-8') as f:
            json.dump(result, f, ensure_ascii=False, indent=2)
        print("情感分析结果已缓存")
    except Exception as e:
        print(f"保存缓存失败: {e}")
    
    return result

def analyze_sentiment(danmu_texts, model_path, tokenizer_path, batch_size=1000):
    """
    使用训练好的模型对弹幕数据进行情感分析（优化版本，使用批处理提高速度）。

    参数:
        danmu_texts (list): 弹幕文本列表。
        model_path (str): 模型文件路径。
        tokenizer_path (str): 词汇表文件路径。
        batch_size (int): 批处理大小，默认1000。

    返回:
        dict: 包含正面和负面情感的比例，以及各自出现次数最多的10条弹幕。
    """
    try:
        model = load_model(model_path)
        print("模型加载成功")

        with open(tokenizer_path, 'rb') as f:
            word_dict = pickle.load(f)  # 加载词汇表字典

        # 批量处理文本
        all_predictions = []
        total_batches = (len(danmu_texts) + batch_size - 1) // batch_size
        print(f"开始批量处理，共 {len(danmu_texts)} 条弹幕，分 {total_batches} 批处理")
        
        for i in range(0, len(danmu_texts), batch_size):
            batch_texts = danmu_texts[i:i + batch_size]
            batch_num = i // batch_size + 1
            print(f"处理第 {batch_num}/{total_batches} 批，包含 {len(batch_texts)} 条弹幕")
            
            # 批量转换序列
            sequences = []
            for text in batch_texts:
                seq = [word_dict[word] for word in text if word in word_dict]
                sequences.append(seq)

            data = pad_sequences(sequences, maxlen=100)
            predictions = model.predict(data, verbose=0)  # 关闭详细输出，提高速度
            all_predictions.extend(np.argmax(predictions, axis=1))

        # 统计积极和消极弹幕及其出现次数
        positive_counter = Counter()
        negative_counter = Counter()
        for text, label in zip(danmu_texts, all_predictions):
            if label == 1:
                positive_counter[text] += 1
            else:
                negative_counter[text] += 1

        positive_count = sum(positive_counter.values())
        negative_count = sum(negative_counter.values())
        total_count = positive_count + negative_count

        sentiment_ratio = {
            'positive': positive_count / total_count if total_count > 0 else 0,
            'negative': negative_count / total_count if total_count > 0 else 0
        }

        # 取出现次数最多的10条积极和消极弹幕
        top_positive = [item[0] for item in positive_counter.most_common(20)]
        top_negative = [item[0] for item in negative_counter.most_common(20)]

        print(f"情感分析完成，共处理 {len(danmu_texts)} 条弹幕")
        print(f"正面弹幕: {positive_count} 条 ({sentiment_ratio['positive']:.2%})")
        print(f"负面弹幕: {negative_count} 条 ({sentiment_ratio['negative']:.2%})")
        
        return {
            'sentiment_ratio': sentiment_ratio,
            'random_positive_comments': top_positive,
            'random_negative_comments': top_negative
        }

    except Exception as e:
        print(f"情感分析时出错：{e}")
        return {}