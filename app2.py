import functools
import pandas as pd
from flask import Flask, jsonify, request
from flask_cors import CORS
from data_processor.spark_analysis import *

app = Flask(__name__)
CORS(app)

# Spark参数
SPARK_USER = 'root'
SPARK_PASSWORD = 'tpy520'
SPARK_DATABASE = 'dan'
SPARK_HOST = 'localhost'
SPARK_PORT = 3306

@app.route('/get_anime_list')
def get_anime_list():
    try:
        selected_type = request.args.get('type', '全部')
        anime_list = fetch_anime_list_spark(None, SPARK_USER, SPARK_PASSWORD, SPARK_DATABASE, selected_type, SPARK_HOST, SPARK_PORT)
        if anime_list:
            return jsonify(anime_list)
        else:
            return jsonify({"error": "未找到动漫信息"}), 404
    except Exception as e:
        print(f"Error while fetching anime list: {e}")
        return jsonify({"error": "数据库连接错误"}), 500

@app.route('/search_anime', methods=['POST'])
def search_anime():
    keyword = request.json.get('keyword')
    selected_type = request.json.get('type', '全部')
    if not keyword:
        return jsonify({"error": "关键词不能为空"}), 400
    try:
        anime_list = search_anime_by_keyword_spark(None, SPARK_USER, SPARK_PASSWORD, SPARK_DATABASE, keyword, SPARK_HOST, SPARK_PORT)
        if selected_type != '全部':
            anime_list = [anime for anime in anime_list if anime['type'] == selected_type]
        return jsonify(anime_list)
    except Exception as e:
        print(f"Error while searching anime: {e}")
        return jsonify({"error": "搜索失败"}), 500

@app.route('/get_anime_info', methods=['POST'])
def get_anime_info():
    anime_title = request.json.get('anime_title')
    if not anime_title:
        return jsonify({"error": "动漫标题不能为空"}), 400
    try:
        anime_info = fetch_anime_info_spark(None, SPARK_USER, SPARK_PASSWORD, SPARK_DATABASE, anime_title, SPARK_HOST, SPARK_PORT)
        if anime_info:
            return jsonify(anime_info)
        else:
            return jsonify({"error": "未找到指定的动漫信息"}), 404
    except Exception as e:
        print(f"Error while fetching anime info: {e}")
        return jsonify({"error": "获取动漫信息失败"}), 500

@app.route('/get_danmu_stats', methods=['POST'])
def get_danmu_stats():
    data = request.get_json()
    anime_title = data.get('anime_title')
    if not anime_title:
        return jsonify({"error": "动漫标题不能为空"}), 400
    table_name = anime_title.replace(" ", "_")
    table_name = ''.join(e for e in table_name if e.isalnum() or e == '_')
    df = read_danmu_from_mysql(table_name, SPARK_USER, SPARK_PASSWORD, SPARK_DATABASE, SPARK_HOST, SPARK_PORT)
    result_df = count_danmu_per_episode_spark(df)
    result_list = []
    for _, row in result_df.iterrows():
        episode = int(row['episode']) if not pd.isnull(row['episode']) else 0
        danmu_count = int(row['count']) if not pd.isnull(row['count']) else 0
        result_list.append({'集数': episode, '弹幕数量': danmu_count})
    print('get_danmu_stats 返回数据:', result_list)
    return jsonify(result_list)

@app.route('/get_day_danmu_time_distribution', methods=['POST'])
def get_day_danmu_time_distribution():
    anime_title = request.json.get('anime_title')
    start_date = request.json.get('start_date')
    end_date = request.json.get('end_date')
    episode = request.json.get('episode')
    if not anime_title:
        return jsonify({"error": "动漫标题不能为空"}), 400
    table_name = anime_title.replace(" ", "_")
    table_name = ''.join(e for e in table_name if e.isalnum() or e == '_')
    df = read_danmu_from_mysql(table_name, SPARK_USER, SPARK_PASSWORD, SPARK_DATABASE, SPARK_HOST, SPARK_PORT)
    filtered_df = read_danmu_data_by_date_and_episode_spark(df, start_date, end_date, episode)
    result_df = danmu_hourly_distribution(filtered_df)
    time_distribution = result_df.to_dict(orient='records')
    print('get_danmu_time_distribution response:', time_distribution)
    return jsonify(time_distribution)

@app.route('/get_danmu_weekly_distribution', methods=['POST'])
def get_danmu_weekly_distribution():
    anime_title = request.json.get('anime_title')
    start_date = request.json.get('start_date')
    end_date = request.json.get('end_date')
    episode = request.json.get('episode')
    if not anime_title:
        return jsonify({"error": "动漫标题不能为空"}), 400
    table_name = anime_title.replace(" ", "_")
    table_name = ''.join(e for e in table_name if e.isalnum() or e == '_')
    df = read_danmu_from_mysql(table_name, SPARK_USER, SPARK_PASSWORD, SPARK_DATABASE, SPARK_HOST, SPARK_PORT)
    filtered_df = read_danmu_data_by_date_and_episode_spark(df, start_date, end_date, episode)
    # 按周分布统计
    from pyspark.sql.functions import from_unixtime, dayofweek
    filtered_df = filtered_df.withColumn('weekday', dayofweek(from_unixtime(filtered_df.create_time)))
    result = filtered_df.groupBy('weekday').count().orderBy('weekday').toPandas()
    days = ['周日','周一','周二','周三','周四','周五','周六']
    weekly_distribution = []
    for _, row in result.iterrows():
        weekly_distribution.append({'星期': days[int(row['weekday'])%7], '弹幕总量': int(row['count'])})
    print('get_danmu_weekly_distribution response:', weekly_distribution)
    return jsonify(weekly_distribution)

@app.route('/get_danmu_length_distribution', methods=['POST'])
def get_danmu_length_distribution():
    anime_title = request.json.get('anime_title')
    if not anime_title:
        return jsonify({"error": "动漫标题不能为空"}), 400
    table_name = anime_title.replace(" ", "_")
    table_name = ''.join(e for e in table_name if e.isalnum() or e == '_')
    df = read_danmu_from_mysql(table_name, SPARK_USER, SPARK_PASSWORD, SPARK_DATABASE, SPARK_HOST, SPARK_PORT)
    result_df = calculate_average_danmu_length_per_episode_spark(df)
    length_distribution = []
    for _, row in result_df.iterrows():
        length_distribution.append({'episode': int(row['episode']), 'avg_danmu_length': float(row['avg_danmu_length'])})
    print('get_danmu_length_distribution response:', length_distribution)
    return jsonify(length_distribution)

@app.route('/get_episode_list', methods=['POST'])
def get_episode_list():
    anime_title = request.json.get('anime_title')
    if not anime_title:
        return jsonify({"error": "标题不能为空"}), 400
    table_name = anime_title.replace(" ", "_")
    table_name = ''.join(e for e in table_name if e.isalnum() or e == '_')
    df = read_danmu_from_mysql(table_name, SPARK_USER, SPARK_PASSWORD, SPARK_DATABASE, SPARK_HOST, SPARK_PORT)
    episode_list = get_episode_list_spark(df)
    return jsonify(episode_list)

@app.route('/get_danmu_time_distribution_in_video', methods=['POST'])
def get_danmu_time_distribution_in_video():
    anime_title = request.json.get('anime_title')
    episode = request.json.get('episode')
    start_date = request.json.get('start_date')
    end_date = request.json.get('end_date')
    if not anime_title:
        return jsonify({"error": "标题不能为空"}), 400
    table_name = anime_title.replace(" ", "_")
    table_name = ''.join(e for e in table_name if e.isalnum() or e == '_')
    df = read_danmu_from_mysql(table_name, SPARK_USER, SPARK_PASSWORD, SPARK_DATABASE, SPARK_HOST, SPARK_PORT)
    filtered_df = read_danmu_data_by_date_and_episode_spark(df, start_date, end_date, episode)
    # 统计视频内弹幕分布（假设视频时长22分钟）
    from pyspark.sql.functions import floor
    filtered_df = filtered_df.withColumn('minute', floor(filtered_df.time_offset/60000))
    result = filtered_df.groupBy('minute').count().orderBy('minute').toPandas()
    time_distribution = []
    for _, row in result.iterrows():
        time_distribution.append({'episodeTime': int(row['minute']), 'count': int(row['count'])})
    print('get_danmu_time_distribution_in_video response:', time_distribution)
    return jsonify(time_distribution)

@app.route('/get_danmu_wordcloud', methods=['POST'])
def get_danmu_wordcloud():
    anime_title = request.json.get('anime_title')
    episode = request.json.get('episode')
    if not anime_title:
        return jsonify({"error": "标题不能为空"}), 400
    table_name = anime_title.replace(" ", "_")
    table_name = ''.join(e for e in table_name if e.isalnum() or e == '_')
    df = read_danmu_from_mysql(table_name, SPARK_USER, SPARK_PASSWORD, SPARK_DATABASE, SPARK_HOST, SPARK_PORT)
    filtered_df = read_danmu_data_by_date_and_episode_spark(df, None, None, episode)
    result_df = danmu_wordcloud(filtered_df)
    wordcloud_data = []
    for _, row in result_df.iterrows():
        if len(row['word']) > 1:
            wordcloud_data.append({'name': row['word'], 'value': int(row['count'])})
    return jsonify(wordcloud_data)

@app.route('/get_sentiment_analysis', methods=['POST'])
def get_sentiment_analysis():
    anime_title = request.json.get('anime_title')
    episode = request.json.get('episode')
    use_cache = request.json.get('use_cache', True)
    batch_size = request.json.get('batch_size', 1000)
    model_path = 'ml_models/sentiment_model.h5'
    tokenizer_path = 'ml_models/word_dictionary.pk'
    if not anime_title:
        return jsonify({"error": "动漫标题不能为空"}), 400
    table_name = anime_title.replace(" ", "_")
    table_name = ''.join(e for e in table_name if e.isalnum() or e == '_')
    df = read_danmu_from_mysql(table_name, SPARK_USER, SPARK_PASSWORD, SPARK_DATABASE, SPARK_HOST, SPARK_PORT)
    filtered_df = read_danmu_data_by_date_and_episode_spark(df, None, None, episode)
    danmu_texts = [row['content'] for row in filtered_df.select('content').toPandas().to_dict(orient='records') if row['content']]
    if not danmu_texts:
        return jsonify({"error": "未找到有效弹幕文本"}), 404
    print(f"开始情感分析，弹幕数量: {len(danmu_texts)}")
    from data_processor.video_sentences import analyze_sentiment_with_cache, analyze_sentiment
    if use_cache:
        sentiment_result = analyze_sentiment_with_cache(danmu_texts, model_path, tokenizer_path, use_cache=True)
    else:
        sentiment_result = analyze_sentiment(danmu_texts, model_path, tokenizer_path, batch_size=batch_size)
    sentiment_ratio = sentiment_result.get('sentiment_ratio', {})
    random_positive_comments = sentiment_result.get('random_positive_comments', [])
    random_negative_comments = sentiment_result.get('random_negative_comments', [])
    sentiment_ratio = {k: round(float(v), 2) for k, v in sentiment_ratio.items()}
    print({
        'sentiment_ratio': sentiment_ratio,
        'random_positive_comments': random_positive_comments,
        'random_negative_comments': random_negative_comments
    })
    return jsonify({
        'sentiment_ratio': sentiment_ratio,
        'random_positive_comments': random_positive_comments,
        'random_negative_comments': random_negative_comments
    })

@app.route('/get_danmu_count_and_average_length', methods=['POST'])
def get_danmu_count_and_average_length():
    episode = request.json.get('episode')
    anime_title = request.json.get('anime_title')
    if not anime_title:
        return jsonify({"error": "动漫标题不能为空"}), 400
    table_name = anime_title.replace(" ", "_")
    table_name = ''.join(e for e in table_name if e.isalnum() or e == '_')
    df = read_danmu_from_mysql(table_name, SPARK_USER, SPARK_PASSWORD, SPARK_DATABASE, SPARK_HOST, SPARK_PORT)
    # 统计每一集的弹幕数量
    result_df = count_danmu_per_episode_spark(df)
    # 计算每集弹幕的平均长度
    avg_df = calculate_average_danmu_length_per_episode_spark(df)
    result = []
    avg_map = {int(row['episode']): float(row['avg_danmu_length']) for _, row in avg_df.iterrows()}
    for _, row in result_df.iterrows():
        episode_num = int(row['episode'])
        danmu_count = int(row['count'])
        average_length = avg_map.get(episode_num, 0.0)
        result.append({'episode': episode_num, 'danmuCount': danmu_count, 'averageLength': average_length})
    return jsonify(result)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080)