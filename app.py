import functools

import pandas as pd
from flask import Flask, jsonify, request

from data_processor.info_queries import fetch_anime_list, search_anime_by_keyword, fetch_anime_info
from flask_cors import CORS

from data_processor.video_sentences import count_danmu_per_episode, read_danmu_data_by_date_and_episode, \
    analyze_weekly_danmu_distribution, analyze_time_distribution, \
    analyze_danmu_time_distribution_in_video_area_chart, count_cipin, calculate_average_danmu_length_per_episode, \
    get_episode_list_from_db, analyze_sentiment, preprocess_sentiment_danmu_data, analyze_sentiment_with_cache

app = Flask(__name__)
CORS(app)  # 允许所有来源访问


@app.route('/get_anime_list')
def get_anime_list():
    try:
        selected_type = request.args.get('type', '全部')
        anime_list = fetch_anime_list(selected_type)
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
        if selected_type == '全部':
            anime_list = search_anime_by_keyword(keyword)
        else:
            anime_list = [anime for anime in search_anime_by_keyword(keyword) if anime['type'] == selected_type]
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
        anime_info = fetch_anime_info( anime_title)
        if anime_info:
            return jsonify(anime_info)
        else:
            return jsonify({"error": "未找到指定的动漫信息"}), 404
    except Exception as e:
        print(f"Error while fetching anime info: {e}")
        return jsonify({"error": "获取动漫信息失败"}), 500

@app.route('/get_danmu_stats', methods=['POST'])
def get_danmu_stats():
    anime_title = request.json.get('anime_title')
    if not anime_title:
        return jsonify({"error": "动漫标题不能为空"}), 400

    table_name = anime_title.replace(" ", "_")
    table_name = ''.join(e for e in table_name if e.isalnum() or e == '_')

    try:
        danmu_stats = count_danmu_per_episode(table_name, episode="all")
        # 修改 DataFrame 的列名
        for episode, df in danmu_stats.items():
            df.rename(columns={'episode': '集数', 'danmu_count': '弹幕数量'}, inplace=True)
        # 将字典中的 DataFrame 合并为一个 DataFrame
        danmu_stats = pd.concat(danmu_stats.values(), ignore_index=True)
        danmu_stats = danmu_stats.to_dict(orient='records')
        print('get_danmu_stats response:', danmu_stats)
        return jsonify(danmu_stats)
    except Exception as e:
        print(f"Error while getting danmu stats: {e}")
        return jsonify({"error": "获取弹幕统计信息失败"}), 500

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

    try:
        all_data_dict = read_danmu_data_by_date_and_episode(table_name, episode=episode, start_date=start_date, end_date=end_date)
        time_distribution = analyze_time_distribution(all_data_dict)
        time_distribution = time_distribution.to_dict(orient='records')
        print('get_danmu_time_distribution response:', time_distribution)
        return jsonify(time_distribution)
    except Exception as e:
        print(f"Error while getting danmu time distribution: {e}")
        return jsonify({"error": "获取弹幕时间分布信息失败"}), 500

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

    try:
        all_data_dict = read_danmu_data_by_date_and_episode(table_name, episode=episode, start_date=start_date, end_date=end_date)
        weekly_distribution = analyze_weekly_danmu_distribution(all_data_dict)
        weekly_distribution = weekly_distribution.to_dict(orient='records')
        print('get_danmu_weekly_distribution response:', weekly_distribution)
        return jsonify(weekly_distribution)
    except Exception as e:
        print(f"Error while getting danmu weekly distribution: {e}")
        return jsonify({"error": "获取弹幕周分布信息失败"}), 500


@app.route('/get_danmu_length_distribution', methods=['POST'])
def get_danmu_length_distribution():
    anime_title = request.json.get('anime_title')
    if not anime_title:
        return jsonify({"error": "动漫标题不能为空"}), 400

    table_name = anime_title.replace(" ", "_")
    table_name = ''.join(e for e in table_name if e.isalnum() or e == '_')

    try:
        # 使用新的分析函数
        length_distribution_dict = calculate_average_danmu_length_per_episode(table_name, episode="all")

        # 将字典中的 DataFrame 合并为一个 DataFrame
        length_distribution = pd.concat(length_distribution_dict.values(), ignore_index=True)

        # 转换为所需的格式
        length_distribution = length_distribution.to_dict(orient='records')

        print('get_danmu_length_distribution response:', length_distribution)
        return jsonify(length_distribution)
    except Exception as e:
        print(f"Error while getting danmu length distribution: {e}")
        return jsonify({"error": "获取弹幕长度分布信息失败"}), 500

@app.route('/get_episode_list', methods=['POST'])
def get_episode_list():
    anime_title = request.json.get('anime_title')
    if not anime_title:
        return jsonify({"error": "标题不能为空"}), 400

    table_name = anime_title.replace(" ", "_")
    table_name = ''.join(e for e in table_name if e.isalnum() or e == '_')

    try:
        episode_list = get_episode_list_from_db(table_name)
        # 确保返回的数据是可以被 JSON 序列化的格式
        episode_list = list(episode_list.keys())  # 将字典的键（集数）转换为列表
        return jsonify(episode_list)
    except Exception as e:
        print(f"Error while getting episode list: {e}")
        return jsonify({"error": "获取集数列表失败"}), 500


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

    try:
        all_data_dict = read_danmu_data_by_date_and_episode(table_name, episode=episode, start_date=start_date, end_date=end_date)
        if not all_data_dict:
            return jsonify([])

        all_danmu_data = pd.concat(all_data_dict.values(), ignore_index=True)
        time_distribution = analyze_danmu_time_distribution_in_video_area_chart(all_danmu_data, video_duration=22)
        time_distribution = time_distribution.to_dict(orient='records')
        print('get_danmu_time_distribution_in_video response:', time_distribution)
        return jsonify(time_distribution)
    except Exception as e:
        print(f"Error while getting danmu time distribution in video: {e}")
        return jsonify({"error": "获取弹幕在视频内时间分布信息失败"}), 500


@app.route('/get_danmu_wordcloud', methods=['POST'])
def get_danmu_wordcloud():
    anime_title = request.json.get('anime_title')
    episode = request.json.get('episode')
    if not anime_title:
        return jsonify({"error": "标题不能为空"}), 400

    table_name = anime_title.replace(" ", "_")
    table_name = ''.join(e for e in table_name if e.isalnum() or e == '_')

    try:
        # 使用新的读取函数
        all_data_dict = read_danmu_data_by_date_and_episode(table_name, episode=episode)

        # 如果没有数据，返回空列表
        if not all_data_dict:
            return jsonify([])

        all_danmu_data = pd.concat(all_data_dict.values(), ignore_index=True)

        text = ' '.join(all_danmu_data['content'].dropna())

        # 调用 count 函数并验证返回值
        word_counts_list = count_cipin(text)
        if not isinstance(word_counts_list, list) or not all(
                isinstance(item, tuple) and len(item) == 2 for item in word_counts_list):
            return jsonify({"error": "count 函数返回值格式错误"}), 500

        word_counts = dict(word_counts_list)
        wordcloud_data = [{"name": word, "value": count} for word, count in word_counts.items() if len(word) > 1]
        return jsonify(wordcloud_data)
    except Exception as e:
        print(f"Error while getting danmu wordcloud: {e}")
        return jsonify({"error": "获取弹幕词频统计云图信息失败"}), 500

@app.route('/get_sentiment_analysis', methods=['POST'])
def get_sentiment_analysis():
    anime_title = request.json.get('anime_title')
    episode = request.json.get('episode')
    use_cache = request.json.get('use_cache', True)  # 新增缓存控制参数
    batch_size = request.json.get('batch_size', 1000)  # 新增批处理大小参数
    model_path = 'ml_models/sentiment_model.h5'
    tokenizer_path = 'ml_models/word_dictionary.pk'

    if not anime_title:
        return jsonify({"error": "动漫标题不能为空"}), 400

    table_name = anime_title.replace(" ", "_")
    table_name = ''.join(e for e in table_name if e.isalnum() or e == '_')

    try:
        all_data_dict = read_danmu_data_by_date_and_episode(table_name, episode=episode)
        if not all_data_dict:
            return jsonify({"error": "未找到弹幕数据"}), 404

        all_danmu_data = pd.concat(all_data_dict.values(), ignore_index=True)
        danmu_texts = preprocess_sentiment_danmu_data(all_danmu_data)

        if not danmu_texts:
            return jsonify({"error": "未找到有效弹幕文本"}), 404

        print(f"开始情感分析，弹幕数量: {len(danmu_texts)}")
        
        # 使用优化后的情感分析函数
        if use_cache:
            sentiment_result = analyze_sentiment_with_cache(danmu_texts, model_path, tokenizer_path, use_cache=True)
        else:
            sentiment_result = analyze_sentiment(danmu_texts, model_path, tokenizer_path, batch_size=batch_size)
            
        sentiment_ratio = sentiment_result.get('sentiment_ratio', {})
        random_positive_comments = sentiment_result.get('random_positive_comments', [])
        random_negative_comments = sentiment_result.get('random_negative_comments', [])

        # 将 np.float64 转换为 float 并保留 2 位小数
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

    except Exception as e:
        print(f"Error while getting sentiment analysis: {e}")
        return jsonify({"error": "情感分析失败"}), 500

@app.route('/get_danmu_count_and_average_length', methods=['POST'])
def get_danmu_count_and_average_length():
    episode=request.json.get('episode')
    anime_title = request.json.get('anime_title')
    if not anime_title:
        return jsonify({"error": "动漫标题不能为空"}), 400

    table_name = anime_title.replace(" ", "_")
    table_name = ''.join(e for e in table_name if e.isalnum() or e == '_')

    try:
        # 统计每一集的弹幕数量
        danmu_count_dict = count_danmu_per_episode(table_name, episode=episode)
        # 计算每集弹幕的平均长度
        average_length_dict = calculate_average_danmu_length_per_episode(table_name, episode="all")

        result = []
        for episode in danmu_count_dict.keys():
            # 提取弹幕数量并转换为 int
            danmu_count = int(danmu_count_dict[episode].iloc[0]['danmu_count'])
            # 提取平均长度并转换为 float
            average_length = float(average_length_dict[episode].iloc[0]['avg_danmu_length'])
            result.append({
                'episode': episode,
                'danmuCount': danmu_count,
                'averageLength': average_length
            })

        return jsonify(result)
    except Exception as e:
        print(f"Error while getting danmu count and average length: {e}")
        return jsonify({"error": "获取弹幕数量和平均长度信息失败"}), 500
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080)