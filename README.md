# 腾讯视频弹幕分析系统

本项目用于采集、分析腾讯视频各类视频作品（包括动漫、电视剧、电影等）的弹幕数据，支持弹幕统计、情感分析、词云展示等功能，并通过微信小程序进行可视化展示。

**本项目支持基于 Spark 的大数据弹幕分析，适合大规模数据场景，能够高效处理和分析百万级弹幕数据。**

> **免责声明：本项目仅供学习和学术研究使用，禁止用于任何商业用途。请遵守相关法律法规，合理合规使用数据。**

## 项目亮点与自动化特性

- **一键采集全集**：只需输入任意一集或一段的腾讯视频网址，系统会自动识别该视频作品（如动漫、电视剧、电影等）的所有集数或分段，批量爬取全集弹幕数据。
- **自动保存**：采集过程中会自动下载并保存视频作品的封面图片，所有弹幕数据自动写入MySQL数据库。
- **多线程支持**：支持多线程批量采集，极大提升数据抓取效率。
- **丰富分析功能**：支持弹幕数量统计、时间分布、词云、情感分析等多维度分析。
- **可视化前端**：微信小程序端直观展示各类分析结果，交互友好。
- **支持基于 Spark 的大数据弹幕分析**：可高效处理和分析百万级弹幕数据，适合大数据量、分布式分析场景。

## 目录结构

```
├── app.py                      # Flask后端主程序（适合小数据量/开发测试）
├── app2.py                     # Spark大数据分析后端（推荐用于大数据量/生产环境）
├── data_crawler/               # 数据爬虫，负责弹幕与视频作品信息采集
│   ├── crawler_main.py         # 主爬虫入口脚本
│   ├── fetch_video_content.py  # 视频内容与弹幕抓取逻辑
│   ├── multi_thread.py         # 多线程批量采集脚本
│   ├── save_data.py            # 数据保存到MySQL的工具
│   └── ...                     # 其他采集相关脚本
├── data_processor/             # 数据处理与分析
│   ├── spark_analysis.py       # 基于Spark的大数据分析工具集
│   ├── info_queries.py         # 视频信息表查询与分析
│   ├── sentiment_model_trainer.py # 情感分析模型训练脚本
│   ├── video_sentences.py      # 弹幕内容处理与统计分析
│   └── ...                     # 其他数据处理脚本
├── ml_models/                  # 训练好的情感分析模型及词典、训练数据
│   ├── sentiment_model.h5      # Keras/TensorFlow情感分析模型
│   ├── word_dictionary.pk      # 分词字典（pickle格式）
│   ├── training_data.csv       # 情感模型训练数据
│   └── ...
├── cache/                      # 弹幕情感分析缓存（如分集缓存的json文件）
├── images_true/                # 视频作品封面图片（采集时自动下载）
├── 封面/                        # 备用或手动上传的视频封面图片
├── wechat_miniprogram/         # 微信小程序前端，负责数据可视化展示
│   ├── pages/                  # 小程序各页面目录
│   ├── components/             # 组件（如echarts、loading等）
│   ├── config/                 # 配置文件（如API路径、主题色等）
│   ├── utils/                  # 工具函数
│   ├── app.js/.json/.wxss      # 小程序全局入口
│   └── ...
├── dan.sql                     # MySQL数据库完整数据（可直接导入）（过大未上传）
├── requirements.txt            # Python依赖包列表
├── archive.tar                 # 归档数据或备份（如弹幕原始数据包）
├── README.md                   # 项目说明文档
└── ...                         # 其他辅助文件
```

---

## 后端服务对比与选择

### app.py —— 传统分析接口（适合小数据量/快速开发）
- 基于 Flask 的后端服务，主要使用 Pandas 和 MySQL 进行数据分析与统计。
- 适合数据量较小、开发测试、对实时性要求不高的场景。
- 启动快、依赖少，代码简单，易于维护。
- 处理大数据量时性能有限。

### app2.py —— Spark 大数据分析接口（适合大数据量/高并发）
- 基于 Flask 的后端服务，所有分析功能均通过 PySpark 实现，直接从 MySQL 读取数据，适合大规模弹幕数据的高效分析。
- 适合数据量大、需要高性能分析、生产环境或大数据场景。
- 支持分布式、并行处理，适合批量、实时、复杂分析任务。
- 启动和分析速度受 Spark 启动影响，首次请求可能较慢，依赖 Spark 运行环境。

### 用户如何选择？
- 数据量小、开发测试、对实时性要求不高：推荐使用 app.py
- 数据量大、需要高性能分析、生产环境或大数据场景：推荐使用 app2.py
- 可以先用 app.py 快速开发和测试，后续数据量增大时平滑切换到 app2.py，无需大幅修改前端和接口调用方式。

---

## app2.py 主要功能
- 提供基于 Flask 的后端服务，专为大数据弹幕分析设计。
- 主要接口均通过 POST 方法，接收前端 JSON 请求，调用 Spark 分析函数，返回 JSON 格式的分析结果。
- 支持的接口包括：
  - /get_danmu_stats：统计每集弹幕数量
  - /get_danmu_time_distribution：弹幕时间分布（日/小时）
  - /get_danmu_wordcloud：弹幕词云（高频词统计）
  - /get_danmu_length_distribution：弹幕内容长度分布
  - /get_danmu_sentiment_analysis：弹幕情感分析
- 所有分析均基于 Spark，适合大数据量弹幕的高效分析和实时接口服务。

## data_processor/spark_analysis.py 主要功能
- 封装了所有基于 PySpark 的弹幕数据分析函数，支持直接从 MySQL 读取数据，无需中间 CSV 文件。
- 主要功能包括：
  - read_danmu_from_mysql：从 MySQL 数据库读取弹幕表为 Spark DataFrame
  - count_danmu_per_episode_spark：统计每集弹幕数量
  - danmu_daily_distribution：按天统计弹幕数量
  - danmu_hourly_distribution：按小时统计弹幕数量
  - danmu_wordcloud：分词并统计高频词，用于词云
  - danmu_length_distribution：统计每集弹幕的平均长度
  - danmu_sentiment_analysis：批量情感分析（集成 Keras/TensorFlow 模型）
- 所有分析函数均以 Spark DataFrame 为输入，返回 pandas DataFrame，便于与 Flask 后端集成。

---

## 端到端运行流程

1. （可选）采集弹幕数据，或直接导入 `dan.sql` 数据库
2. （可选）如需自定义情感分析模型，运行 `data_processor/sentiment_model_trainer.py` 进行训练
3. 启动后端服务：
   - 小数据量/开发测试：`python app.py`
   - 大数据量/生产环境/高性能分析：`python app2.py`（推荐，基于 Spark 的大数据分析）
4. 配置小程序端API地址为后端公网IP，微信开发者工具导入 `wechat_miniprogram/` 目录，运行预览

---

## 后端服务

- 启动：
  ```bash
  python app.py
  ```
- 默认监听 8080 端口，支持跨域。
- 主要接口：
  - `/get_anime_list` 获取视频作品列表
  - `/search_anime` 关键词搜索视频作品
  - `/get_anime_info` 获取视频作品详细信息
  - `/get_danmu_stats` 获取每集/分段弹幕数量
  - `/get_day_danmu_time_distribution` 获取一天内弹幕分布
  - `/get_danmu_weekly_distribution` 获取一周弹幕分布
  - `/get_danmu_length_distribution` 获取弹幕长度分布
  - `/get_episode_list` 获取集数/分段列表
  - `/get_danmu_time_distribution_in_video` 获取视频内弹幕分布
  - `/get_danmu_wordcloud` 获取词云数据
  - `/get_sentiment_analysis` 获取情感分析结果
  - `/get_danmu_count_and_average_length` 获取弹幕总数和平均长度

---

## 微信小程序端
本项目的前端代码请见：[mahuhahaha/txsp-wp](https://git.weixin.qq.com/mahuhahaha/txsp-wp)
- 目录：`wechat_miniprogram/`
- 主要页面：
  - 首页（index）：视频作品列表、搜索、类型筛选
  - 详情页（animeDetail）：视频作品信息、封面、跳转分析
  - 弹幕统计页（danmuStats）：每集/分段弹幕数、长度分布、情感饼图，支持与 Spark 版后端高效对接
  - 详细分析页（day）：一天/一周/视频内弹幕分布、词云、热门弹幕
- 代码结构规范，所有后端请求均通过统一的 utils.request 方法，便于切换和维护（支持传统后端与 Spark 版后端接口）。
- 支持主题色自定义、loading 状态、无数据友好提示等用户体验优化。
- 配置文件（config/config.js）集中管理 API 路径、主题色、静态资源等，便于灵活扩展。
- **注意：需在小程序端修改后端API地址为实际公网IP**
- 需在微信开发者工具中导入本目录，调试运行

----

## 联系方式

如有问题、建议或合作意向，请联系项目维护者：

- 邮箱：2969088707@qq.com
