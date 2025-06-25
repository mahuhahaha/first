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
├── data_crawler/               # 数据爬虫（弹幕与视频作品信息采集）
├── data_processor/             # 数据处理与分析
│   ├── spark_analysis.py       # 基于Spark的大数据分析工具集
│   ├── info_queries.py         # 传统MySQL查询与分析
│   ├── sentiment_model_trainer.py # 情感分析模型训练脚本
│   └── ...                     # 其他数据处理脚本
├── ml_models/                  # 训练好的情感分析模型及词典、训练数据
├── cache/                      # 弹幕情感分析缓存
├── wechat_miniprogram/         # 微信小程序前端，负责数据可视化展示
├── dan.sql                     # MySQL数据库完整数据（可直接导入）
├── README.md                   # 项目说明文档
├── requirements.txt            # Python依赖
└── ...                         # 其他辅助文件
```

## 数据库结构与导入

### 1. info表（视频作品信息表）

| 字段名         | 类型         | 说明         |
| -------------- | ------------ | ------------ |
| id             | int          | 主键，自增   |
| title          | varchar      | 视频作品标题 |
| nation         | varchar      | 地区         |
| popularity     | varchar      | 热度         |
| update_time    | varchar      | 上映/更新时间|
| introduction   | varchar      | 简介         |

### 2. 弹幕表（每部视频作品一张，表名为title去空格和特殊字符）

| 字段名         | 类型         | 说明         |
| -------------- | ------------ | ------------ |
| id             | int          | 主键，自增   |
| time_offset    | int          | 弹幕出现时间（毫秒）|
| create_time    | int          | 弹幕发送时间（Unix时间戳）|
| content        | varchar      | 弹幕内容     |
| episode        | int          | 集数/分段    |

### 3. 数据库导入导出

- 导出：  
  `mysqldump -u root -p dan > dan.sql`
- 导入：  
  `mysql -u root -p dan < dan.sql`

## 依赖环境

### Python 版本

建议 Python 3.8+，以下为主要依赖及版本（部分）：

- Flask==3.0.3
- flask-cors==6.0.1
- pandas==2.2.3
- numpy==2.0.2
- jieba==0.42.1
- mysql-connector-python==9.1.0
- selenium==4.33.0
- requests==2.32.3
- tensorflow==2.19.0
- keras==3.10.0
- scikit-learn==1.6.1
- matplotlib==3.9.4

> 其余依赖见你实际环境，建议使用 `pip freeze > requirements.txt` 统一管理。

### 小程序端

- 需安装 [微信开发者工具](https://developers.weixin.qq.com/miniprogram/dev/devtools/download.html)
- 需手动下载 [ec-canvas](https://github.com/ecomfe/echarts-for-weixin) 组件，复制到 `wechat_miniprogram/components/ec-canvas/`

## 数据采集与处理流程

### 1. 自动采集视频作品全集弹幕

- 运行 `data_crawler/crawler_main.py`，输入任意一集或一段的腾讯视频URL，系统会自动：
  - 解析出该视频作品（如动漫、电视剧、电影等）的所有集数或分段
  - 依次爬取每一集/分段的弹幕数据
  - 下载并保存该作品的封面图片到 `images_true/` 目录
  - 将所有弹幕数据自动写入MySQL数据库（每部作品一张表）
- 支持多线程采集：运行 `data_crawler/multi_thread.py`，可同时输入10个URL批量采集不同作品

### 2. 数据库直接导入

- 若无需自行爬取，可直接用 `dan.sql` 导入数据库，省略采集步骤。

## 情感分析模型

- 训练数据：`ml_models/training_data.csv`，格式如下：

  | evaluation | label |
  | ---------- | ----- |
  | 弹幕内容   | 情感标签（如正面/负面） |

- 训练脚本：`data_processor/sentiment_model_trainer.py`，会生成 `sentiment_model.h5`（模型）和 `word_dictionary.pk`（分词字典）。
- 支持自定义训练，便于适配不同弹幕风格。

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

## 端到端运行流程

1. （可选）采集弹幕数据，或直接导入 `dan.sql` 数据库
2. （可选）如需自定义情感分析模型，运行 `data_processor/sentiment_model_trainer.py` 进行训练
3. 启动后端服务：
   - 小数据量/开发测试：`python app.py`
   - 大数据量/生产环境/高性能分析：`python app2.py`（推荐，基于 Spark 的大数据分析）
4. 配置小程序端API地址为后端公网IP，微信开发者工具导入 `wechat_miniprogram/` 目录，运行预览

## 常见问题与注意事项

- 数据库需为MySQL，字符集建议 utf8mb4
- 采集弹幕需本地安装 Chrome 浏览器及 chromedriver，并配置好路径
- 小程序端 ec-canvas 组件需手动下载
- 后端API地址需为公网可访问，且小程序端需同步修改
- 若数据量大，建议直接导入 `dan.sql`，避免重复爬取
- 本项目仅供学习和研究，禁止用于商业用途

---

## 联系方式

如有问题、建议或合作意向，请联系项目维护者：

- 邮箱：your_email@example.com
- 微信：your_wechat_id

（请将上述联系方式替换为你的真实信息）

如有问题请联系项目维护者。 