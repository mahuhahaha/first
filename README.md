# 腾讯视频弹幕分析系统

本项目用于采集、分析腾讯视频各类视频作品（包括动漫、电视剧、电影等）的弹幕数据，支持弹幕统计、情感分析、词云展示等功能，并通过微信小程序进行可视化展示。

> **免责声明：本项目仅供学习和学术研究使用，禁止用于任何商业用途。请遵守相关法律法规，合理合规使用数据。**

## 项目亮点与自动化特性

- **一键采集全集**：只需输入任意一集或一段的腾讯视频网址，系统会自动识别该视频作品（如动漫、电视剧、电影等）的所有集数或分段，批量爬取全集弹幕数据。
- **自动保存**：采集过程中会自动下载并保存视频作品的封面图片，所有弹幕数据自动写入MySQL数据库。
- **多线程支持**：支持多线程批量采集，极大提升数据抓取效率。
- **丰富分析功能**：支持弹幕数量统计、时间分布、词云、情感分析等多维度分析。
- **可视化前端**：微信小程序端直观展示各类分析结果，交互友好。

## 目录结构

```
├── app.py                      # Flask后端主程序
├── data_crawler/               # 数据爬虫（弹幕与视频作品信息采集）
├── data_processor/             # 数据处理与分析（统计、情感分析等）
├── ml_models/                  # 训练好的情感分析模型及词典、训练数据
├── cache/                      # 弹幕情感分析缓存
├── images_true/                # 视频作品封面图片
├── wechat_miniprogram/         # 微信小程序前端
├── dan.sql                     # MySQL数据库完整数据（可直接导入）
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
  | 弹幕内容   | 情感标签（如正面/负面/中性） |

- 训练脚本：`data_processor/sentiment_model_trainer.py`，会生成 `sentiment_model.h5`（模型）和 `word_dictionary.pk`（分词字典）。
- 支持自定义训练，便于适配不同弹幕风格。

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

## 微信小程序端

- 目录：`wechat_miniprogram/`
- 主要页面：
  - 首页（index）：视频作品列表、搜索、类型筛选
  - 详情页（animeDetail）：视频作品信息、封面、跳转分析
  - 弹幕统计页（danmuStats）：每集/分段弹幕数、长度分布、情感饼图
  - 详细分析页（day）：一天/一周/视频内弹幕分布、词云、热门弹幕
- **注意：需在小程序端修改后端API地址为实际公网IP**
- 需在微信开发者工具中导入本目录，调试运行

## 端到端运行流程

1. （可选）采集弹幕数据，或直接导入 `dan.sql` 数据库
2. （可选）如需自定义情感分析模型，运行 `data_processor/sentiment_model_trainer.py` 进行训练
3. 启动后端服务：`python app.py`
4. 配置小程序端API地址为后端公网IP，微信开发者工具导入 `wechat_miniprogram/` 目录，运行预览

## 常见问题与注意事项

- 数据库需为MySQL，字符集建议 utf8mb4
- 采集弹幕需本地安装 Chrome 浏览器及 chromedriver，并配置好路径
- 小程序端 ec-canvas 组件需手动下载
- 后端API地址需为公网可访问，且小程序端需同步修改
- 若数据量大，建议直接导入 `dan.sql`，避免重复爬取
- 本项目仅供学习和研究，禁止用于商业用途

---

如有问题请联系项目维护者。 