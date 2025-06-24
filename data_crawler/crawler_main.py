import os
import os

import pandas as pd

from data_crawler.save_data import save_data_to_mysql
from data_crawler.fetch_video_content import get_danmu
from data_crawler.fetch_video_content import get_script_contents_and_image_urls, find_vid, jianjie, download_image


def pa(url, prefix):
    # 获取目标网页的内容
    script13_content, image_urls = get_script_contents_and_image_urls(url, prefix)

    if script13_content:
        title, video_vids = find_vid(script13_content)  # vid列表和标题
        result2 = jianjie(script13_content)  # 动漫简介

        # 数据库连接配置
        db_config = {
            'host': 'localhost',  # 数据库服务器地址
            'user': 'root',  # 数据库用户名
            'password': 'tpy520',  # 数据库密码
            'database': 'dan'  # 数据库名称
        }

        # 将result2的信息保存到info表中
        index_data = [
            (
                title,
                result2[0],  # 类型
                result2[1],  # 地区
                result2[2],  # 热度
                result2[3],  # 上映时间
                result2[4]  # 封面描述
            )
        ]
        save_data_to_mysql(index_data, db_config, 'info')

        # 获取封面图片并保存
        if image_urls:
            save_dir = "../wechat_miniprogram/images"
            if not os.path.exists(save_dir):
                os.makedirs(save_dir)
            if len(image_urls) == 1:
                for url in image_urls:
                    save_path = os.path.join(save_dir, f"{title}.jpg")
                    download_image(url, save_path)
            else:
                last_url = image_urls[-1]
                save_path = os.path.join(save_dir, f"{title}.jpg")
                download_image(last_url, save_path)

        # 统一处理所有视频内容
        all_danmu_DataFrame = pd.DataFrame()
        for i, video_code in enumerate(video_vids, start=1):  # 从1开始递增集数
            episode = i  # 集数
            print(f"开始爬取第{episode}集的弹幕，视频编号为：{video_code}")
            # 根据视频标题生成表名
            table_name = title.replace(" ", "_")  # 替换空格为下划线，避免表名中包含空格
            table_name = ''.join(e for e in table_name if e.isalnum() or e == '_')  # 去除非字母数字和下划线的字符
            episode_danmu_df = get_danmu(video_code, episode)
            all_danmu_DataFrame = pd.concat([all_danmu_DataFrame, episode_danmu_df])
            # 将当前集的弹幕数据保存到MySQL数据库
            save_data_to_mysql(episode_danmu_df.values.tolist(), db_config, table_name)

        print("所有视频的弹幕和封面保存完成！")
    else:
        print("未获取到 script13 的内容")

if __name__ == '__main__':
    url = input("请输入腾讯视频URL: ")
    prefix = 'https://vcover-vt-pic.puui.qpic.cn/vcover_vt_pic'
    pa(url, prefix)