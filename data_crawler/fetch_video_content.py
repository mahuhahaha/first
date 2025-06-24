import json
import os
import re
import time

import pandas as pd
import requests
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service


def get_script_contents_and_image_urls(url, prefix):
    """
    打开目标网页，提取所有 <script> 标签的内容，并获取所有符合条件的图片URL
    :param url: 目标网页的 URL
    :param prefix: 图片URL的前缀
    :return: script13 的内容和符合条件的图片URL列表
    """
    # 指定 chromedriver 的路径
    driver_path = r'chromedriver.exe'  # 替换为你的 chromedriver 实际路径

    # 指定 Chrome 浏览器的路径（如果非默认路径）
    chrome_binary_path = r"C:\Users\Administrator\AppData\Local\Google\Chrome\Bin\chrome.exe"  # 替换为你的 Chrome 浏览器实际路径

    # 创建 Chrome 选项对象
    chrome_options = Options()
    chrome_options.binary_location = chrome_binary_path

    # 启用无头模式
    chrome_options.add_argument("--headless")

    # 使用 Service 类初始化 WebDriver
    service = Service(executable_path=driver_path)
    driver = webdriver.Chrome(service=service, options=chrome_options)

    try:
        # 打开网页
        driver.get(url)

        # 等待页面加载（根据需要调整等待时间）
        time.sleep(10)

        # 执行JavaScript代码，获取页面中的所有 <script> 标签内容
        script = """
        var scriptTags = document.querySelectorAll('script');
        var scriptContents = [];
        for (var i = 0; i < scriptTags.length; i++) {
            scriptContents.push(scriptTags[i].innerText.trim());
        }
        return scriptContents;
        """
        script_contents = driver.execute_script(script)


        # 提取 script13 的内容
        script13_content = None
        if len(script_contents) > 13:
            script13_content = script_contents[12]  # script13 的索引是 12

        # 获取页面中的所有<img>标签
        img_tags = driver.find_elements('tag name', 'img')

        # 提取<img>标签的src属性，并筛选出以特定前缀开头的URL
        image_urls = []
        for img in img_tags:
            src = img.get_attribute('src')
            if src and src.startswith(prefix):
                image_urls.append(src)

        return script13_content, image_urls
    finally:
        # 关闭浏览器
        driver.quit()

def find_vid(script13_content):
    """
    解析 script13 的内容，提取所需的信息
    :param script13_content: script13 的内容
    :return: 提取的结果列表
    """
    match = re.search(r'"video_ids":\s*(\[.*?])', script13_content)
    title_match = re.search(r'"title":\s*"([^"]+)"', script13_content)

    if not match:
        print("未找到 video_ids")
        return []

    # 将匹配到的 JSON 字符串解析为 Python 列表
    video_ids = json.loads(match.group(1))
    title = title_match.group(1)
    return title,video_ids

def download_image(image_url, save_path):
    """
    下载图片并保存到本地
    :param image_url: 图片的URL
    :param save_path: 保存路径
    """
    try:
        response = requests.get(image_url)
        if response.status_code == 200:
            with open(save_path, 'wb') as f:
                f.write(response.content)
            print(f"图片已成功保存到 {save_path}")
        else:
            print(f"图片下载失败，状态码：{response.status_code}")
    except Exception as e:
        print(f"下载图片时发生错误：{e}")

def jianjie(script13_content):


    # 定义需要匹配的正则表达式模式列表
    patterns = [
        r'"type_name"\s*:\s*"([^"]+)"',
        r'"area_name"\s*:\s*"([^"]+)"',
        r'"hotval"\s*:\s*"([^"]+)"',
        r'"holly_online_time"\s*:\s*"([^"]+)"',
        r'"cover_description"\s*:\s*"([^"]+)"',

    ]

    # 初始化结果列表
    result = []

    # 遍历每个正则表达式模式，查找匹配项
    for pattern in patterns:
        matches = re.findall(pattern, script13_content)
        if matches:
            # 如果有匹配项，添加第一个匹配项
            result.append(matches[0])
        else:
            # 如果没有匹配项，添加空字符串
            result.append("无")

    return result
def get_danmu(video_code, episode, num=10000, step=30000):
    """
    爬取腾讯视频单集弹幕
    :param video_code: 视频的编号
    :param episode: 第几集
    :param num: 获取弹幕的次数
    :param step: 步进参数
    :return: DataFrame
    """
    episodes_danmu_DataFrame = pd.DataFrame()

    for i in range(num):
        url = f'https://dm.video.qq.com/barrage/segment/{video_code}/t/v1/{i * step}/{i * step + step}'
        response = requests.get(url=url).json()
        if len(response["barrage_list"]) > 0:
            temp_danmu_DataFrame = pd.json_normalize(response['barrage_list'], errors='ignore')
            temp_danmu_DataFrame['episode'] = episode  # 添加第几集列
            episodes_danmu_DataFrame = pd.concat([episodes_danmu_DataFrame, temp_danmu_DataFrame])
        else:
            break

    print(f"第{episode}集总共获取到{episodes_danmu_DataFrame.shape[0]}条弹幕")
    rows = episodes_danmu_DataFrame.shape
    print(f"第{episode}集请求得到的表格行数与列数：{rows}")

    # 选择保存的列
    episodes_danmu_DataFrame = episodes_danmu_DataFrame.loc[:, ['time_offset', 'create_time', 'content', 'episode']]
    return episodes_danmu_DataFrame



def main():
    # 输入腾讯视频页面的URL
    url = 'https://v.qq.com/x/cover/mzc002009pn2c2r/h4101usl2v6.html'
    # 图片URL的前缀
    prefix = 'https://vcover-vt-pic.puui.qpic.cn/vcover_vt_pic'

    # 获取 script13 的内容和符合条件的图片URL
    script13_content, image_urls = get_script_contents_and_image_urls(url, prefix)

    if script13_content:
        title,video_vids = find_vid(script13_content)
        print(title,video_vids)

        # 如果找到符合条件的图片URL，下载并保存
        if image_urls:
            print(f"找到的符合条件的图片URL如下：")
            for url in image_urls:
                print(url)

            # 保存图片到本地
            save_dir = "../wechat_miniprogram/封面"
            if not os.path.exists(save_dir):
                os.makedirs(save_dir)
            for i, url in enumerate(image_urls):
                save_path = os.path.join(save_dir, f"{title}.jpg")
                download_image(url, save_path)

        else:
            print("未找到符合条件的图片URL")
    else:
        print("未获取到 script13 的内容")

if __name__ == "__main__":
    main()