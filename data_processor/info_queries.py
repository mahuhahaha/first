import mysql.connector
from mysql.connector import Error

# 数据库连接配置
db_config = {
    'host': 'localhost',  # 数据库主机地址
    'user': 'root',  # 数据库用户名
    'password': 'tpy520',  # 数据库密码
    'database': 'dan',  # 数据库名
    'charset': 'utf8mb4'  # 指定字符集为 utf8mb4
}

def fetch_anime_list(selected_type='全部'):
    """
    从数据库中读取动漫列表。

    参数:
        selected_type (str): 筛选类型，默认为 '全部'。

    返回:
        list: 包含动漫信息的列表。
    """
    try:
        connection = mysql.connector.connect(**db_config)
        cursor = connection.cursor(dictionary=True)
        if selected_type == '全部':
            query = "SELECT * FROM info"
            cursor.execute(query)
        else:
            query = "SELECT * FROM info WHERE type = %s"
            cursor.execute(query, (selected_type,))
        anime_list = cursor.fetchall()
        return anime_list
    except Error as e:
        print(f"Error while connecting to MySQL: {e}")
        return []
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()



def fetch_anime_info(anime_title):
    """
    从数据库中获取指定动漫的详细信息。

    参数:
        db_config (dict): 数据库连接配置。
        anime_title (str): 动漫标题。

    返回:
        dict: 包含动漫详细信息的字典，如果未找到则返回 None。
    """
    try:
        connection = mysql.connector.connect(**db_config)
        cursor = connection.cursor(dictionary=True)
        query = "SELECT * FROM info WHERE title = %s"
        cursor.execute(query, (anime_title,))
        anime_info = cursor.fetchone()
        return anime_info
    except Error as e:
        print(f"Error while connecting to MySQL: {e}")
        return None
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

def search_anime_by_keyword(keyword):
    """
    根据关键词搜索动漫信息。

    参数:
        db_config (dict): 数据库连接配置。
        keyword (str): 搜索关键词。

    返回:
        list: 包含匹配动漫信息的列表。
    """
    try:
        connection = mysql.connector.connect(**db_config)
        cursor = connection.cursor(dictionary=True)
        query = "SELECT * FROM info WHERE title LIKE %s"
        cursor.execute(query, (f"%{keyword}%",))
        anime_list = cursor.fetchall()
        return anime_list
    except Error as e:
        print(f"Error while connecting to MySQL: {e}")
        return []
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()


