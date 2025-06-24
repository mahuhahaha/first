import mysql.connector
from mysql.connector import Error

def save_data_to_mysql(data, db_config, table_name):
    """
    将数据保存到MySQL数据库中。

    :param data: 要插入的数据，格式为列表的元组，例如 [(title, type, nation, popularity, update_time, introduction), ...]
    :param db_config: 数据库连接配置，格式为字典，例如 {'host': 'localhost', 'user': 'root', 'password': 'password', 'database': 'dbname'}
    :param table_name: 要插入数据的表名
    """
    try:
        # 连接到数据库
        connection = mysql.connector.connect(**db_config)
        if connection.is_connected():
            print("Connected to the database successfully!")

            # 创建游标对象
            cursor = connection.cursor()

            # 创建表（如果表不存在）
            if table_name == 'info':
                create_table_query = f"""
                CREATE TABLE IF NOT EXISTS {table_name} (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    title VARCHAR(255) NOT NULL,
                    type VARCHAR(255) NOT NULL,
                    nation VARCHAR(255) NOT NULL,
                    popularity VARCHAR(255) NOT NULL,
                    update_time VARCHAR(255) NOT NULL,
                    introduction TEXT NOT NULL,
                    UNIQUE (title)  -- 添加唯一约束
                );
                """
            else:
                create_table_query = f"""
                CREATE TABLE IF NOT EXISTS {table_name} (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    time_offset INT NOT NULL,
                    create_time INT NOT NULL,
                    content TEXT NOT NULL,
                    episode INT NOT NULL,
                    UNIQUE (time_offset, create_time, content(255), episode)  -- 添加唯一约束
                );
                """
            cursor.execute(create_table_query)
            print(f"Table '{table_name}' created or already exists.")

            if table_name == 'info':
                insert_query = f"""
                INSERT INTO {table_name} (title, type, nation, popularity, update_time, introduction)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                type = VALUES(type),
                nation = VALUES(nation),
                popularity = VALUES(popularity),
                update_time = VALUES(update_time),
                introduction = VALUES(introduction);
                """
            else:
                insert_query = f"""
                INSERT IGNORE INTO {table_name} (time_offset, create_time, content, episode)
                VALUES (%s, %s, %s, %s);
                """
            try:
                cursor.executemany(insert_query, data)
                connection.commit()
                print(f"{cursor.rowcount} rows inserted/updated successfully into table '{table_name}'.")
            except mysql.connector.Error as e:
                if e.errno == 1062:  # Duplicate entry error
                    print(f"Duplicate entry detected. Skipping duplicate data: {e}")
                else:
                    print(f"Error while inserting data: {e}")

    except Error as e:
        print(f"Error while connecting to MySQL: {e}")

    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()
            print("MySQL connection is closed")