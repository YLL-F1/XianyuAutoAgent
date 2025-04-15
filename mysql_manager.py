import mysql.connector
import time
from loguru import logger
from dotenv import load_dotenv
import os

class XianyuMySQLManager:
    def __init__(self):
        """初始化MySQL数据库管理器"""
        load_dotenv()
        # 从环境变量获取配置，并确保端口是整数
        host = os.getenv('MYSQL_HOST', 'localhost')
        port = int(os.getenv('MYSQL_PORT', '3306'))  # 默认MySQL端口
        user = os.getenv('MYSQL_USER', 'root')
        password = os.getenv('MYSQL_PASSWORD', '')
        database = os.getenv('MYSQL_DATABASE', 'xianyu')
        
        # 创建连接配置字典
        config = {
            'host': host,
            'port': port,
            'user': user,
            'password': password,
            'database': database,
            'charset': 'utf8mb4',
            'collation': 'utf8mb4_unicode_ci',
            'use_unicode': True
        }
        
        try:
            self.conn = mysql.connector.connect(**config)
            self.cursor = self.conn.cursor(dictionary=True)
            self._create_tables()
            logger.info(f"成功连接到MySQL数据库: {host}:{port}")
        except mysql.connector.Error as err:
            logger.error(f"连接MySQL数据库失败: {err}")
            raise
        
    def _create_tables(self):
        """创建数据库表"""
        try:
            # 设置数据库字符集为UTF-8
            self.cursor.execute("SET NAMES utf8mb4")
            self.cursor.execute("SET CHARACTER SET utf8mb4")
            self.cursor.execute("SET character_set_connection=utf8mb4")
            
            # 创建聊天消息表
            self.cursor.execute('''
                CREATE TABLE IF NOT EXISTS chat_message (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    user_id VARCHAR(255) NOT NULL,
                    user_name VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
                    local_id VARCHAR(255),
                    chat TEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
                    time VARCHAR(255),
                    url TEXT,
                    order_id VARCHAR(255)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            ''')
            
            # 创建订单消息表
            self.cursor.execute('''
                CREATE TABLE IF NOT EXISTS order_message (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    order_id VARCHAR(255) NOT NULL,
                    message TEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            ''')
            
            # 创建订单状态表
            self.cursor.execute('''
                CREATE TABLE IF NOT EXISTS order_status (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    order_id VARCHAR(255) NOT NULL,
                    status VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            ''')
            
            self.conn.commit()
            logger.info("MySQL数据库表创建成功")
        except Exception as e:
            logger.error(f"创建MySQL数据库表时发生错误: {str(e)}")
            raise
            
    def save_chat_message(self, user_id, user_name, local_id, chat, url=None, order_id=None):
        """保存聊天消息"""
        try:
            # 确保所有文本字段使用UTF-8编码
            if user_name:
                user_name = user_name.encode('utf-8').decode('utf-8')
            if chat:
                chat = chat.encode('utf-8').decode('utf-8')
            if url:
                url = url.encode('utf-8').decode('utf-8')
            if order_id:
                order_id = order_id.encode('utf-8').decode('utf-8')
                
            current_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
            self.cursor.execute('''
                INSERT INTO chat_message (user_id, user_name, local_id, chat, time, url, order_id)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            ''', (user_id, user_name, local_id, chat, current_time, url, order_id))
            self.conn.commit()
            return self.cursor.lastrowid
        except Exception as e:
            logger.error(f"保存聊天消息时发生错误: {str(e)}")
            raise
            
    def save_order_message(self, order_id, message):
        """保存订单消息"""
        try:
            self.cursor.execute('''
                INSERT INTO order_message (order_id, message)
                VALUES (%s, %s)
            ''', (order_id, message))
            self.conn.commit()
            return self.cursor.lastrowid
        except Exception as e:
            logger.error(f"保存订单消息时发生错误: {str(e)}")
            raise
            
    def update_order_status(self, order_id, status):
        """更新订单状态"""
        try:
            self.cursor.execute('''
                INSERT INTO order_status (order_id, status)
                VALUES (%s, %s)
            ''', (order_id, status))
            self.conn.commit()
            return self.cursor.lastrowid
        except Exception as e:
            logger.error(f"更新订单状态时发生错误: {str(e)}")
            raise
            
    def get_chat_messages(self, user_id=None, order_id=None, limit=100):
        """获取聊天消息"""
        try:
            if user_id and order_id:
                self.cursor.execute('''
                    SELECT * FROM chat_message 
                    WHERE user_id = %s AND order_id = %s
                    ORDER BY time DESC 
                    LIMIT %s
                ''', (user_id, order_id, limit))
            elif user_id:
                self.cursor.execute('''
                    SELECT * FROM chat_message 
                    WHERE user_id = %s 
                    ORDER BY time DESC 
                    LIMIT %s
                ''', (user_id, limit))
            elif order_id:
                self.cursor.execute('''
                    SELECT * FROM chat_message 
                    WHERE order_id = %s 
                    ORDER BY time DESC 
                    LIMIT %s
                ''', (order_id, limit))
            else:
                self.cursor.execute('''
                    SELECT * FROM chat_message 
                    ORDER BY time DESC 
                    LIMIT %s
                ''', (limit,))
            return self.cursor.fetchall()
        except Exception as e:
            logger.error(f"获取聊天消息时发生错误: {str(e)}")
            raise
            
    def get_order_messages(self, order_id):
        """获取订单消息"""
        try:
            self.cursor.execute('''
                SELECT * FROM order_message 
                WHERE order_id = %s 
                ORDER BY id DESC
            ''', (order_id,))
            return self.cursor.fetchall()
        except Exception as e:
            logger.error(f"获取订单消息时发生错误: {str(e)}")
            raise
            
    def get_order_status(self, order_id):
        """获取订单状态"""
        try:
            self.cursor.execute('''
                SELECT * FROM order_status 
                WHERE order_id = %s 
                ORDER BY id DESC 
                LIMIT 1
            ''', (order_id,))
            return self.cursor.fetchone()
        except Exception as e:
            logger.error(f"获取订单状态时发生错误: {str(e)}")
            raise
            
    def close(self):
        """关闭数据库连接"""
        try:
            self.conn.close()
            logger.info("MySQL数据库连接已关闭")
        except Exception as e:
            logger.error(f"关闭MySQL数据库连接时发生错误: {str(e)}")
            raise 