import mysql.connector
import time
from loguru import logger
from dotenv import load_dotenv
import os
import random

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
            self._update_tables_charset()  # 更新现有表的字符集
            logger.info(f"成功连接到MySQL数据库: {host}:{port}")
        except Exception as e:
            logger.error(f"连接MySQL数据库失败: {str(e)}")
            raise
        
    def _random_delay(self):
        """随机延迟0.5-1.5秒"""
        delay = random.uniform(0.5, 1.5)
        time.sleep(delay)
        
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
                    order_id VARCHAR(255),
                    chat_type VARCHAR(50) DEFAULT 'text',
                    city VARCHAR(255) DEFAULT NULL,
                    country VARCHAR(255) DEFAULT NULL,
                    platform VARCHAR(50) DEFAULT NULL,
                    client_ip VARCHAR(50) DEFAULT NULL
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            ''')
            
            # 创建订单消息表
            self.cursor.execute('''
                CREATE TABLE IF NOT EXISTS order_message (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    order_id VARCHAR(255) NOT NULL,
                    message TEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
                    time VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
                    user_url TEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            ''')
            
            # 创建订单状态表
            self.cursor.execute('''
                CREATE TABLE IF NOT EXISTS order_status (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    order_id VARCHAR(255) NOT NULL,
                    status VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
                    time VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
                    handler_info INT DEFAULT 0
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            ''')
            
            self.conn.commit()
            logger.info("MySQL数据库表创建成功")
        except Exception as e:
            logger.error(f"创建MySQL数据库表时发生错误: {str(e)}")
            raise
            
    def _update_tables_charset(self):
        """更新现有表的字符集为UTF-8"""
        try:
            # 更新order_status表
            self.cursor.execute('''
                ALTER TABLE order_status 
                MODIFY COLUMN status VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci
            ''')
            
            # 更新order_message表
            self.cursor.execute('''
                ALTER TABLE order_message 
                MODIFY COLUMN message TEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci
            ''')
            
            self.conn.commit()
            logger.info("成功更新表字符集为UTF-8")
        except Exception as e:
            logger.error(f"更新表字符集时发生错误: {str(e)}")
            # 不抛出异常，因为表可能还不存在
            
    def save_chat_message(self, user_id, user_name, local_id, chat, url=None, order_id=None, chat_type='text', city=None, country=None, platform=None, client_ip=None):
        """保存聊天消息
        Args:
            user_id: 用户ID
            user_name: 用户名
            local_id: 本地ID
            chat: 聊天内容
            url: URL，可选
            order_id: 订单ID，可选
            chat_type: 消息类型，默认为'text'
            city: 城市，可选
            country: 国家，可选
            platform: 平台，可选
            client_ip: 客户端IP，可选
        """
        try:
            # 添加随机延迟
            self._random_delay()
            
            # 确保所有文本字段使用UTF-8编码
            if user_name:
                user_name = user_name.encode('utf-8').decode('utf-8')
            if chat:
                chat = chat.encode('utf-8').decode('utf-8')
            if url:
                url = url.encode('utf-8').decode('utf-8')
            if order_id:
                order_id = order_id.encode('utf-8').decode('utf-8')
            if city:
                city = city.encode('utf-8').decode('utf-8')
            if country:
                country = country.encode('utf-8').decode('utf-8')
            if platform:
                platform = platform.encode('utf-8').decode('utf-8')
            if client_ip:
                client_ip = client_ip.encode('utf-8').decode('utf-8')
                
            current_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
            self.cursor.execute('''
                INSERT INTO chat_message (user_id, user_name, local_id, chat, time, url, order_id, chat_type, city, country, platform, client_ip)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ''', (user_id, user_name, local_id, chat, current_time, url, order_id, chat_type, city, country, platform, client_ip))
            self.conn.commit()
            return self.cursor.lastrowid
        except Exception as e:
            logger.error(f"保存聊天消息时发生错误: {str(e)}")
            raise
            
    def save_order_message(self, order_id, message, user_url=None):
        """保存订单消息
        Args:
            order_id: 订单ID
            message: 消息内容
            user_url: 用户URL，可选
        """
        try:
            # 添加随机延迟
            self._random_delay()
            
            current_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
            self.cursor.execute('''
                INSERT INTO order_message (order_id, message, time, user_url)
                VALUES (%s, %s, %s, %s)
            ''', (order_id, message, current_time, user_url))
            self.conn.commit()
            return self.cursor.lastrowid
        except Exception as e:
            logger.error(f"保存订单消息时发生错误: {str(e)}")
            raise
            
    def update_order_status(self, order_id, status):
        """更新订单状态
        Args:
            order_id: 订单ID
            status: 订单状态
        """
        try:
            # 添加随机延迟
            self._random_delay()
            
            current_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
            
            # 检查订单ID是否存在
            self.cursor.execute('''
                SELECT id FROM order_status WHERE order_id = %s
            ''', (order_id,))
            result = self.cursor.fetchone()
            
            if result:
                # 如果订单ID存在，更新状态
                self.cursor.execute('''
                    UPDATE order_status 
                    SET status = %s, time = %s 
                    WHERE order_id = %s
                ''', (status, current_time, order_id))
            else:
                # 如果订单ID不存在，插入新记录
                self.cursor.execute('''
                    INSERT INTO order_status (order_id, status, time)
                    VALUES (%s, %s, %s)
                ''', (order_id, status, current_time))
                
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