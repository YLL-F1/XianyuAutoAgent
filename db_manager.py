import sqlite3
import time
from loguru import logger

class XianyuDBManager:
    def __init__(self, db_path="data/xianyu.db"):
        """初始化数据库管理器"""
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path)
        self.cursor = self.conn.cursor()
        self._create_tables()
        
    def _create_tables(self):
        """创建数据库表"""
        try:
            # 创建聊天消息表
            self.cursor.execute('''
                CREATE TABLE IF NOT EXISTS chat_message (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id TEXT NOT NULL,
                    user_name TEXT,
                    local_id TEXT,
                    chat TEXT,
                    time TEXT,
                    url TEXT,
                    order_id TEXT
                )
            ''')
            
            # 创建订单消息表
            self.cursor.execute('''
                CREATE TABLE IF NOT EXISTS order_message (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    order_id TEXT NOT NULL,
                    message TEXT,
                    time TEXT,
                    user_url TEXT
                )
            ''')
            
            # 创建订单状态表
            self.cursor.execute('''
                CREATE TABLE IF NOT EXISTS order_status (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    order_id TEXT NOT NULL,
                    status TEXT,
                    time TEXT
                )
            ''')
            
            self.conn.commit()
            logger.info("数据库表创建成功")
        except Exception as e:
            logger.error(f"创建数据库表时发生错误: {str(e)}")
            raise
            
    def save_chat_message(self, user_id, user_name, local_id, chat, url=None, order_id=None):
        """保存聊天消息"""
        try:
            current_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
            self.cursor.execute('''
                INSERT INTO chat_message (user_id, user_name, local_id, chat, time, url, order_id)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (user_id, user_name, local_id, chat, current_time, url, order_id))
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
            current_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
            self.cursor.execute('''
                INSERT INTO order_message (order_id, message, time, user_url)
                VALUES (?, ?, ?, ?)
            ''', (order_id, message, current_time, user_url))
            self.conn.commit()
            return self.cursor.lastrowid
        except Exception as e:
            logger.error(f"保存订单消息时发生错误: {str(e)}")
            raise
            
    def update_order_status(self, order_id, status):
        """更新订单状态"""
        try:
            current_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
            self.cursor.execute('''
                INSERT INTO order_status (order_id, status, time)
                VALUES (?, ?, ?)
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
                    WHERE user_id = ? AND order_id = ?
                    ORDER BY time DESC 
                    LIMIT ?
                ''', (user_id, order_id, limit))
            elif user_id:
                self.cursor.execute('''
                    SELECT * FROM chat_message 
                    WHERE user_id = ? 
                    ORDER BY time DESC 
                    LIMIT ?
                ''', (user_id, limit))
            elif order_id:
                self.cursor.execute('''
                    SELECT * FROM chat_message 
                    WHERE order_id = ? 
                    ORDER BY time DESC 
                    LIMIT ?
                ''', (order_id, limit))
            else:
                self.cursor.execute('''
                    SELECT * FROM chat_message 
                    ORDER BY time DESC 
                    LIMIT ?
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
                WHERE order_id = ? 
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
                WHERE order_id = ? 
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
            logger.info("数据库连接已关闭")
        except Exception as e:
            logger.error(f"关闭数据库连接时发生错误: {str(e)}")
            raise 