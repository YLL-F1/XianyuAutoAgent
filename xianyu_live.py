import base64
import json
import asyncio
import time
import os
import websockets
import redis
import threading
import sys
from loguru import logger
from dotenv import load_dotenv
from cookie_fetcher import refresh_and_get_cookies
from XianyuApis import XianyuApis
from mysql_manager import XianyuMySQLManager
from utils.xianyu_utils import generate_mid, generate_uuid, trans_cookies, generate_device_id, decrypt
from XianyuAgent import DifyAgent
from worker import start_workers, stop_workers, message_worker, batch_process_messages, process_wait_ship_orders
from heartbeat import send_heartbeat, heartbeat_loop, handle_heartbeat_response
from message_handler import (
    handle_message, _handle_message, send_msg, init,
    is_chat_message, is_sync_package, is_typing_status
)

class XianyuLive:
    def __init__(self, cookies_str, bot=None):
        # 方法绑定（必须在任何线程启动前）
        self.start_workers = start_workers.__get__(self)
        self.stop_workers = stop_workers.__get__(self)
        self.message_worker = message_worker.__get__(self)
        self.batch_process_messages = batch_process_messages.__get__(self)
        self.process_wait_ship_orders = process_wait_ship_orders.__get__(self)
        self.send_heartbeat = send_heartbeat.__get__(self)
        self.heartbeat_loop = heartbeat_loop.__get__(self)
        self.handle_heartbeat_response = handle_heartbeat_response.__get__(self)
        self.handle_message = handle_message.__get__(self)
        self._handle_message = _handle_message.__get__(self)
        self.send_msg = send_msg.__get__(self)
        self.init = init.__get__(self)
        self.is_chat_message = is_chat_message.__get__(self)
        self.is_sync_package = is_sync_package.__get__(self)
        self.is_typing_status = is_typing_status.__get__(self)

        self.bot = bot
        self.xianyu = XianyuApis()
        self.base_url = 'wss://wss-goofish.dingtalk.com/'
        self.cookies_str = cookies_str
        self.cookies = trans_cookies(cookies_str)
        self.myid = self.cookies['unb']
        self.device_id = generate_device_id(self.myid)
        self.db_manager = XianyuMySQLManager()  # 使用MySQL管理器
        # 批量处理消息的时间阈值（秒）
        self.message_batch_threshold = 5
        
        # Redis配置
        self.redis_client = redis.Redis(
            host='localhost',
            port=6379,
            db=0,
            decode_responses=True
        )
        self.message_queue_key = 'xianyu:messages'
        self.processing_queue_key = 'xianyu:processing'
        self.chat_messages_key = 'xianyu:chat_messages'  # 新增：存储聊天消息的Redis键
        self.order_first_message_time = {}  # 记录每个order_id的首次消息时间
        
        # 线程相关配置
        self.max_workers = 10  # 最大工作线程数
        self.worker_threads = []
        self.stop_event = threading.Event()
        
        # 心跳相关配置
        self.heartbeat_interval = 15  # 心跳间隔15秒
        self.heartbeat_timeout = 5    # 心跳超时5秒
        self.last_heartbeat_time = 0
        self.last_heartbeat_response = 0
        self.heartbeat_task = None
        self.ws = None
        
        #启动定时处理线程
        self.batch_process_thread = threading.Thread(
            target=self.batch_process_messages,
            daemon=True
        )
        self.batch_process_thread.start()

        # 新增：每10秒轮询 Redis，处理订单
        self.order_wait_ship_thread = threading.Thread(
            target=self.process_wait_ship_orders,
            daemon=True
        )
        self.order_wait_ship_thread.start()

    async def main(self):
        # 启动工作线程
        self.start_workers()
        
        try:
            while True:
                try:
                    headers = {
                        "Cookie": self.cookies_str,
                        "Host": "wss-goofish.dingtalk.com",
                        "Connection": "Upgrade",
                        "Pragma": "no-cache",
                        "Cache-Control": "no-cache",
                        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36",
                        "Origin": "https://www.goofish.com",
                        "Accept-Encoding": "gzip, deflate, br, zstd",
                        "Accept-Language": "zh-CN,zh;q=0.9",
                    }

                    async with websockets.connect(self.base_url, extra_headers=headers) as websocket:
                        self.ws = websocket
                        await self.init(websocket)
                        
                        # 初始化心跳时间
                        self.last_heartbeat_time = time.time()
                        self.last_heartbeat_response = time.time()
                        
                        # 启动心跳任务
                        self.heartbeat_task = asyncio.create_task(self.heartbeat_loop(websocket))
                        
                        async for message in websocket:
                            try:
                                message_data = json.loads(message)
                                
                                # 处理心跳响应
                                if await self.handle_heartbeat_response(message_data):
                                    continue
                                
                                # 发送通用ACK响应
                                if "headers" in message_data and "mid" in message_data["headers"]:
                                    ack = {
                                        "code": 200,
                                        "headers": {
                                            "mid": message_data["headers"]["mid"],
                                            "sid": message_data["headers"].get("sid", "")
                                        }
                                    }
                                    # 复制其他可能的header字段
                                    for key in ["app-key", "ua", "dt"]:
                                        if key in message_data["headers"]:
                                            ack["headers"][key] = message_data["headers"][key]
                                    await websocket.send(json.dumps(ack))
                                
                                # 将消息放入Redis队列
                                await self.handle_message(message_data, websocket)

                            except json.JSONDecodeError:
                                logger.error("消息解析失败")
                            except Exception as e:
                                logger.error(f"处理消息时发生错误: {str(e)}")
                                #logger.debug(f"原始消息: {message}")

                except websockets.exceptions.ConnectionClosed:
                    logger.warning("WebSocket连接已关闭")
                    if self.heartbeat_task:
                        self.heartbeat_task.cancel()
                        try:
                            await self.heartbeat_task
                        except asyncio.CancelledError:
                            pass
                    await asyncio.sleep(5)  # 等待5秒后重连
                    
                except Exception as e:
                    logger.error(f"连接发生错误: {e}")
                    if self.heartbeat_task:
                        self.heartbeat_task.cancel()
                        try:
                            await self.heartbeat_task
                        except asyncio.CancelledError:
                            pass
                    # 直接重启程序
                    os.execv(sys.executable, [sys.executable] + sys.argv)
        finally:
            # 确保在程序退出时停止工作线程
            self.stop_workers()

# ... 这里复制 XianyuLive 类的全部内容 ...
# class XianyuLive:
#     def __init__(self, cookies_str):
#         ...
#     # ... 其余方法 ... 