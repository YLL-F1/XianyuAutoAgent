import base64
import json
import asyncio
import time
import os
import websockets
import redis
import threading
from loguru import logger
from dotenv import load_dotenv
from XianyuApis import XianyuApis
from mysql_manager import XianyuMySQLManager
from utils.xianyu_utils import generate_mid, generate_uuid, trans_cookies, generate_device_id, decrypt
from XianyuAgent import DifyAgent

class XianyuLive:
    def __init__(self, cookies_str):
        self.xianyu = XianyuApis()
        self.base_url = 'wss://wss-goofish.dingtalk.com/'
        self.cookies_str = cookies_str
        self.cookies = trans_cookies(cookies_str)
        self.myid = self.cookies['unb']
        self.device_id = generate_device_id(self.myid)
        self.db_manager = XianyuMySQLManager()  # 使用MySQL管理器
        
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

    def start_workers(self):
        """启动工作线程"""
        for i in range(self.max_workers):
            thread = threading.Thread(
                target=self.message_worker,
                name=f'MessageWorker-{i}',
                daemon=True
            )
            thread.start()
            self.worker_threads.append(thread)
        logger.info(f"已启动 {self.max_workers} 个工作线程")

    def stop_workers(self):
        """停止工作线程"""
        self.stop_event.set()
        for thread in self.worker_threads:
            thread.join(timeout=5)
        self.worker_threads.clear()
        self.stop_event.clear()
        logger.info("所有工作线程已停止")

    def message_worker(self):
        """消息处理工作线程"""
        while not self.stop_event.is_set():
            try:
                # 从Redis队列中获取消息
                message_data = self.redis_client.brpoplpush(
                    self.message_queue_key,
                    self.processing_queue_key,
                    timeout=1
                )
                
                if not message_data:
                    continue
                
                try:
                    # 解析消息数据
                    data = json.loads(message_data)
                    message = data['message']
                    websocket = self.ws  # 使用当前的WebSocket连接
                    
                    # 处理消息
                    asyncio.run(self._handle_message(message, websocket))
                    
                    # 处理完成后从处理队列中移除
                    self.redis_client.lrem(self.processing_queue_key, 0, message_data)
                    
                except Exception as e:
                    logger.error(f"处理消息时发生错误: {str(e)}")
                    # 将处理失败的消息移回主队列
                    self.redis_client.lpush(self.message_queue_key, message_data)
                    self.redis_client.lrem(self.processing_queue_key, 0, message_data)
                    
            except Exception as e:
                logger.error(f"工作线程发生错误: {str(e)}")
                time.sleep(1)  # 发生错误时等待1秒后继续

    async def handle_message(self, message_data, websocket):
        """将消息放入Redis队列"""
        try:
            # 检查是否为同步包消息
            if not self.is_sync_package(message_data):
                return

            # 获取并解密数据
            sync_data = message_data["body"]["syncPushPackage"]["data"][0]
            
            # 检查是否有必要的字段
            if "data" not in sync_data:
                return

            # 将消息数据序列化并放入Redis队列
            data = {
                'message': message_data,
                'timestamp': time.time()
            }
            self.redis_client.lpush(self.message_queue_key, json.dumps(data))
            logger.debug("消息已放入Redis队列")
        except Exception as e:
            logger.error(f"将消息放入Redis队列时发生错误: {str(e)}")

    async def _handle_message(self, message_data, websocket):
        """实际处理消息的方法"""
        try:
            # 发送通用ACK响应
            try:
                message = message_data
                ack = {
                    "code": 200,
                    "headers": {
                        "mid": message["headers"]["mid"] if "mid" in message["headers"] else generate_mid(),
                        "sid": message["headers"]["sid"] if "sid" in message["headers"] else '',
                    }
                }
                if 'app-key' in message["headers"]:
                    ack["headers"]["app-key"] = message["headers"]["app-key"]
                if 'ua' in message["headers"]:
                    ack["headers"]["ua"] = message["headers"]["ua"]
                if 'dt' in message["headers"]:
                    ack["headers"]["dt"] = message["headers"]["dt"]
                await websocket.send(json.dumps(ack))
            except Exception as e:
                pass

            # 如果不是同步包消息，直接返回
            if not self.is_sync_package(message_data):
                return

            # 获取并解密数据
            sync_data = message_data["body"]["syncPushPackage"]["data"][0]
            
            # 检查是否有必要的字段
            if "data" not in sync_data:
                logger.debug("同步包中无data字段")
                return

            # 解密数据
            try:
                data = sync_data["data"]
                try:
                    data = base64.b64decode(data).decode("utf-8")
                    data = json.loads(data)
                    # logger.info(f"无需解密 message: {data}")
                    return
                except Exception as e:
                    # logger.info(f'加密数据: {data}')
                    decrypted_data = decrypt(data)
                    message = json.loads(decrypted_data)
            except Exception as e:
                logger.error(f"消息解密失败: {e}")
                return
            print("ddddd",message)
            try:
                # 判断是否为订单消息
                if message['3']['redReminder'] == '等待买家付款':
                    user_id = message['1'].split('@')[0]  # 使用user_id作为order_id
                    user_url = f'https://www.goofish.com/personal?userId={user_id}'
                    
                    # 创建初始订单状态
                    self.db_manager.update_order_status(user_id, '等待买家付款')
                    logger.info(f"订单状态已更新: {user_id} -> 等待买家付款")
                    
                    # 保存订单消息
                    self.db_manager.save_order_message(
                        order_id=user_id,  # 使用user_id作为order_id
                        message='订单创建，等待买家付款',
                        user_url=user_url
                    )
                    logger.info(f"订单消息已保存: {user_id} -> 订单创建，等待买家付款")
                    logger.info(f'等待买家 {user_url} 付款')
                    return
                    
                elif message['3']['redReminder'] == '交易关闭':
                    user_id = message['1'].split('@')[0]  # 使用user_id作为order_id
                    user_url = f'https://www.goofish.com/personal?userId={user_id}'
                    
                    # 更新订单状态
                    self.db_manager.update_order_status(user_id, '交易关闭')
                    logger.info(f"订单状态已更新: {user_id} -> 交易关闭")
                    
                    # 保存订单消息
                    self.db_manager.save_order_message(
                        order_id=user_id,  # 使用user_id作为order_id
                        message='交易已关闭',
                        user_url=user_url
                    )
                    logger.info(f"订单消息已保存: {user_id} -> 交易已关闭")
                    logger.info(f'卖家 {user_url} 交易关闭')
                    return
                    
                elif message['3']['redReminder'] == '等待卖家发货':
                    user_id = message['1'].split('@')[0]  # 使用user_id作为order_id
                    user_url = f'https://www.goofish.com/personal?userId={user_id}'
                    
                    # 更新订单状态
                    self.db_manager.update_order_status(user_id, '等待卖家发货')
                    logger.info(f"订单状态已更新: {user_id} -> 等待卖家发货")
                    
                    # 保存订单消息
                    self.db_manager.save_order_message(
                        order_id=user_id,  # 使用user_id作为order_id
                        message='买家已付款，等待卖家发货',
                        user_url=user_url
                    )
                    logger.info(f"订单消息已保存: {user_id} -> 买家已付款，等待卖家发货")
                    logger.info(f'交易成功 {user_url} 等待卖家发货')
                    return

            except Exception as e:
                logger.error(f"处理订单消息时发生错误: {str(e)}")
                pass

            # 判断消息类型
            if self.is_typing_status(message):
                logger.debug("用户正在输入")
                return
            elif not self.is_chat_message(message):
                logger.debug("其他非聊天消息")
                logger.debug(f"原始消息: {message}")
                return

            # 处理聊天消息
            create_time = int(message["1"]["5"])
            send_user_name = message["1"]["10"]["reminderTitle"]
            send_user_id = message["1"]["10"]["senderUserId"]
            send_message = message["1"]["10"]["reminderContent"]
            order_id = message["1"]["2"].split('@')[0]
            url_info = message["1"]["10"]["reminderUrl"]
            
            # 时效性验证（过滤5分钟前消息）
            if (time.time() * 1000 - create_time) > 300000:
                logger.debug("过期消息丢弃")
                return
            
            # 构造消息数据
            chat_data = {
                'user_id': send_user_id,
                'user_name': send_user_name,
                'local_id': self.myid,
                'chat': send_message,
                'url': url_info,
                'order_id': order_id,
                'timestamp': time.time()
            }
            
            # 记录首次消息时间
            if order_id not in self.order_first_message_time:
                self.order_first_message_time[order_id] = time.time()
            
            # 将消息添加到对应order_id的列表中
            self.redis_client.lpush(
                f"{self.chat_messages_key}:{order_id}",
                json.dumps(chat_data)
            )
            # 设置24小时过期
            self.redis_client.expire(f"{self.chat_messages_key}:{order_id}", 86400)
            
            logger.info(f"已将消息存入Redis - order_id: {order_id}, message: {send_message}")
            
        except Exception as e:
            logger.error(f"处理消息时发生错误: {str(e)}")
            logger.debug(f"原始消息: {message_data}")

    async def send_msg(self, ws, cid, toid, text):
        text = {
            "contentType": 1,
            "text": {
                "text": text
            }
        }
        text_base64 = str(base64.b64encode(json.dumps(text).encode('utf-8')), 'utf-8')
        msg = {
            "lwp": "/r/MessageSend/sendByReceiverScope",
            "headers": {
                "mid": generate_mid()
            },
            "body": [
                {
                    "uuid": generate_uuid(),
                    "cid": f"{cid}@goofish",
                    "conversationType": 1,
                    "content": {
                        "contentType": 101,
                        "custom": {
                            "type": 1,
                            "data": text_base64
                        }
                    },
                    "redPointPolicy": 0,
                    "extension": {
                        "extJson": "{}"
                    },
                    "ctx": {
                        "appVersion": "1.0",
                        "platform": "web"
                    },
                    "mtags": {},
                    "msgReadStatusSetting": 1
                },
                {
                    "actualReceivers": [
                        f"{toid}@goofish",
                        f"{self.myid}@goofish"
                    ]
                }
            ]
        }
        await ws.send(json.dumps(msg))

    async def init(self, ws):
        token = self.xianyu.get_token(self.cookies, self.device_id)['data']['accessToken']
        msg = {
            "lwp": "/reg",
            "headers": {
                "cache-header": "app-key token ua wv",
                "app-key": "444e9908a51d1cb236a27862abc769c9",
                "token": token,
                "ua": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36 DingTalk(2.1.5) OS(Windows/10) Browser(Chrome/133.0.0.0) DingWeb/2.1.5 IMPaaS DingWeb/2.1.5",
                "dt": "j",
                "wv": "im:3,au:3,sy:6",
                "sync": "0,0;0;0;",
                "did": self.device_id,
                "mid": generate_mid()
            }
        }
        await ws.send(json.dumps(msg))
        # 等待一段时间，确保连接注册完成
        await asyncio.sleep(1)
        msg = {"lwp": "/r/SyncStatus/ackDiff", "headers": {"mid": "5701741704675979 0"}, "body": [
            {"pipeline": "sync", "tooLong2Tag": "PNM,1", "channel": "sync", "topic": "sync", "highPts": 0,
             "pts": int(time.time() * 1000) * 1000, "seq": 0, "timestamp": int(time.time() * 1000)}]}
        await ws.send(json.dumps(msg))
        logger.info('连接注册完成')

    def is_chat_message(self, message):
        """判断是否为用户聊天消息"""
        try:
            return (
                isinstance(message, dict) 
                and "1" in message 
                and isinstance(message["1"], dict)  # 确保是字典类型
                and "10" in message["1"]
                and isinstance(message["1"]["10"], dict)  # 确保是字典类型
                and "reminderContent" in message["1"]["10"]
            )
        except Exception:
            return False

    def is_sync_package(self, message_data):
        """判断是否为同步包消息"""
        try:
            return (
                isinstance(message_data, dict)
                and "body" in message_data
                and "syncPushPackage" in message_data["body"]
                and "data" in message_data["body"]["syncPushPackage"]
                and len(message_data["body"]["syncPushPackage"]["data"]) > 0
            )
        except Exception:
            return False

    def is_typing_status(self, message):
        """判断是否为用户正在输入状态消息"""
        try:
            return (
                isinstance(message, dict)
                and "1" in message
                and isinstance(message["1"], list)
                and len(message["1"]) > 0
                and isinstance(message["1"][0], dict)
                and "1" in message["1"][0]
                and isinstance(message["1"][0]["1"], str)
                and "@goofish" in message["1"][0]["1"]
            )
        except Exception:
            return False

    async def send_heartbeat(self, ws):
        """发送心跳包并等待响应"""
        try:
            heartbeat_mid = generate_mid()
            heartbeat_msg = {
                "lwp": "/!",
                "headers": {
                    "mid": heartbeat_mid
                }
            }
            await ws.send(json.dumps(heartbeat_msg))
            self.last_heartbeat_time = time.time()
            logger.debug("心跳包已发送")
            return heartbeat_mid
        except Exception as e:
            logger.error(f"发送心跳包失败: {e}")
            raise

    async def heartbeat_loop(self, ws):
        """心跳维护循环"""
        while True:
            try:
                current_time = time.time()
                
                # 检查是否需要发送心跳
                if current_time - self.last_heartbeat_time >= self.heartbeat_interval:
                    await self.send_heartbeat(ws)
                
                # 检查上次心跳响应时间，如果超时则认为连接已断开
                if (current_time - self.last_heartbeat_response) > (self.heartbeat_interval + self.heartbeat_timeout):
                    logger.warning("心跳响应超时，可能连接已断开")
                    break
                
                await asyncio.sleep(1)
            except Exception as e:
                logger.error(f"心跳循环出错: {e}")
                break

    async def handle_heartbeat_response(self, message_data):
        """处理心跳响应"""
        try:
            if (
                isinstance(message_data, dict)
                and "headers" in message_data
                and "mid" in message_data["headers"]
                and "code" in message_data
                and message_data["code"] == 200
            ):
                self.last_heartbeat_response = time.time()
                logger.debug("收到心跳响应")
                return True
        except Exception as e:
            logger.error(f"处理心跳响应出错: {e}")
        return False

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
                                logger.debug(f"原始消息: {message}")

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
                    await asyncio.sleep(5)  # 等待5秒后重连
        finally:
            # 确保在程序退出时停止工作线程
            self.stop_workers()

    def batch_process_messages(self):
        """处理达到5秒时间阈值的order_id消息"""
        while True:
            try:
                current_time = time.time()
                
                # 获取所有活跃的order_id
                pattern = f"{self.chat_messages_key}:*"
                order_keys = self.redis_client.keys(pattern)
                
                for order_key in order_keys:
                    try:
                        order_id = order_key.split(':')[2]
                        
                        # 检查是否已经过了5秒
                        first_message_time = self.order_first_message_time.get(order_id)
                        if not first_message_time or (current_time - first_message_time) < 5:
                            continue  # 跳过未到5秒的order_id
                            
                        # 获取Redis中该order_id的所有消息
                        messages = []
                        while True:
                            msg_data = self.redis_client.rpop(order_key)
                            if not msg_data:
                                break
                            messages.append(json.loads(msg_data))
                        
                        if not messages:
                            # 清理首次消息时间记录
                            self.order_first_message_time.pop(order_id, None)
                            continue
                            
                        # 按时间排序
                        messages.sort(key=lambda x: x['timestamp'])
                        
                        # 获取MySQL中最近5条历史消息
                        history_messages = self.db_manager.get_chat_messages(
                            order_id=order_id,
                            limit=5
                        )
                        
                        # 构建完整对话上下文
                        context = []
                        
                        # 添加历史消息
                        for msg in history_messages:
                            context.append(f"{msg['user_name']}: {msg['chat']}")
                        
                        # 添加Redis中的新消息
                        for msg in messages:
                            context.append(f"{msg['user_name']}: {msg['chat']}")
                        
                        # 合并上下文
                        full_context = "\n".join(context)
                        
                        # 生成回复
                        bot_reply = bot.generate(
                            user_msg=full_context,
                            user_id=messages[-1]['user_id'],
                            order_id=order_id
                        )
                        
                        # 保存所有新消息到MySQL
                        for msg in messages:
                            self.db_manager.save_chat_message(
                                user_id=msg['user_id'],
                                user_name=msg['user_name'],
                                local_id=msg['local_id'],
                                chat=msg['chat'],
                                url=msg['url'],
                                order_id=msg['order_id']
                            )
                        
                        # 保存机器人回复到MySQL
                        self.db_manager.save_chat_message(
                            user_id=messages[-1]['user_id'],
                            user_name="me",
                            local_id=self.myid,
                            chat=bot_reply,
                            url=messages[-1]['url'],
                            order_id=order_id
                        )
                        
                        # 发送回复
                        asyncio.run(self.send_msg(
                            self.ws,
                            order_id,
                            messages[-1]['user_id'],
                            bot_reply
                        ))
                        
                        logger.info(f"批量处理完成 - order_id: {order_id}, reply: {bot_reply}")
                        
                        # 清理已处理的order_id的首次消息时间
                        self.order_first_message_time.pop(order_id, None)
                        
                    except Exception as e:
                        logger.error(f"处理order_id {order_id}的消息时出错: {str(e)}")
                        # 出错时也清理首次消息时间，避免消息卡住
                        self.order_first_message_time.pop(order_id, None)
                
                # 短暂休眠以减少CPU使用
                time.sleep(0.1)
                
            except Exception as e:
                logger.error(f"批量处理消息时发生错误: {str(e)}")
                time.sleep(0.1)

if __name__ == '__main__':
    #加载环境变量 cookie
    load_dotenv()
    cookies_str = os.getenv("COOKIES_STR")
    bot = DifyAgent()
    xianyuLive = XianyuLive(cookies_str)
    # 常驻进程
    asyncio.run(xianyuLive.main())
