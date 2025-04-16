import base64
import json
import asyncio
import time
import os
import websockets
import redis
import threading
import ssl
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
            order_id = message["1"]["2"].split('@')[0]  # 获取订单ID
            
            # 时效性验证（过滤5分钟前消息）
            if (time.time() * 1000 - create_time) > 300000:
                logger.debug("过期消息丢弃")
                return
                
            #if send_user_id == self.myid:
            #    logger.debug("过滤自身消息")
            #    return
                
            url_info = message["1"]["10"]["reminderUrl"]
            item_id = url_info.split("itemId=")[1].split("&")[0] if "itemId=" in url_info else None
            
            if not item_id:
                logger.warning("无法获取商品ID")
                return
                
            item_info = self.xianyu.get_item_info(self.cookies, item_id)['data']['itemDO']
            item_description = f"{item_info['desc']};当前商品售卖价格为:{str(item_info['soldPrice'])}"
            item_image_url = item_info.get('images', [{}])[0].get('url') if item_info.get('images') else None
            
            # 保存聊天消息到数据库
            try:
                self.db_manager.save_chat_message(
                    user_id=send_user_id,  # 使用发送者ID
                    user_name=send_user_name,
                    local_id=self.myid,  # 使用机器人ID作为local_id
                    chat=send_message,
                    url=url_info,  # 保存完整的URL信息
                    order_id=order_id  # 保存订单ID
                )
                logger.info(f"已保存聊天消息到数据库: {send_user_name} - {send_message}")
            except Exception as e:
                logger.error(f"保存聊天消息到数据库时发生错误: {str(e)}")
            
            logger.info(f"user: {send_user_name}, 发送消息: {send_message}")
            
            # 添加用户消息到上下文
            #self.context_manager.add_message(send_user_id, item_id, "user", send_message)
            
            # 获取完整的对话上下文
            #context = self.context_manager.get_context(send_user_id, item_id)
            
            # 生成回复
            bot_reply = bot.generate(
                user_msg=send_message,
                user_id=send_user_id,
                order_id=order_id
            )
            
            # 保存机器人回复到数据库
            try:
                self.db_manager.save_chat_message(
                    user_id=send_user_id,  # 使用发送者ID
                    user_name="me",  # 机器人名称
                    local_id=self.myid,  # 使用机器人ID作为local_id
                    chat=bot_reply,
                    url=url_info,  # 保存相同的URL信息
                    order_id=order_id  # 保存相同的订单ID
                )
                logger.info(f"已保存机器人回复到数据库: {bot_reply}")
            except Exception as e:
                logger.error(f"保存机器人回复到数据库时发生错误: {str(e)}")
            
            logger.info(f"机器人回复: {bot_reply}")
            cid = message["1"]["2"].split('@')[0]
            await self.send_msg(websocket, cid, send_user_id, bot_reply)
            
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

    async def connect(self):
        """建立WebSocket连接"""
        try:
            # 添加SSL上下文配置
            ssl_context = ssl.create_default_context()
            ssl_context.check_hostname = False
            ssl_context.verify_mode = ssl.CERT_NONE
            
            # 添加连接超时和重试逻辑
            for attempt in range(3):  # 最多重试3次
                try:
                    self.ws = await websockets.connect(
                        self.base_url,
                        ssl=ssl_context,
                        ping_interval=20,  # 每20秒发送一次ping
                        ping_timeout=10,   # ping超时时间为10秒
                        close_timeout=5,   # 关闭超时时间为5秒
                        max_size=2**20     # 最大消息大小为1MB
                    )
                    logger.info("WebSocket连接成功")
                    return
                except (websockets.exceptions.InvalidStatusCode,
                       websockets.exceptions.InvalidHandshake,
                       websockets.exceptions.InvalidMessage,
                       websockets.exceptions.ConnectionClosed,
                       ssl.SSLError,
                       OSError) as e:
                    if attempt < 2:  # 如果不是最后一次尝试
                        wait_time = (attempt + 1) * 5  # 递增等待时间
                        logger.warning(f"WebSocket连接失败，{wait_time}秒后重试: {str(e)}")
                        await asyncio.sleep(wait_time)
                    else:
                        raise
        except Exception as e:
            logger.error(f"WebSocket连接失败: {str(e)}")
            raise

    async def handle_message(self, message):
        """处理接收到的消息"""
        try:
            # 将消息放入Redis队列
            message_data = {
                'message': message,
                'timestamp': time.time()
            }
            self.redis_client.rpush(self.message_queue_key, json.dumps(message_data))
            logger.debug(f"消息已加入队列: {message}")
        except Exception as e:
            logger.error(f"处理消息时发生错误: {str(e)}")
            # 如果Redis操作失败，尝试重新连接
            try:
                self.redis_client = redis.Redis(
                    host='localhost',
                    port=6379,
                    db=0,
                    socket_timeout=5,
                    socket_connect_timeout=5,
                    retry_on_timeout=True
                )
                # 重试一次
                self.redis_client.rpush(self.message_queue_key, json.dumps(message_data))
            except Exception as retry_error:
                logger.error(f"重试Redis操作失败: {str(retry_error)}")

    async def main(self):
        """主函数"""
        try:
            # 启动工作线程
            self.start_workers()
            
            while True:
                try:
                    # 建立WebSocket连接
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
                    
        except Exception as e:
            logger.error(f"主循环发生错误: {str(e)}")
        finally:
            # 确保在程序退出时停止工作线程
            self.stop_workers()
            if hasattr(self, 'ws') and self.ws:
                await self.ws.close()

if __name__ == '__main__':
    #加载环境变量 cookie
    load_dotenv()
    cookies_str = os.getenv("COOKIES_STR")
    bot = DifyAgent()
    xianyuLive = XianyuLive(cookies_str)
    # 常驻进程
    asyncio.run(xianyuLive.main())
