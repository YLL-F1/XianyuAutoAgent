import base64
import json
import time
import os
import sys
import asyncio
import requests
from loguru import logger
from utils.xianyu_utils import generate_mid, generate_uuid, decrypt

def get_city_by_ip(ip):
    # 通过 http://ip-api.com/json/{ip} 获取城市和国家
    try:
        if not ip:
            return "", ""
        url = f"http://ip-api.com/json/{ip}"
        resp = requests.get(url, timeout=2)
        if resp.status_code == 200:
            data = resp.json()
            if data.get("status") == "success":
                return data.get("country", ""), data.get("regionName", "")
    except Exception as e:
        logger.error(f"IP归属地查询失败: {e}")
    return "", ""

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

                # 新增：把订单信息存入Redis，设置10秒过期
                self.redis_client.setex(f"xianyu:order_wait_ship:{user_id}", 10, "wait_ship")
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
        elif message['1']['7'] == 1:
            logger.debug("系统消息")
            return
        else:
            logger.debug("聊天消息")

        # 处理聊天消息
        create_time = int(message["1"]["5"])
        send_user_name = message["1"]["10"]["reminderTitle"]
        send_user_id = message["1"]["10"]["senderUserId"]
        order_id = message["1"]["2"].split('@')[0]
        url_info = message["1"]["10"]["reminderUrl"]
        platform = message["1"]["10"].get("_platform", "")
        client_ip = message["1"]["10"].get("clientIp", "")
        country, city = get_city_by_ip(client_ip)
        # 判断消息类型和内容
        chat_type = 'text'  # 默认为文本类型
        chat_content = message["1"]["10"]["reminderContent"]  # 默认为提醒内容

        # 检查消息类型
        if "6" in message["1"] and isinstance(message["1"]["6"], dict):
            msg_type = message["1"]["6"].get("1")
            if msg_type == 101:  # 系统消息
                msg_content = message["1"]["6"].get("3", {})
                if isinstance(msg_content, dict):
                    content_type = msg_content.get("4")
                    if content_type == 2:  # 图片消息
                        chat_type = 'image'
                        # 从图片数据中提取URL
                        try:
                            image_data = json.loads(msg_content.get("5", "{}"))
                            if "image" in image_data and "pics" in image_data["image"]:
                                pics = image_data["image"]["pics"]
                                if pics and len(pics) > 0:
                                    chat_content = pics[0].get("url", "")
                        except Exception as e:
                            logger.error(f"解析图片数据失败: {e}")
                            chat_content = "[图片解析失败]"

        # 时效性验证（过滤5分钟前消息）
        if (time.time() * 1000 - create_time) > 300000:
            logger.debug("过期消息丢弃")
            return

        # if send_user_id == self.myid:
        #     logger.debug("过滤自身消息")
        #     return

        # 构造消息数据
        chat_data = {
            'user_id': send_user_id,
            'user_name': send_user_name,
            'local_id': self.myid,
            'chat': chat_content,
            'url': url_info,
            'order_id': order_id,
            'timestamp': time.time(),
            'chat_type': chat_type,
            'platform': platform,
            'client_ip': client_ip,
            'city': city,
            'country': country
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
        
        logger.info(f"已将消息存入Redis - order_id: {order_id}, message: {chat_content}")
        
    except Exception as e:
        logger.error(f"处理消息时发生错误: {str(e)}")
        logger.debug(f"原始消息: {message_data}")

async def send_msg(self, ws, cid, toid, text):
    """发送消息"""
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
    try:
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
    except Exception as e:
        if "'accessToken'" in str(e):
            logger.error("accessToken 错误，正在重启程序...")
            # 重启程序
            os.execv(sys.executable, [sys.executable] + sys.argv)
        else:
            raise e

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

def save_chat_messages(self, messages):
    """保存聊天消息"""
    for msg in messages:
        self.db_manager.save_chat_message(
            user_id=msg['user_id'],
            user_name=msg['user_name'],
            local_id=msg['local_id'],
            chat=msg['chat'],
            url=msg['url'],
            order_id=msg['order_id'],
            chat_type=msg.get('chat_type', 'text'),
            city=msg.get('city', None),
            country=msg.get('country', None),
            platform=msg.get('platform', None),
            client_ip=msg.get('client_ip', None)
        ) 