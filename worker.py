import time
import json
import threading
from loguru import logger
import asyncio


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
                        context.append(f"[历史消息] {msg['user_name']}: {msg['chat']}")
                    
                    # 添加Redis中的新消息
                    for msg in messages:
                        context.append(f"{msg['user_name']}: {msg['chat']}")
                    
                    # 合并上下文
                    full_context = "\n".join(context)
                    print(full_context)
                    
                    # 生成回复
                    bot_reply = self.bot.generate(
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
                            order_id=msg['order_id'],
                            chat_type=msg.get('chat_type', 'text')
                        )
                    
                    # 保存机器人回复到MySQL
                    self.db_manager.save_chat_message(
                        user_id=messages[-1]['user_id'],
                        user_name="me",
                        local_id=self.myid,
                        chat=bot_reply,
                        url=messages[-1]['url'],
                        order_id=order_id,
                        chat_type='text'
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

def process_wait_ship_orders(self):
    """每10秒检测是否有等待卖家发货的订单，并进行回复"""
    while True:
        try:
            # 查找所有等待卖家发货的订单
            keys = self.redis_client.keys("xianyu:order_wait_ship:*")
            for key in keys:
                order_id = key.split(":")[-1]
                # 检查key是否还存在（防止被其他线程消费）
                if not self.redis_client.exists(key):
                    continue

                # 从数据库 chat_message 检索 user_id 和 local_id
                # 取最新一条消息
                chat_msgs = self.db_manager.get_chat_messages(order_id=order_id, limit=1)
                if not chat_msgs:
                    logger.warning(f"未找到order_id={order_id}的聊天消息，无法自动回复")
                    self.redis_client.delete(key)
                    continue

                msg = chat_msgs[0]
                user_id = msg['user_id'] if isinstance(msg, dict) else msg[1]
                local_id = msg['local_id'] if isinstance(msg, dict) else msg[3]

                # 生成回复
                reply = self.bot.generate(
                    user_msg="您的订单已付款，卖家会尽快发货，请耐心等待。",
                    user_id=user_id,
                    order_id=order_id
                )

                # 保存机器人回复到MySQL
                self.db_manager.save_chat_message(
                    user_id=user_id,
                    user_name="me",
                    local_id=local_id,
                    chat=reply,
                    url=None,
                    order_id=order_id,
                    chat_type='text'
                )

                # 发送消息
                asyncio.run(self.send_msg(
                    self.ws,
                    order_id,
                    user_id,
                    reply
                ))

                logger.info(f"已自动回复等待卖家发货订单: {order_id}")

                # 删除已处理的key
                self.redis_client.delete(key)

            time.sleep(10)
        except Exception as e:
            logger.error(f"处理等待卖家发货订单时出错: {str(e)}")
            time.sleep(10) 