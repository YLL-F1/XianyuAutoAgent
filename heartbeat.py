import time
import json
import asyncio
from loguru import logger
from utils.xianyu_utils import generate_mid, generate_uuid, decrypt
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