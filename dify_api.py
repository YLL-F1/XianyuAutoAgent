import requests
import json
import os
from dotenv import load_dotenv

# 加载环境变量
load_dotenv()

def send_message_to_dify(api_key, query, conversation_id="", user_id="abc-123", image_url=None):
    """
    向 Dify.ai API 发送消息
    
    参数:
        api_key (str): 你的 Dify.ai API 密钥
        query (str): 要发送的消息内容
        conversation_id (str, 可选): 会话ID，默认为空字符串
        user_id (str, 可选): 用户ID，默认为 "abc-123"
        image_url (str, 可选): 要包含的图片URL，默认为 None
    
    返回:
        API 的响应对象
    """
    url = "https://api.dify.ai/v1/chat-messages"
    
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json"
    }
    
    # 准备请求数据
    payload = {
        "inputs": {},
        "query": query,
        "response_mode": "blocking",
        "conversation_id": conversation_id,
        "user": user_id
    }
    
    # 如果提供了图片URL，添加到请求中
    if image_url:
        payload["files"] = [
            {
                "type": "image",
                "transfer_method": "remote_url",
                "url": image_url
            }
        ]
    
    # 发送POST请求
    response = requests.post(url, headers=headers, json=payload)
    return response

# 使用示例
if __name__ == "__main__":
    # 从环境变量获取API密钥
    API_KEY = os.getenv('DIFY_API_KEY')
    
    if not API_KEY:
        raise ValueError("请在.env文件中设置DIFY_API_KEY环境变量")
    
    # 示例查询
    query = "iPhone 13 Pro Max的规格是什么？"
    
    # 示例图片URL
    image_url = "https://cloud.dify.ai/logo/logo-site.png"
    
    # 发送请求
    response = send_message_to_dify(
        api_key=API_KEY,
        query=query,
        image_url=image_url
    )
    
    # 打印响应
    print(f"状态码: {response.status_code}")
    print(f"响应内容: {response.text}") 