from asyncio.log import logger
import requests
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
                return data.get("country", ""), data.get("city", "")
    except Exception as e:
        logger.error(f"IP归属地查询失败: {e}")
    return "", ""

print(get_city_by_ip("114.249.128.12"))