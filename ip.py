from asyncio.log import logger
import requests
import IP2Location

try:
    import IP2Location
    IP2LOCATION_AVAILABLE = True
except ImportError:
    IP2LOCATION_AVAILABLE = False
    logger.warning("ip2location库未安装，本地IP查询功能将不可用")

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

def get_city_by_ip_local(ip):
    """使用ip2location本地库获取城市和国家"""
    try:
        if not ip:
            return "", ""
        
        if not IP2LOCATION_AVAILABLE:
            logger.warning("ip2location库未安装，无法使用本地IP查询")
            return "", ""
            
        # 初始化ip2location数据库
        # 请确保IP2LOCATION.BIN文件在正确路径下
        database = IP2Location.IP2Location()
        database.open("data/IP2LOCATION.BIN")
        
        # 查询IP
        record = database.get_all(ip)
        
        if record:
            country = record.country_long if record.country_long else ""
            city = record.city if record.city else ""
            return country, city
        else:
            return "", ""
            
    except Exception as e:
        logger.error(f"本地IP归属地查询失败: {e}")
        return "", ""

if __name__ == '__main__':
    print(get_city_by_ip("114.249.128.12"))
    print(get_city_by_ip_local("114.249.128.12"))