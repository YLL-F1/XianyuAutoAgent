from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
import requests
import json
import time
import browser_cookie3

def check_remote_debugging_connection():
    """检查远程调试连接是否可用"""
    try:
        response = requests.get('http://localhost:9222/json/version')
        if response.status_code == 200:
            return True
        else:
            print(f"Chrome调试连接失败，状态码: {response.status_code}")
            return False
    except Exception as e:
        print(f"检查调试连接时出错: {e}")
        return False

def get_open_tabs():
    """获取所有打开的标签页信息"""
    try:
        response = requests.get('http://localhost:9222/json/list')
        if response.status_code == 200:
            tabs = response.json()
            # 查找闲鱼页面
            for tab in tabs:
                if 'goofish.com' in tab.get('url', ''):
                    return tab
            print("未找到闲鱼页面，请先打开闲鱼网站")
            return None
        return None
    except Exception as e:
        print(f"获取标签页时出错: {e}")
        return None

def get_goofish_cookies():
    """获取咸鱼(goofish.com)的Cookie并格式化"""
    try:
        domains = ["_goofish.com"]
        cookies_dict = {}
        
        # 从每个域名获取cookie
        for domain in domains:
            try:
                domain_cookies = browser_cookie3.chrome(domain_name=domain)
                for cookie in domain_cookies:
                    cookies_dict[cookie.name] = cookie.value
            except Exception as e:
                print(f"获取 {domain} 的Cookie出错: {str(e)}")
        
        # 按指定顺序提取cookie
        cookie_keys = [
            'mtop_partitioned_detect',
            '_m_h5_tk',
            '_m_h5_tk_enc',
            'cna',
            'xlly_s',
            't',
            '_samesite_flag_',
            'cookie2',
            '_tb_token_',
            'sgcookie',
            'tracknick',
            'csg',
            'unb',
            'havana_lgc2_77',
            '_hvn_lgc_',
            'havana_lgc_exp',
            'sdkSilent',
            'tfstk'
        ]
        
        # 构建cookie字符串
        cookie_parts = []
        for key in cookie_keys:
            if key in cookies_dict:
                cookie_parts.append(f"{key}={cookies_dict[key]}")
        
        # 拼接cookie字符串
        cookie_string = "; ".join(cookie_parts)
        
        # 检查必要的cookie是否都存在
        required_keys = ['_m_h5_tk', '_m_h5_tk_enc', 'tfstk']
        missing_keys = [key for key in required_keys if key not in cookies_dict]
        if missing_keys:
            print(f"警告: 缺少重要的Cookie: {', '.join(missing_keys)}")
        
        print(f"找到 {len(cookie_parts)}/{len(cookie_keys)} 个Cookie项")
        return cookie_string
        
    except Exception as e:
        print(f"获取Cookie时出错: {e}")
        return None

def refresh_and_get_cookies():
    """刷新浏览器并获取Cookie"""
    # 先检查连接是否可用
    if not check_remote_debugging_connection():
        print("无法连接到Chrome调试端口，请确保Chrome已以调试模式启动")
        print("启动命令: /Applications/Google\\ Chrome.app/Contents/MacOS/Google\\ Chrome --remote-debugging-port=9222")
        return None
    
    # 查找闲鱼页面
    tab = get_open_tabs()
    if not tab:
        return None
    
    try:
        # 设置连接选项
        chrome_options = Options()
        chrome_options.add_experimental_option("debuggerAddress", "127.0.0.1:9222")
        
        # 直接使用Chrome驱动
        driver = webdriver.Chrome(options=chrome_options)
        
        print(f"正在刷新闲鱼页面: {tab.get('url')}")
        driver.refresh()
        time.sleep(5)  # 等待页面刷新完成
        
        # 获取Cookie
        print("正在获取Cookie...")
        cookie_string = get_goofish_cookies()
        
        if cookie_string:
            # 保存到文件
            with open("goofish_cookies.txt", "w", encoding="utf-8") as f:
                f.write(cookie_string)
            print("\nCookie已保存到 goofish_cookies.txt")
            
            # 更新 .env 文件
            try:
                # 读取现有的 .env 文件内容
                with open(".env", "r", encoding="utf-8") as f:
                    env_lines = f.readlines()
                
                # 更新或添加 COOKIES_STR
                cookie_line = f'COOKIES_STR="{cookie_string}"\n'
                cookie_found = False
                
                for i, line in enumerate(env_lines):
                    if line.startswith("COOKIES_STR="):
                        env_lines[i] = cookie_line
                        cookie_found = True
                        break
                
                if not cookie_found:
                    env_lines.append(cookie_line)
                
                # 写回 .env 文件
                with open(".env", "w", encoding="utf-8") as f:
                    f.writelines(env_lines)
                print("Cookie已更新到 .env 文件")
                
            except Exception as e:
                print(f"更新 .env 文件时出错: {e}")
            
            return cookie_string
        else:
            print("未能获取到Cookie")
            return None
        
    except Exception as e:
        print(f"连接到Chrome或刷新页面时出错: {e}")
        return None

# 执行刷新和获取Cookie
if __name__ == "__main__":
    q = refresh_and_get_cookies()
    print(q)
    print(type(q))