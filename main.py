from cookie_fetcher import refresh_and_get_cookies
from dotenv import load_dotenv
import asyncio
if __name__ == '__main__':
    load_dotenv()
    cookies_str = refresh_and_get_cookies()
    from XianyuAgent import DifyAgent
    from xianyu_live import XianyuLive
    bot = DifyAgent()
    xianyuLive = XianyuLive(cookies_str, bot=bot)
    asyncio.run(xianyuLive.main())
