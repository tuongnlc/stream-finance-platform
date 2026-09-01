import asyncio
import httpx
from bs4 import BeautifulSoup

# Done
url = "https://voz.vn/t/clb-chung-khoan-chia-se-kinh-nghiem-dau-tu-chung-khoan-2026-ky-nguyen-vuon-minh.1188208/page-6683"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
}


async def fetch_real_html(client: httpx.AsyncClient, url: str) -> str:
    """Hàm lấy dữ liệu của 1 URL cụ thể"""
    try:
        response = await client.get(url, headers=HEADERS,follow_redirects=True, timeout=10.0)
        print(response.url)
        
        if response.status_code == 200:
            soup = BeautifulSoup(response.text, 'html.parser')
            posts = soup.select('article.message--post')

            fingerprint_data = []
            # fingerprint = ""
            for index, post in enumerate(posts, start=1):
                one_post = {}
                time_node = post.select_one('header.message-attribution time.u-dt')
                username = post.get('data-author', '').strip() or "Ẩn danh"
                post_id = post.get('id', f'unknown-{index}')
                
                time_node = post.select_one('header.message-attribution time.u-dt')
                post_time = time_node.text.strip() if time_node else "Không xác định thời gian"
                
                content_node = post.select_one('.bbWrapper')
                content = content_node.text.strip() if content_node else ""
                # print)
                # print(f"Post {index}: {username} ({post_time}) - {content[:100]}...")
                one_post["post_id"] = post_id
                one_post["username"] = username
                one_post["post_time"] = post_time
                one_post["content"] = content
                fingerprint_data.append(one_post)
                # fingerprint_data[post_id] = f"{username}|{post_time}|{content}"
            print(fingerprint_data)
            return fingerprint_data
        else:
            print(f"❌ [{url}] Lỗi HTTP code: {response.status_code}")
            return ""
    except Exception as e:
        print(f"❌ [{url}] Lỗi kết nối: {e}")
        return ""

async def main():
    await fetch_real_html(httpx.AsyncClient(), url)
    
if __name__ == "__main__":
    asyncio.run(main())