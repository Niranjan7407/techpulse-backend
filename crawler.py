import asyncio
from crawl4ai import AsyncWebCrawler

async def crawl(urls):
    async with AsyncWebCrawler() as crawler:
        results = []
        for i in urls:
            result = await crawler.arun(i)
            if result.success:
                results.append(result)
            else:
                print(f"Failed to crawl {i}: {result.error}")
        return results

if __name__=="__main__":
    urls = ["https://news.ycombinator.com", "https://techcrunch.com/"]
    results = asyncio.run(crawl(urls))
    print(results)