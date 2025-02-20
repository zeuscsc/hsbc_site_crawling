from crawlee import EnqueueStrategy, Glob
from crawlee.crawlers import PlaywrightCrawler, PlaywrightCrawlingContext
from crawlee.storages import Dataset
import pandas as pd
from urllib.parse import ParseResult, urlparse

FILE_NAME="hsbc_doormats.csv"
def extract_base_url(url:str):
    parsed_url:ParseResult = urlparse(url)
    base_url = parsed_url.scheme + "://" + parsed_url.netloc
    return base_url
async def main():
    dataset:Dataset=await Dataset.open()
    async def handle_request(context: PlaywrightCrawlingContext) -> None:
        context.log.info(f"Processing {context.request.url}")
        data={
            'url': context.request.url,
            'title': await context.page.title(),
            'content': await context.page.content(),
        }
        await dataset.push_data(data)
        base_url=extract_base_url(context.request.url)
        await context.enqueue_links(
            strategy=EnqueueStrategy.SAME_DOMAIN,
            include=[Glob(f"{base_url}/**")],
        )
    crawler = PlaywrightCrawler(
        request_handler=handle_request,
        max_requests_per_crawl=500000,
        max_request_retries=3,
        headless=True,
    )
    df=pd.read_csv(FILE_NAME)
    url_list=df['full_url'].to_list()
    url_list.append('https://www.hsbc.com.hk/')
    await crawler.run(url_list)


if __name__ == '__main__':
    import asyncio
    asyncio.run(main())