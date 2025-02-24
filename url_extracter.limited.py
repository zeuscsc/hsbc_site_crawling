from crawlee import EnqueueStrategy
from crawlee.crawlers import PlaywrightCrawler, PlaywrightCrawlingContext
from crawlee.storages import Dataset
import pandas as pd
from urllib.parse import urlparse

async def main():
    dataset:Dataset=await Dataset.open()
    async def handle_request(context: PlaywrightCrawlingContext) -> None:
        context.log.info(f"Processing {context.request.url}")
        doormat_heading_link_elements=await context.page.query_selector_all('body > div.header-wrapper > div > header > div.header-wrapper-main > div > div.header-main-container.hide-on-mobile-and-tablet > div > div > nav > ul > li:nth-child(1) > div.doormat-menu .doormat-heading-link')
        doormat_links_elements=await context.page.query_selector_all('body > div.header-wrapper > div > header > div.header-wrapper-main > div > div.header-main-container.hide-on-mobile-and-tablet > div > div > nav > ul > li:nth-child(1) > div.doormat-menu .doormat-links > li')
        crawling_links=[]
        for element in doormat_heading_link_elements:
            data={
                'url': await element.get_attribute('href'),
                'title': (await element.text_content()).replace('\n', ' ').strip(),
                'parent_url': context.request.url,
            }
            crawling_links.append(data)
            pass
        for element in doormat_links_elements:
            data={
                'url': await (await element.query_selector('a')).get_attribute('href'),
                'title': (await element.text_content()).replace('\n', ' ').strip(),
                'parent_url': context.request.url,
            }
            crawling_links.append(data)
            pass
        await dataset.push_data(crawling_links)
        
    crawler = PlaywrightCrawler(
        request_handler=handle_request,
        max_requests_per_crawl=500000,
        max_request_retries=3,
        headless=True,
    )
    await crawler.run(['https://www.hsbc.com.hk/','https://www.hsbc.com.hk/zh-hk/'])

    import os
    import json
    import re

    folder_path = "storage/datasets/default"
    output_file = "hsbc_doormats.limited.csv"
    json_list = []

    try:
        files = os.listdir(folder_path)
        for filename in files:
            if filename.endswith(".json") and filename != "__metadata__.json" and re.match(r"^\d+\.json$", filename):
                file_path = os.path.join(folder_path, filename)
                with open(file_path, 'r',encoding="utf-8") as f:
                    try:
                        json_object = json.load(f)
                        json_list.append(json_object)
                    except json.JSONDecodeError as e:
                        print(f"Error decoding JSON in file: {filename}. Error: {e}")
    except FileNotFoundError:
        print(f"Folder not found: {folder_path}")
    except Exception as e:
        print(f"An error occurred: {e}")

    df = pd.DataFrame(json_list)
    def transform_url(url:str,base_url:str):
        if url.startswith("/"):
            return urlparse(base_url).scheme + "://" + urlparse(base_url).netloc + url
        else:
            return url
    def extract_base_url(url):
        parsed_url = urlparse(url)
        base_url = parsed_url.scheme + "://" + parsed_url.netloc
        return base_url
    df['full_url'] = df['url'].apply(lambda x: transform_url(x, df['parent_url'].iloc[0]))
    df['base_url'] = df['full_url'].apply(extract_base_url)
    df.to_csv(output_file, index=False)
    print(f"Saved to: {output_file}")


if __name__ == '__main__':
    import asyncio
    asyncio.run(main())
    