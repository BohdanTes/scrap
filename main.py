import math
import random
import aiofiles
from fake_useragent import UserAgent
from bs4 import BeautifulSoup
from datetime import datetime
import aiohttp
import asyncio
import csv
import os
import argparse
import pandas as pd


user_agent = UserAgent()

MAIN_URL = "https://store.igefa.de/"
API_URL = "https://api.igefa.de/shop/v1/products?page={}&filter[taxonomy]={}&requiresAggregations={}"
PRODUCTS_PER_API_REQUEST = 20
NUM_WORKERS = 20
WORKDIR_NAME = "workdir"
DATASET_FILENAME_TEMPLATE = "dataset_{}.csv"
PROGRESS_FILENAME_TEMPLATE = "progress_{}.txt"
DATETIME_FORMAT = "%Y-%m-%d_%H-%M-%S"
CSV_HEADERS = ["Product Name", "Original Data Column 1 (Breadcrumb)", "Original Data Column 2 (Ausführung)",
               "Supplier Article Number", "EAN/GTIN", "Article Number", "Product Description", "Supplier",
               "Supplier-URL", "Product Image URL", "Manufacturer", "Original Data Column 3 (Add. Description)"]


def get_header():
    return {
        'user-agent': user_agent.random
    }


async def async_request(session: aiohttp.ClientSession, url: str):
    while True:
        try:
            async with session.get(url) as response:
                if response.status == 200:
                    print(f"{url}\t{response.status}")
                    return await response.json()
                else:
                    print(f"{url}\t{response.status} retrying...")
                    delay = random.uniform(0.5, 60)
                    await asyncio.sleep(delay)
        except Exception as e:
            print(f"{url}\t{e} retrying...")
            delay = random.uniform(0.5, 60)
            await asyncio.sleep(delay)


def get_categories_ids(soup: BeautifulSoup) -> list[str]:
    footer = soup.find('footer', class_=['ant-layout-footer'])
    footer_div = footer.find('div', class_=['ant-row Footer_middleFooterRow__b32c0'])
    category_div = footer_div.find('div', class_=['ant-col ant-col-xs-24 ant-col-sm-12 ant-col-lg-7'])
    divs = category_div.find('div',
                             class_=['FooterItemCollection_flex__9aa79 FooterItemCollection_column__2e6c1']).find_all('div')
    urls = [el.find('a').get('href') for el in divs]
    ids = [url.split('/')[-1] for url in urls]
    return ids


def get_add_description(description: str) -> tuple[None | str, str]:

    if not "\n---\n" in description:
        return None, description
    parts = description.replace("&amp;", "&").split("\n---\n")
    return parts[0], parts[1]


def construct_url(product_slug: str, product_id: str) -> str:
    return f"https://store.igefa.de/p/{product_slug}/{product_id}"


def get_manufacturer(product_attributes: list[dict]) -> str | None:
    for prod_attr in product_attributes:
        if prod_attr.get("label") == "Hersteller":
            return prod_attr["value"]
    return None


async def fetch_category_total_records(session: aiohttp.ClientSession, url: str) -> int:
    json_data = await async_request(session, url)
    return json_data["total"]


async def fetch_all_categories_total_records(session, urls: list[str]) -> tuple:
    tasks = [asyncio.create_task(fetch_category_total_records(session, url)) for url in urls]
    total_records = await asyncio.gather(*tasks)
    return total_records


async def get_urls_to_parse(session, categories_ids: list[str]):

    categories_urls = [API_URL.format(1, category_id, 1) for category_id in categories_ids]
    total_records = await fetch_all_categories_total_records(session, categories_urls)

    urls = []
    for total, category_id in zip(total_records, categories_ids):
        pages = math.ceil(total / PRODUCTS_PER_API_REQUEST)
        urls.extend([API_URL.format(i, category_id, 1) for i in range(1, pages + 1)])
    return urls


def convert_breadcrumb(breadcrumb: str):
    return breadcrumb.replace("_", "/")


def product_json_to_csv_rows(json_data: dict):

    products = []

    for hit in json_data["hits"]:
        main_variant = hit["mainVariant"]
        name = hit["name"]
        breadcrumbs = convert_breadcrumb(hit["clientFields"]["converlyticsBreadcrumbs"])
        variation_name = hit["variationName"]
        supplier_article_number = hit["sku"]
        gtin = main_variant["gtin"]
        article_number = hit["skuProvidedBySupplier"]
        additional_description, description = get_add_description(hit["description"])
        url = construct_url(main_variant["slug"], main_variant["id"])

        image_url = None
        if "defaultImage" in main_variant and main_variant["defaultImage"] is not None:
            if "url" in main_variant["defaultImage"]:
                image_url = main_variant["defaultImage"]["url"]

        elif "images" in main_variant and main_variant["images"] is not None:
            if "url" in main_variant["images"]:
                image_url = main_variant["images"]["url"]

        manufacturer = get_manufacturer(hit["clientFields"]["attributes"])

        products.append({
            "Product Name": name,
            "Original Data Column 1 (Breadcrumb)": breadcrumbs,
            "Original Data Column 2 (Ausführung)": variation_name,
            "Supplier Article Number": supplier_article_number,
            "EAN/GTIN": gtin,
            "Article Number": article_number,
            "Product Description": description,
            "Supplier": "igefa Handelsgesellschaft",
            "Supplier-URL": url,
            "Product Image URL": image_url,
            "Manufacturer": manufacturer,
            "Original Data Column 3 (Add. Description)": additional_description,
        })
    return products


async def worker(session: aiohttp.ClientSession, worker_queue: asyncio.Queue, writer_queue: asyncio.Queue):
    while True:
        url = await worker_queue.get()
        if url is None:
            break

        json_data = await async_request(session, url)
        rows = product_json_to_csv_rows(json_data)
        await writer_queue.put((rows, url))

        worker_queue.task_done()


async def writer(data_filename: str, progress_filename: str, writer_queue: asyncio.Queue):
    async with aiofiles.open(data_filename, mode="a", newline='', encoding="utf-8") as data_file:
        async with aiofiles.open(progress_filename, mode="a", encoding="utf-8") as progress_file:
            csv_writer = csv.DictWriter(data_file, fieldnames=CSV_HEADERS, delimiter="|", quoting=csv.QUOTE_ALL, quotechar='"', escapechar='\\')

            while True:
                data = await writer_queue.get()
                if data is None:
                    break
                rows, url = data
                for row in rows:
                    await csv_writer.writerow(row)

                await progress_file.write(f"{url}\n")

                writer_queue.task_done()


def create_new_files_pair():
    now = datetime.utcnow().strftime(DATETIME_FORMAT)
    dataset_file_name = os.path.join(WORKDIR_NAME, DATASET_FILENAME_TEMPLATE.format(now))
    progress_file_name = os.path.join(WORKDIR_NAME, PROGRESS_FILENAME_TEMPLATE.format(now))
    with open(dataset_file_name, mode='w', encoding="utf-8") as dataset_file:
        csv_writer = csv.DictWriter(dataset_file, fieldnames=CSV_HEADERS, delimiter="|", quoting=csv.QUOTE_ALL, quotechar='"', escapechar='\\')
        csv_writer.writeheader()
    with open(progress_file_name, mode='w', encoding="utf-8") as progress_file:
        pass


def get_newest_file(dir_path: str, prefix: str):

    newest_file = newest_date = None
    files = filter(lambda el: el.startswith(prefix), os.listdir(dir_path))

    for file_name in files:
        date_str = "_".join(file_name.split("_")[1:]).split(".")[0]
        date = datetime.strptime(date_str, DATETIME_FORMAT)

        if newest_date is None or date > newest_date:
            newest_date = date
            newest_file = file_name

    return os.path.join(dir_path, newest_file)


def remove_processed_urls(urls: list[str], progress_file_path: str):

    with open(progress_file_path, "r", encoding="utf-8") as progress_file:
        processed_urls = [line.strip() for line in progress_file if line != ""]
    valid_urls = set(urls) - set(processed_urls)
    return list(valid_urls)


async def main():

    print("Init")

    if not os.path.isdir(WORKDIR_NAME):
        os.mkdir(WORKDIR_NAME)
    if not os.listdir(WORKDIR_NAME):
        create_new_files_pair()

    dataset_file = get_newest_file(WORKDIR_NAME, "dataset")
    progress_file = get_newest_file(WORKDIR_NAME, "progress")

    worker_queue = asyncio.Queue()
    writer_queue = asyncio.Queue()

    writer_task = asyncio.create_task(writer(dataset_file, progress_file, writer_queue))

    async with aiohttp.ClientSession(headers=get_header()) as session:
        async with session.get(MAIN_URL) as response:

            print("Status:", response.status)
            print("Content-type:", response.headers['content-type'])

            html = await response.text()
            soup = BeautifulSoup(html, 'html.parser')

            ids = get_categories_ids(soup)
            urls = await get_urls_to_parse(session, ids)

        print("Scrapping")

        urls = remove_processed_urls(urls, progress_file)

        for url in urls:
            await worker_queue.put(url)

        tasks = []
        for i in range(NUM_WORKERS):
            task = asyncio.create_task(worker(session, worker_queue, writer_queue))
            tasks.append(task)

        await worker_queue.join()

        for _ in range(NUM_WORKERS):
            await worker_queue.put(None)
        await asyncio.gather(*tasks)

        await writer_queue.put(None)
        await writer_task

    pd.read_csv(
        dataset_file,
        names=CSV_HEADERS,
        sep="|",
        quoting=csv.QUOTE_ALL,
        quotechar='"',
        escapechar="\\",
    ).drop_duplicates().to_csv(
        dataset_file,
        sep="|",
        index=False,
        header=True,
        quoting=csv.QUOTE_ALL,
        quotechar='"',
        escapechar="\\",
    )

    print(f"Dataset path: {dataset_file}")


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "mode",
        choices=["run", "create_new"],
    )
    args = parser.parse_args()

    if args.mode == "run":
        asyncio.run(main())
    else:
        create_new_files_pair()
