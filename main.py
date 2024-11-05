import math
import random
import aiofiles
from fake_useragent import UserAgent
from bs4 import BeautifulSoup
from datetime import datetime
import aiohttp
import asyncio
import csv
import json
import os
import argparse
import pandas as pd
import traceback


user_agent = UserAgent()

BASE_URL = "https://store.igefa.de/"
PRODUCT_URL_TEMPLATE = "https://store.igefa.de/p/{}/{}"
API_URL = "https://api.igefa.de/shop/v1/products?page={}&filter[taxonomy]={}&requiresAggregations={}"
PRODUCTS_PER_API_REQUEST = 20
NUM_WORKERS = 30
WORKDIR_NAME = "workdir"
DATASET_FILENAME_TEMPLATE = "dataset_{}.csv"
PROGRESS_FILENAME_TEMPLATE = "progress_{}.txt"
DATETIME_FORMAT = "%Y-%m-%d_%H-%M-%S"
CSV_HEADERS = ["Product Name", "Original Data Column 1 (Breadcrumb)", "Original Data Column 2 (Ausführung)",
               "Supplier Article Number", "EAN/GTIN", "Article Number", "Product Description", "Supplier",
               "Supplier-URL", "Product Image URL", "Manufacturer", "Original Data Column 3 (Add. Description)"]

CSV_ARGS = dict(fieldnames=CSV_HEADERS, delimiter="|", quoting=csv.QUOTE_ALL)


def get_header():
    return {
        'user-agent': user_agent.random
    }


async def async_request(session: aiohttp.ClientSession, url: str, delay_range: tuple[float | int, float | int] = (1, 60),
                        timeout: int = 20)\
        -> str:
    while True:
        try:
            async with session.get(url, timeout=timeout) as response:
                if response.status == 200:
                    print(f"{url}\t{response.status}")
                    return await response.text()
                else:
                    print(f"{url}\t{response.status} retrying...")
                    delay = random.uniform(*delay_range)
                    await asyncio.sleep(delay)
        except Exception as e:
            print(f"{url}\t{e} retrying...")
            delay = random.uniform(*delay_range)
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
    return PRODUCT_URL_TEMPLATE.format(product_slug, product_id)


def get_manufacturer(product_attributes: list[dict]) -> str | None:
    for prod_attr in product_attributes:
        if prod_attr.get("label") == "Hersteller":
            return prod_attr["value"]
    return None


async def fetch_category_total_records(session: aiohttp.ClientSession, url: str) -> int:
    raw_data = await async_request(session, url)
    json_data = await asyncio.to_thread(json.loads, raw_data)
    return json_data["total"]


async def fetch_all_categories_total_records(session: aiohttp.ClientSession, urls: list[str]) -> tuple:
    tasks = [asyncio.create_task(fetch_category_total_records(session, url)) for url in urls]
    total_records = await asyncio.gather(*tasks)
    return total_records


async def get_urls_to_parse(session: aiohttp.ClientSession, categories_ids: list[str]) -> list[str]:
    categories_urls = [API_URL.format(1, category_id, 1) for category_id in categories_ids]
    total_records = await fetch_all_categories_total_records(session, categories_urls)

    urls = []
    for total, category_id in zip(total_records, categories_ids):
        pages = math.ceil(total / PRODUCTS_PER_API_REQUEST)
        urls.extend([API_URL.format(i, category_id, 1) for i in range(1, pages + 1)])
    return urls


async def fetch_urls(session: aiohttp.ClientSession) -> list[str]:
    raw_data = await async_request(session, BASE_URL)
    soup = BeautifulSoup(raw_data, 'html.parser')
    ids = get_categories_ids(soup)
    urls = await get_urls_to_parse(session, ids)
    return urls


def convert_breadcrumb(breadcrumb: str | None) -> str:
    return breadcrumb.replace("_", "/")


def product_json_parse(json_data: dict) -> list[dict]:

    products = []

    for hit in json_data["hits"]:

        main_variant = hit.get("mainVariant", {})
        name = hit.get("name", None)
        breadcrumbs = hit.get("clientFields", {}).get("converlyticsBreadcrumbs", None)
        if breadcrumbs is not None:
            breadcrumbs = convert_breadcrumb(breadcrumbs)
        variation_name = hit.get("variationName", None)
        supplier_article_number = hit.get("sku", None)
        gtin = main_variant.get("gtin", None)
        article_number = hit.get("skuProvidedBySupplier", None)

        additional_description = description = hit.get("description", None)
        if description is not None:
            additional_description, description = get_add_description(description)

        slug = main_variant.get("slug", None)
        product_id = main_variant.get("id", None)
        url = construct_url(slug, product_id) if slug is not None and product_id is not None else None

        image_url = main_variant.get("defaultImage", None)
        if image_url is not None:
            image_url = image_url.get("url", None)
        manufacturer = get_manufacturer(hit.get("clientFields", {}).get("attributes", None))

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

        try:
            raw_data = await async_request(session, url)

            json_data = await asyncio.to_thread(json.loads, raw_data)
            rows = product_json_parse(json_data)
            await writer_queue.put((rows, url))

        except Exception as e:
            # Log the exception for debugging
            print(f"Error processing {url}: {e}")
            traceback.print_exc()

        finally:
            worker_queue.task_done()


async def writer(data_filename: str, progress_filename: str, writer_queue: asyncio.Queue):
    async with aiofiles.open(data_filename, mode="a", newline='', encoding="utf-8") as data_file:
        async with aiofiles.open(progress_filename, mode="a", encoding="utf-8") as progress_file:
            csv_writer = csv.DictWriter(data_file, **CSV_ARGS)

            while True:
                data = await writer_queue.get()
                if data is None:
                    break
                rows, url = data

                # write rows chunk into dataset
                for row in rows:
                    await csv_writer.writerow(row)

                # save progress
                await progress_file.write(f"{url}\n")

                writer_queue.task_done()


def create_new_files_pair():
    now = datetime.utcnow().strftime(DATETIME_FORMAT)
    dataset_file_name = os.path.join(WORKDIR_NAME, DATASET_FILENAME_TEMPLATE.format(now))
    progress_file_name = os.path.join(WORKDIR_NAME, PROGRESS_FILENAME_TEMPLATE.format(now))
    with open(dataset_file_name, mode='w', encoding="utf-8") as dataset_file:
        csv_writer = csv.DictWriter(dataset_file, **CSV_ARGS)
        csv_writer.writeheader()
    with open(progress_file_name, mode='w', encoding="utf-8") as progress_file:
        pass


def get_newest_file(dir_path: str, prefix: str) -> str:

    newest_file = newest_date = None
    files = filter(lambda el: el.startswith(prefix), os.listdir(dir_path))

    for file_name in files:
        date_str = "_".join(file_name.split("_")[1:]).split(".")[0]
        date = datetime.strptime(date_str, DATETIME_FORMAT)

        if newest_date is None or date > newest_date:
            newest_date = date
            newest_file = file_name

    return os.path.join(dir_path, newest_file)


def remove_processed_urls(urls: list[str], progress_file_path: str) -> list[str]:

    with open(progress_file_path, "r", encoding="utf-8") as progress_file:
        processed_urls = [line.strip() for line in progress_file if line != ""]
    valid_urls = set(urls) - set(processed_urls)
    return list(valid_urls)


async def main():

    print("Init")
    print(f"Workers: {NUM_WORKERS}")

    # creating work directory if not exists
    if not os.path.isdir(WORKDIR_NAME):
        os.mkdir(WORKDIR_NAME)

    # creating first dataset pair if work directory is empty
    if not os.listdir(WORKDIR_NAME):
        create_new_files_pair()

    # finding datasets for current scrapping
    dataset_file = get_newest_file(WORKDIR_NAME, "dataset")
    progress_file = get_newest_file(WORKDIR_NAME, "progress")

    # creating queues
    worker_queue = asyncio.Queue()
    writer_queue = asyncio.Queue()

    async with aiohttp.ClientSession(headers=get_header()) as session:

        print("Fetching categories ids and urls")

        # fetching all urls
        try:
            urls = await fetch_urls(session)
        except Exception as e:
            exit(f"Unable to fetch urls, error: {e}")
            traceback.print_exc()

        # removing urls that have been already scrapped previous run (checking in corresponding progress file)
        urls = remove_processed_urls(urls, progress_file)

        print(f"Urls to process: {len(urls)}")
        print("Scrapping")

        # loading workers queue
        for url in urls:
            await worker_queue.put(url)

        # writer
        writer_task = asyncio.create_task(writer(dataset_file, progress_file, writer_queue))

        # creating workers
        tasks = []
        for i in range(NUM_WORKERS):
            task = asyncio.create_task(worker(session, worker_queue, writer_queue))
            tasks.append(task)

        # waiting until queue empty
        await worker_queue.join()

        # shutting down workers
        for _ in range(NUM_WORKERS):
            await worker_queue.put(None)
        await asyncio.gather(*tasks)

        await writer_queue.put(None)
        await writer_task

    print("Deleting duplicates")

    # using pandas to delete potential duplicates
    df = pd.read_csv(
        dataset_file,
        names=CSV_HEADERS,
        sep="|",
        quoting=csv.QUOTE_ALL,
    ).drop_duplicates()

    df.to_csv(
        dataset_file,
        sep="|",
        header=False,
        index=False,
        quoting=csv.QUOTE_ALL,
        na_rep="NA"
    )

    print(f"Total rows: {df.shape[0] - 1}\nDataset path: {dataset_file}")


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "mode",
        help="""Choose 'run' mode if you want to begin or continue scrapping into the most recent created dataset, 
            otherwise if you want to begin new scrapping choose 'create_new' to create new dataset""",
        choices=["run", "create_new"],
    )
    args = parser.parse_args()

    if args.mode == "run":
        asyncio.run(main())
    else:
        if not os.path.isdir(WORKDIR_NAME):
            os.mkdir(WORKDIR_NAME)
        create_new_files_pair()
