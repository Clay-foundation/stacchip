import json
import os
import random
from pathlib import Path
from urllib.parse import urlparse

import boto3
import pyarrow as pa
from dateutil import parser
from geoarrow.pyarrow import io
from pystac import Item
from rio_stac import create_stac_item

from stacchip.indexer import NoStatsChipIndexer

PLATFORM_NAME = "linz"

nz_prefixes = [
    "auckland/auckland_2022_0.075m/",
    "wellington/wellington_2021_0.075m/",
    "wellington/wellington_2021_0.3m/",
    "bay-of-plenty/bay-of-plenty_2023_0.1m/",
    "bay-of-plenty/tauranga_2022_0.1m/",
    "bay-of-plenty/tauranga-winter_2022_0.1m/",
    "canterbury/canterbury_2023_0.3m/",
    "canterbury/ashburton_2023_0.1m/",
    "canterbury/hurunui_2023_0.075m/",
    "canterbury/timaru_2022-2023_0.1m/",
    "canterbury/selwyn_2022-2023_0.075m/",
    "gisborne/gisborne_2023_0.075m/",
    "hawkes-bay/hawkes-bay_2022_0.05m/",
    "hawkes-bay/napier_2017-2018_0.05m/",
    "hawkes-bay/wairoa_2014-2015_0.1m/",
    "manawatu-whanganui/manawatu-whanganui_2021-2022_0.3m/",
    "manawatu-whanganui/palmerston-north_2022_0.125m/",
    "manawatu-whanganui/rangitikei_2021_0.125m/",
    "manawatu-whanganui/tararua_2024_0.1m/",
    "manawatu-whanganui/whanganui_2022_0.075m/",
    "marlborough/marlborough_2023_0.075m/",
    "nelson/nelson_2022_0.075m/",
    "northland/northland_2016_0.1m/",
    "otago/queenstown_2021_0.1m/",
    "otago/otago_2018_0.1m/",
    "otago/dunedin_2018-2019_0.1m/",
    "southland/southland_2023_0.1m/",
    "southland/invercargill_2022_0.05m/",
    "taranaki/taranaki_2022_0.05m/",
    "taranaki/new-plymouth_2017_0.1m/",
    "tasman/tasman_2023_0.075m/",
    "waikato/hamilton_2023_0.05m/",
    "waikato/otorohanga_2021_0.1m/",
    "waikato/taupo_2023_0.075m/",
    "waikato/thames-coromandel_2021_0.1m/",
    "waikato/waikato_2021_0.1m/",
    "waikato/waipa_2021_0.1m/",
    "west-coast/buller_2020_0.2m/",
    "west-coast/west-coast_2016_0.1m/",
]


def get_linz_tiffs(prefix) -> list:

    s3_resource = boto3.resource("s3")
    s3_bucket = s3_resource.Bucket(name="nz-imagery")

    files = []
    s3_object_iterator = s3_bucket.objects.filter(Prefix=prefix)

    for s3_object in s3_object_iterator:
        if s3_object.key.endswith(".tiff"):
            files.append(s3_object.key)

    # Sample a percentage of all scenes
    sample_size = max(min(int(len(files) / 10), 600), 10)
    print(f"Found {len(files)} scenes for {prefix}, keeping {sample_size}")
    random.seed(42)
    return random.sample(files, sample_size)


def get_original_item(key: str) -> Item:
    s3_resource = boto3.resource("s3")
    content_object = s3_resource.Object("nz-imagery", key.rstrip("tiff") + "json")
    file_content = content_object.get()["Body"].read().decode("utf-8")
    json_content = json.loads(file_content)
    return Item.from_dict(json_content)


def process_linz_tile(index, bucket):

    tiffs = get_linz_tiffs(nz_prefixes[index])

    for key in tiffs:
        print(f"Working on {key}")

        href = f"s3://nz-imagery/{key}"
        item = create_stac_item(href, with_proj=True)

        original_item = get_original_item(key)
        item.datetime = parser.parse(original_item.properties["start_datetime"])
        item.id = original_item.id

        url = urlparse(href)
        copy_source = {
            "Bucket": "nz-imagery",
            "Key": url.path.lstrip("/"),
        }
        print(f"Copying {copy_source}")
        new_key = f"{PLATFORM_NAME}/{item.id}/{Path(item.assets['asset'].href).name}"

        s3 = boto3.resource("s3")
        s3.meta.client.copy(
            copy_source,
            bucket,
            new_key,
        )

        item.assets["asset"].href = f"s3://{bucket}/{new_key}"

        # Convert Dictionary to JSON String
        data_string = json.dumps(item.to_dict())

        # Upload JSON String to an S3 Object
        s3_bucket = s3.Bucket(name=bucket)
        s3_bucket.put_object(
            Key=f"{PLATFORM_NAME}/{item.id}/stac_item.json",
            Body=data_string,
        )

        indexer = NoStatsChipIndexer(item)
        index = indexer.create_index()

        writer = pa.BufferOutputStream()
        io.write_geoparquet_table(index, writer)
        body = bytes(writer.getvalue())
        # Centralize the index files to make combining them easier later on
        s3_bucket.put_object(
            Body=body,
            Key=f"index/{PLATFORM_NAME}/{item.id}/index_{item.id}.parquet",
        )


def process() -> None:

    if "AWS_BATCH_JOB_ARRAY_INDEX" not in os.environ:
        raise ValueError("AWS_BATCH_JOB_ARRAY_INDEX env var not set")
    if "STACCHIP_BUCKET" not in os.environ:
        raise ValueError("STACCHIP_BUCKET env var not set")

    index = int(os.environ["AWS_BATCH_JOB_ARRAY_INDEX"])
    bucket = os.environ["STACCHIP_BUCKET"]

    process_linz_tile(index, bucket)
