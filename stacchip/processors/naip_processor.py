import json
import os
import random
from pathlib import Path
from urllib.parse import urlparse

import boto3
import geopandas as gp
import pyarrow as pa
import pystac_client
from botocore.exceptions import ClientError
from geoarrow.pyarrow import io

from stacchip.indexer import NoStatsChipIndexer

STAC_API = "https://planetarycomputer.microsoft.com/api/stac/v1"

AWS_S3_URL = (
    "s3://naip-analytic/{state}/{year}/{resolution}/rgbir_cog/{block}{subblock}/{name}"
)
PLATFORM_NAME = "naip"


def process_naip_tile(
    index: int, sample_source: str, bucket: str, latest_only: bool = False
) -> None:
    # Prepare resources for the job
    catalog = pystac_client.Client.open(STAC_API)
    s3 = boto3.resource("s3")
    data = gp.read_file(sample_source)
    row = data.iloc[index]

    items = catalog.search(
        collections=["naip"],
        intersects=row.geometry.centroid,
        sortby="properties.naip:year",
        max_items=10,
    )
    items = list(items.item_collection())

    if not len(items):
        print(f"No items found, skipping index {index}")
        return

    latest_item = items.pop()
    items_to_process = [latest_item]
    if not latest_only:
        random.seed(index)
        random_item = random.choice(items)
        items_to_process.append(random_item)

    for item in items_to_process:
        print(f"Processing item {item.id}")
        for key in list(item.assets.keys()):
            if key != "image":
                del item.assets[key]
                continue

            new_key = f"{PLATFORM_NAME}/{item.id}/{Path(item.assets[key].href).name}"
            try:
                href = AWS_S3_URL.format(
                    year=item.properties["naip:year"],
                    state=item.properties["naip:state"],
                    resolution=f"{int(item.properties['gsd'] * 100)}cm",
                    block=item.id.split("_")[2][:5],
                    subblock=f"/{item.id.split('_')[2][5:]}",
                    name=item.assets["image"].href.split("/")[-1],
                )
                url = urlparse(href)
                copy_source = {
                    "Bucket": "naip-analytic",
                    "Key": url.path.lstrip("/"),
                }
                print(f"Copying {copy_source}")
                s3.Object("naip-analytic", url.path.lstrip("/")).load(
                    RequestPayer="requester"
                )
                s3.meta.client.copy(
                    copy_source,
                    bucket,
                    new_key,
                    ExtraArgs={"RequestPayer": "requester"},
                )
            except ClientError:
                href = AWS_S3_URL.format(
                    year=item.properties["naip:year"],
                    state=item.properties["naip:state"],
                    resolution=f"{int(item.properties['gsd'] * 100)}cm",
                    block=item.id.split("_")[2][:5],
                    subblock="",
                    name=item.assets["image"].href.split("/")[-1],
                )
                url = urlparse(href)
                copy_source = {
                    "Bucket": "naip-analytic",
                    "Key": url.path.lstrip("/"),
                }
                print(f"Failed, now copying {copy_source}")
                s3.Object("naip-analytic", url.path.lstrip("/")).load(
                    RequestPayer="requester"
                )
                s3.meta.client.copy(
                    copy_source,
                    bucket,
                    new_key,
                    ExtraArgs={"RequestPayer": "requester"},
                )

            item.assets[key].href = f"s3://{bucket}/{new_key}"

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
        print("Indexer info", indexer.x_size, indexer.y_size, indexer.shape)

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
    if "STACCHIP_SAMPLE_SOURCE" not in os.environ:
        raise ValueError("STACCHIP_SAMPLE_SOURCE env var not set")
    if "STACCHIP_BUCKET" not in os.environ:
        raise ValueError("STACCHIP_BUCKET env var not set")

    index = int(os.environ["AWS_BATCH_JOB_ARRAY_INDEX"])
    sample_source = os.environ["STACCHIP_SAMPLE_SOURCE"]
    bucket = os.environ["STACCHIP_BUCKET"]

    process_naip_tile(index, sample_source, bucket)
