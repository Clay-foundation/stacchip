import json
import os
import random
from pathlib import Path
from urllib.parse import urlparse

import boto3
import geopandas as gp
import pyarrow as pa
import pystac_client
from geoarrow.pyarrow import io

from stacchip.indexer import Sentinel2Indexer

STAC_API = "https://earth-search.aws.element84.com/v1"
S2_ASSETS = [
    "blue",
    "green",
    "nir",
    "nir08",
    "red",
    "rededge1",
    "rededge2",
    "rededge3",
    "scl",
    "swir16",
    "swir22",
]
ABSOLUTE_CLOUD_COVER_FILTER = 0.75
PLATFORM_NAME = "sentinel-2-l2a"
SCENE_NODATA_LIMIT = 20
quartals = [
    "{year}-01-01/{year}-03-31",
    "{year}-04-01/{year}-06-30",
    "{year}-07-01/{year}-09-30",
    "{year}-10-01/{year}-12-31",
]


def process_mgrs_tile(index: int, mgrs_source: str, bucket: str) -> None:
    # Prepare resources for the job
    catalog = pystac_client.Client.open(STAC_API)
    s3 = boto3.resource("s3")
    data = gp.read_file(mgrs_source)
    row = data.iloc[index]

    print("MGRS", row["name"])
    random.seed(index)
    for year in random.sample(range(2018, 2024), 2):
        print(f"Year {year}")
        for quartal in quartals:
            print(f"Quartal {quartal.format(year=year)}")
            items = catalog.search(
                collections=["sentinel-2-l2a"],
                datetime=quartal.format(year=year),
                max_items=1,
                intersects=row.geometry,
                sortby="properties.eo:cloud_cover",
                query={
                    "grid:code": {
                        "eq": f"MGRS-{row['name']}",
                    },
                    "s2:nodata_pixel_percentage": {"lte": SCENE_NODATA_LIMIT},
                },
            )
            item = items.item_collection()[0]

            if item.properties["eo:cloud_cover"] > ABSOLUTE_CLOUD_COVER_FILTER:
                continue

            print(f"Cloud cover is {item.properties['eo:cloud_cover']}")

            for key in list(item.assets.keys()):
                if key not in S2_ASSETS:
                    del item.assets[key]
                else:
                    url = urlparse(item.assets[key].href)
                    copy_source = {
                        "Bucket": "sentinel-cogs",
                        "Key": url.path.lstrip("/"),
                    }
                    print(f"Copying {copy_source}")
                    new_key = (
                        f"{PLATFORM_NAME}/{item.id}/{Path(item.assets[key].href).name}"
                    )
                    s3.meta.client.copy(copy_source, bucket, new_key)
                    item.assets[key].href = f"s3://{bucket}/{new_key}"

            # Convert Dictionary to JSON String
            data_string = json.dumps(item.to_dict())

            # Upload JSON String to an S3 Object
            s3_bucket = s3.Bucket(name=bucket)
            s3_bucket.put_object(
                Key=f"{PLATFORM_NAME}/{item.id}/stac_item.json",
                Body=data_string,
            )

            indexer = Sentinel2Indexer(item)
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
    if "STACCHIP_MGRS_SOURCE" not in os.environ:
        raise ValueError("STACCHIP_MGRS_SOURCE env var not set")
    if "STACCHIP_BUCKET" not in os.environ:
        raise ValueError("STACCHIP_BUCKET env var not set")

    index = int(os.environ["AWS_BATCH_JOB_ARRAY_INDEX"])
    mgrs_source = os.environ["STACCHIP_MGRS_SOURCE"]
    bucket = os.environ["STACCHIP_BUCKET"]

    process_mgrs_tile(index, mgrs_source, bucket)
