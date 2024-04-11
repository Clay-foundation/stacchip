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

from stacchip.indexer import LandsatIndexer

STAC_API = "https://landsatlook.usgs.gov/stac-server"

LS_ASSETS_L1 = [
    "blue",
    "green",
    "red",
    "nir08",
    "swir16",
    "lwir",
    "lwir_high",
    "swir22",
    "pan",
    "qa_pixel",
]
LS_ASSETS_L2 = [
    "blue",
    "green",
    "red",
    "nir08",
    "swir16",
    "swir22",
    "qa_pixel",
]
ABSOLUTE_CLOUD_COVER_FILTER = 75
PLATFORM_NAME_L2 = "landsat-c2l2-sr"
PLATFORM_NAME_L1 = "landsat-c2l1"
quartals = [
    "{year}-01-01/{year}-03-31",
    "{year}-04-01/{year}-06-30",
    "{year}-07-01/{year}-09-30",
    "{year}-10-01/{year}-12-31",
]


def process_landsat_tile(index: int, sample_source: str, bucket: str) -> None:
    # Prepare resources for the job
    catalog = pystac_client.Client.open(STAC_API)
    s3 = boto3.resource("s3")
    data = gp.read_file(sample_source)
    row = data.iloc[index]

    print("MGRS", row["name"])
    for platform_name in [PLATFORM_NAME_L1, PLATFORM_NAME_L2]:
        random.seed(index)
        for year in random.sample(range(2018, 2024), 1):
            print(f"Year {year}")
            for quartal in quartals:
                print(f"Quartal {quartal.format(year=year)}")
                items = catalog.search(
                    collections=[platform_name],
                    datetime=quartal.format(year=year),
                    max_items=1,
                    intersects=row.geometry.centroid,
                    sortby="properties.eo:cloud_cover",
                    query={
                        "platform": {"in": ["LANDSAT_8", "LANDSAT_9"]},
                    },
                )
                item = items.item_collection()[0]

                if item.properties["eo:cloud_cover"] > ABSOLUTE_CLOUD_COVER_FILTER:
                    continue

                print(
                    f"Cloud cover is {item.properties['eo:cloud_cover']} ({item.properties['platform']})"
                )

                for key in list(item.assets.keys()):
                    if (
                        platform_name == PLATFORM_NAME_L1 and key not in LS_ASSETS_L1
                    ) or (key not in LS_ASSETS_L2):
                        del item.assets[key]
                    else:
                        href = item.assets[key].extra_fields["alternate"]["s3"]["href"]
                        url = urlparse(href)
                        copy_source = {
                            "Bucket": url.netloc,
                            "Key": url.path.lstrip("/"),
                        }
                        print(f"Copying {key} band to {copy_source}")
                        new_key = f"{platform_name}/{item.id}/{Path(href).name}"
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
                    Key=f"{platform_name}/{item.id}/stac_item.json",
                    Body=data_string,
                )

                indexer = LandsatIndexer(item, chip_max_nodata=0)
                chip_index = indexer.create_index()

                writer = pa.BufferOutputStream()
                io.write_geoparquet_table(chip_index, writer)
                body = bytes(writer.getvalue())
                # Centralize the index files to make combining them easier later on
                s3_bucket.put_object(
                    Body=body,
                    Key=f"index/{platform_name}/{item.id}/index_{item.id}.parquet",
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

    process_landsat_tile(index, sample_source, bucket)
