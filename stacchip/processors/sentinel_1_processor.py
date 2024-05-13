import json
import os
import random
from pathlib import Path
from urllib.parse import urlparse

import boto3
import geopandas as gp
import planetary_computer as pc
import pyarrow as pa
import pystac_client
import rasterio
from geoarrow.pyarrow import io
from rasterio.io import MemoryFile

from stacchip.indexer import NoDataMaskChipIndexer

STAC_API = "https://planetarycomputer.microsoft.com/api/stac/v1"
S1_ASSETS = [
    "vv",
    "vh",
]
PLATFORM_NAME = "sentinel-1-rtc"
quartals = [
    "{year}-01-01/{year}-03-31",
    "{year}-04-01/{year}-06-30",
    "{year}-07-01/{year}-09-30",
    "{year}-10-01/{year}-12-31",
]


def process_mgrs_tile(index: int, mgrs_source: str, bucket: str) -> None:
    # Prepare resources for the job
    s3 = boto3.resource("s3")
    data = gp.read_file(mgrs_source)
    row = data.iloc[index]
    catalog = pystac_client.Client.open(STAC_API, modifier=pc.sign_inplace)
    print("MGRS", row["name"])
    random.seed(index)
    for year in random.sample(range(2018, 2024), 1):
        print(f"Year {year}")
        for quartal in random.sample(quartals, 1):
            print(f"Quartal {quartal.format(year=year)}")
            items = catalog.search(
                max_items=1,
                filter_lang="cql2-json",
                filter={
                    "op": "and",
                    "args": [
                        # {
                        #     "op": "s_intersects",
                        #     "args": [
                        #         {"property": "geometry"},
                        #         row.geometry.__geo_interface__,
                        #     ],
                        # },
                        {
                            "op": "anyinteracts",
                            "args": [
                                {"property": "datetime"},
                                quartal.format(year=year),
                            ],
                        },
                        {
                            "op": "=",
                            "args": [{"property": "collection"}, "sentinel-1-rtc"],
                        },
                    ],
                },
            )
            item = items.item_collection()[0]

            nodata_mask = None
            for key in list(item.assets.keys()):
                if key not in S1_ASSETS:
                    del item.assets[key]
                else:
                    url = item.assets[key].href
                    with rasterio.open(url) as rst:
                        data = rst.read()
                        meta = rst.meta.copy()
                        if nodata_mask is None:
                            nodata_mask = data[0] == rst.nodata

                    with MemoryFile() as memfile:
                        with memfile.open(**meta, compress="deflate") as dst:
                            dst.write(data)

                        memfile.seek(0)

                        s3_bucket = s3.Bucket(name=bucket)
                        new_key = (
                            f"{PLATFORM_NAME}/{item.id}/{Path(urlparse(url).path).name}"
                        )
                        print(f"Copying {urlparse(url).path}")
                        s3_bucket.put_object(
                            Key=new_key,
                            Body=memfile.read(),
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
            indexer = NoDataMaskChipIndexer(item, nodata_mask=nodata_mask)
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
