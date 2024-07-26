import calendar
import json
import os
import random
import tempfile
from datetime import datetime
from pathlib import Path

import boto3
import planetary_computer as pc
import pyarrow as pa
import pystac_client
import rasterio
from geoarrow.pyarrow import io
from rasterio.warp import Resampling, calculate_default_transform, reproject

from stacchip.indexer import ModisIndexer

STAC_API = "https://planetarycomputer.microsoft.com/api/stac/v1"
COLLECTION = "modis-09A1-061"
BANDS = [
    "sur_refl_b01",
    "sur_refl_b02",
    "sur_refl_b03",
    "sur_refl_b04",
    "sur_refl_b05",
    "sur_refl_b06",
    "sur_refl_b07",
    "sur_refl_qc_500m",
]
# The grid tiles were selected to not have nodata
# in the SIN projection. This is to avoid effects
# of the international dateline cutoff.
SIN_GRID_TILES = [
    (2, 12),
    (2, 13),
    (2, 14),
    (2, 15),
    (2, 16),
    (2, 17),
    (2, 18),
    (2, 19),
    (2, 20),
    (2, 21),
    (2, 22),
    (2, 23),
    (3, 9),
    (3, 11),
    (3, 12),
    (3, 13),
    (3, 14),
    (3, 15),
    (3, 17),
    (3, 18),
    (3, 19),
    (3, 20),
    (3, 21),
    (3, 22),
    (3, 23),
    (3, 24),
    (3, 25),
    (3, 26),
    (4, 8),
    (4, 10),
    (4, 11),
    (4, 12),
    (4, 13),
    (4, 14),
    (4, 17),
    (4, 18),
    (4, 19),
    (4, 20),
    (4, 21),
    (4, 22),
    (4, 23),
    (4, 24),
    (4, 25),
    (4, 26),
    (4, 27),
    (4, 28),
    (5, 7),
    (5, 8),
    (5, 9),
    (5, 10),
    (5, 11),
    (5, 12),
    (5, 15),
    (5, 16),
    (5, 17),
    (5, 18),
    (5, 19),
    (5, 20),
    (5, 21),
    (5, 23),
    (5, 24),
    (5, 25),
    (5, 26),
    (5, 27),
    (5, 28),
    (5, 29),
    (5, 30),
    (6, 3),
    (6, 7),
    (6, 8),
    (6, 9),
    (6, 10),
    (6, 11),
    (6, 16),
    (6, 17),
    (6, 18),
    (6, 19),
    (6, 20),
    (6, 21),
    (6, 22),
    (6, 23),
    (6, 24),
    (6, 25),
    (6, 26),
    (6, 27),
    (6, 28),
    (6, 29),
    (6, 30),
    (6, 31),
    (7, 3),
    (7, 7),
    (7, 8),
    (7, 9),
    (7, 10),
    (7, 11),
    (7, 15),
    (7, 16),
    (7, 17),
    (7, 18),
    (7, 19),
    (7, 20),
    (7, 21),
    (7, 22),
    (7, 23),
    (7, 24),
    (7, 25),
    (7, 26),
    (7, 27),
    (7, 28),
    (7, 29),
    (7, 30),
    (7, 31),
    (7, 32),
    (7, 33),
    (8, 1),
    (8, 2),
    (8, 8),
    (8, 9),
    (8, 10),
    (8, 11),
    (8, 12),
    (8, 13),
    (8, 16),
    (8, 18),
    (8, 19),
    (8, 20),
    (8, 21),
    (8, 22),
    (8, 23),
    (8, 25),
    (8, 26),
    (8, 27),
    (8, 28),
    (8, 29),
    (8, 30),
    (8, 31),
    (8, 32),
    (8, 33),
    (8, 34),
    (9, 1),
    (9, 2),
    (9, 3),
    (9, 4),
    (9, 8),
    (9, 9),
    (9, 10),
    (9, 11),
    (9, 12),
    (9, 13),
    (9, 14),
    (9, 16),
    (9, 19),
    (9, 21),
    (9, 22),
    (9, 23),
    (9, 25),
    (9, 27),
    (9, 28),
    (9, 29),
    (9, 30),
    (9, 31),
    (9, 32),
    (9, 33),
    (9, 34),
    (10, 2),
    (10, 3),
    (10, 4),
    (10, 5),
    (10, 10),
    (10, 11),
    (10, 12),
    (10, 13),
    (10, 14),
    (10, 17),
    (10, 19),
    (10, 20),
    (10, 21),
    (10, 22),
    (10, 23),
    (10, 27),
    (10, 28),
    (10, 29),
    (10, 30),
    (10, 31),
    (10, 32),
    (10, 33),
    (11, 3),
    (11, 4),
    (11, 5),
    (11, 6),
    (11, 8),
    (11, 10),
    (11, 11),
    (11, 12),
    (11, 13),
    (11, 14),
    (11, 15),
    (11, 19),
    (11, 20),
    (11, 21),
    (11, 22),
    (11, 23),
    (11, 27),
    (11, 28),
    (11, 29),
    (11, 30),
    (11, 31),
    (11, 32),
    (12, 11),
    (12, 12),
    (12, 13),
    (12, 16),
    (12, 17),
    (12, 19),
    (12, 20),
    (12, 24),
    (12, 27),
    (12, 28),
    (12, 29),
    (12, 30),
    (13, 12),
    (13, 13),
    (13, 17),
    (13, 20),
    (13, 21),
    (13, 22),
    (13, 28),
    (14, 13),
    (14, 14),
    (14, 15),
    (14, 16),
    (14, 18),
    (14, 22),
]
PLATFORM_NAME = "modis"
DST_CRS = "EPSG:3857"


def process_modis_tile(
    index: int,
    bucket: str,
) -> None:

    # Prepare resources for the job
    catalog = pystac_client.Client.open(STAC_API, modifier=pc.sign_inplace)

    s3 = boto3.resource("s3")

    i, j = SIN_GRID_TILES[index]

    items_to_process = []
    for year in [2018, 2020, 2022, 2023]:
        # Sample four months randomly
        random.seed(i * j * year)
        months = random.sample(range(1, 13), 4)
        for month in months:
            # Compute date range for this month
            end = calendar.monthrange(year, month)[1]
            timerange = (
                f"{year}-{str(month).zfill(2)}-01/"
                f"{year}-{str(month).zfill(2)}-{str(end).zfill(2)}"
            )
            # Query catalog
            items = catalog.search(
                collections=[COLLECTION],
                datetime=timerange,
                query={
                    "modis:vertical-tile": {
                        "eq": i,
                    },
                    "modis:horizontal-tile": {
                        "eq": j,
                    },
                },
                max_items=1,
            )
            items = list(items.item_collection())

            if not len(items):
                print(f"No items found for timerange {timerange}")
                continue

            items_to_process.append(items[0])

    for item in items_to_process:
        for key in list(item.assets.keys()):
            if key not in BANDS:
                del item.assets[key]

        # Manually set datetime to end date. Modis products are
        # composited from a date range.
        item.datetime = datetime.strptime(
            item.properties["end_datetime"], "%Y-%m-%dT%H:%M:%SZ"
        )

        for key, asset in item.assets.items():
            new_key = f"{PLATFORM_NAME}/{item.id}/{Path(asset.href.split('?')[0]).name}"
            new_href = f"s3://{bucket}/{new_key}"

            with rasterio.open(asset.href) as src:
                transform, width, height = calculate_default_transform(
                    src.crs, DST_CRS, src.width, src.height, *src.bounds
                )
                kwargs = src.meta.copy()
                kwargs.update(
                    {
                        "crs": DST_CRS,
                        "transform": transform,
                        "width": width,
                        "height": height,
                        "compress": "deflate",
                    }
                )
                with tempfile.NamedTemporaryFile(mode="w") as temp_file:
                    with rasterio.open(temp_file.name, "w", **kwargs) as dst:
                        for i in range(1, src.count + 1):
                            reproject(
                                source=rasterio.band(src, i),
                                destination=rasterio.band(dst, i),
                                src_transform=src.transform,
                                src_crs=src.crs,
                                dst_transform=transform,
                                dst_crs=DST_CRS,
                                resampling=Resampling.nearest,
                            )
                    s3_client = boto3.client("s3")
                    s3_client.upload_file(temp_file.name, bucket, new_key)

            item.assets[key].href = new_href

        # Update proj extension to match new data format
        item.properties["proj:shape"] = (height, width)
        item.properties["proj:epsg"] = 3857
        del item.properties["proj:wkt2"]
        item.properties["proj:transform"] = transform

        # Convert Dictionary to JSON String
        data_string = json.dumps(item.to_dict())

        # Upload JSON String to an S3 Object
        s3_bucket = s3.Bucket(name=bucket)
        s3_bucket.put_object(
            Key=f"{PLATFORM_NAME}/{item.id}/stac_item.json",
            Body=data_string,
        )

        indexer = ModisIndexer(item)
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
    if "STACCHIP_BUCKET" not in os.environ:
        raise ValueError("STACCHIP_BUCKET env var not set")

    index = int(os.environ["AWS_BATCH_JOB_ARRAY_INDEX"])
    bucket = os.environ["STACCHIP_BUCKET"]

    process_modis_tile(index, bucket)
