import datetime
import math
import os
from io import BytesIO
from multiprocessing import Pool
from typing import Union

import boto3
import numpy as np
import pyarrow as pa
from pyarrow import dataset as da

from stacchip.chipper import Chipper
from stacchip.utils import load_indexer_s3

VERSION = "mode_v1_chipper_v2"

CUBESIZE = 128

S2_BANDS = [
    "blue",
    "green",
    "red",
    "rededge1",
    "rededge2",
    "rededge3",
    "nir",
    "nir08",
    "swir16",
    "swir22",
]
LS_BANDS = [
    "red",
    "green",
    "blue",
    "nir08",
    "swir16",
    "swir22",
]
NAIP_BANDS = ["red", "green", "blue", "nir"]
LINZ_BANDS = ["red", "green", "blue"]
S1_BANDS = ["vv", "vh"]


def normalize_timestamp(date):

    week = date.isocalendar().week * 2 * np.pi / 52
    hour = date.hour * 2 * np.pi / 24

    return (math.sin(week), math.cos(week)), (math.sin(hour), math.cos(hour))


def normalize_latlon(bounds):
    lon = bounds[0] + (bounds[2] - bounds[0]) / 2
    lat = bounds[1] + (bounds[3] - bounds[1]) / 2

    lat = lat * np.pi / 180
    lon = lon * np.pi / 180

    return (math.sin(lat), math.cos(lat)), (math.sin(lon), math.cos(lon))


def stack_chips(chips: list, cube_id: int, chip_bucket: str, platform: str):
    print(f"Writing cube {cube_id}")

    pixels = np.stack([chip["pixels"] for chip in chips], dtype="float32")
    lon_norm = np.vstack([chip["lon_norm"] for chip in chips], dtype="float32")
    lat_norm = np.vstack([chip["lat_norm"] for chip in chips], dtype="float32")
    week_norm = np.vstack([chip["week_norm"] for chip in chips], dtype="float32")
    hour_norm = np.vstack([chip["hour_norm"] for chip in chips], dtype="float32")

    key = f"{VERSION}/{platform}/cube_{cube_id}.npz"

    client = boto3.client("s3")
    with BytesIO() as bytes:
        np.savez_compressed(
            file=bytes,
            pixels=pixels,
            lon_norm=lon_norm,
            lat_norm=lat_norm,
            week_norm=week_norm,
            hour_norm=hour_norm,
        )
        bytes.seek(0)
        client.upload_fileobj(Fileobj=bytes, Bucket=chip_bucket, Key=key)


def get_chip(
    data_bucket: str,
    row: int,
    platform: str,
    item_id: str,
    date: Union[datetime.date, datetime.datetime],
    chip_index_x: int,
    chip_index_y: int,
):
    print(
        "Getting chip",
        data_bucket,
        row,
        platform,
        item_id,
        date,
        chip_index_x,
        chip_index_y,
    )

    indexer = load_indexer_s3(
        bucket=data_bucket,
        platform=platform,
        item_id=item_id,
    )
    chipper = Chipper(indexer)

    chip = chipper.chip(chip_index_x, chip_index_y)

    if platform == "naip":
        pixels = chip["image"]
        bands = NAIP_BANDS
    elif platform == "linz":
        pixels = chip["asset"]
        bands = LINZ_BANDS
    elif platform == "sentinel-2-l2a":
        pixels = np.vstack([chip[band] for band in S2_BANDS])
        bands = S2_BANDS
    elif platform in ["landsat-c2l2-sr", "landsat-c2l1"]:
        pixels = np.vstack([chip[band] for band in LS_BANDS])
        bands = LS_BANDS
    elif platform == "sentinel-1-rtc":
        if any(band not in chip for band in S1_BANDS):
            return
        pixels = np.vstack([chip[band] for band in S1_BANDS])
        bands = S1_BANDS

    if len(pixels) != len(bands):
        raise ValueError(
            f"Pixels shape {pixels.shape} is not equal to nr of bands {bands} for item {item_id}"
        )

    if isinstance(date, datetime.date):
        # Assume noon for dates without timestamp
        date = datetime.datetime(date.year, date.month, date.day, 12)
    week_norm, hour_norm = normalize_timestamp(date)

    bounds = chipper.indexer.get_chip_bbox(chip_index_x, chip_index_y).bounds
    lon_norm, lat_norm = normalize_latlon(bounds)

    return {
        "pixels": pixels,
        "lon_norm": lon_norm,
        "lat_norm": lat_norm,
        "week_norm": week_norm,
        "hour_norm": hour_norm,
    }


def process() -> None:
    # GDAL read optimization is recommended
    # os.environ["GDAL_DISABLE_READDIR_ON_OPEN"] = "YES"
    # os.environ["CPL_VSIL_CURL_ALLOWED_EXTENSIONS"] = ".tif,.png,.jp2,.tiff"

    if "AWS_BATCH_JOB_ARRAY_INDEX" not in os.environ:
        raise ValueError("AWS_BATCH_JOB_ARRAY_INDEX env var not set")
    if "STACCHIP_DATA_BUCKET" not in os.environ:
        raise ValueError("STACCHIP_DATA_BUCKET env var not set")
    if "STACCHIP_INDEXPATH" not in os.environ:
        raise ValueError("STACCHIP_INDEXPATH env var not set")
    if "STACCHIP_CHIP_BUCKET" not in os.environ:
        raise ValueError("STACCHIP_TARGETPATH env var not set")

    index = int(os.environ["AWS_BATCH_JOB_ARRAY_INDEX"])
    data_bucket = os.environ["STACCHIP_DATA_BUCKET"]
    indexpath = os.environ["STACCHIP_INDEXPATH"]
    chip_bucket = os.environ["STACCHIP_CHIP_BUCKET"]
    platform = os.environ.get("STACCHIP_PLATFORM", "")
    cubes_per_job = int(os.environ.get("STACCHIP_CUBES_PER_JOB", 10))
    pool_size = int(os.environ.get("STACCHIP_POOL_SIZE", 10))
    chip_max_nodata = int(os.environ.get("STACCHIP_MAX_NODATA", 0.05))

    # Open table
    table = da.dataset(indexpath, format="parquet").to_table(
        columns=["chipid", "platform", "item", "date", "chip_index_x", "chip_index_y"]
    )
    if platform:
        table = table.filter(pa.compute.field("platform") == platform)

    initial_count = len(table)
    if chip_max_nodata:
        table = table.filter(pa.compute.field("nodata_percentage") <= chip_max_nodata)
    print(
        f"Dropped {initial_count - len(table)} chips due to nodata filter, keeping {len(table)}"
    )

    np.random.seed(42)
    random_rows = np.random.randint(0, len(table), len(table))

    for cube_id in range(index * cubes_per_job, (index + 1) * cubes_per_job):
        random_rows_cube = random_rows[cube_id * CUBESIZE : (cube_id + 1) * CUBESIZE]
        if len(random_rows_cube) != CUBESIZE:
            print("Finishing because of incomplete cubes")
            return

        # Extract chips data for this job
        all_chips = []
        for row in random_rows_cube:
            all_chips.append(
                (
                    data_bucket,
                    row,
                    table.column("platform")[row].as_py(),
                    table.column("item")[row].as_py(),
                    table.column("date")[row].as_py(),
                    table.column("chip_index_x")[row].as_py(),
                    table.column("chip_index_y")[row].as_py(),
                )
            )

        with Pool(pool_size) as pl:
            data = pl.starmap(
                get_chip,
                all_chips,
            )

        if None in data:
            print(f"Not all cubes are complete, skipping stacking for cube {cube_id}")
            continue

        stack_chips(data, cube_id=cube_id, chip_bucket=chip_bucket, platform=platform)
