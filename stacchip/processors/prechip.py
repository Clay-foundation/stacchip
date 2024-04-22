import datetime
import math
import os
from io import BytesIO
from multiprocessing import Pool, cpu_count
from typing import Union

import boto3
import numpy as np
import pyarrow as pa
import shapely
from pyarrow import dataset as da

from stacchip.chipper import Chipper

VERSION = "mode_v1_chipper_v1"


def normalize_timestamp(date):

    week = date.isocalendar().week * 2 * np.pi / 52
    hour = date.hour * 2 * np.pi / 24

    return (math.sin(week), math.cos(week)), (math.sin(hour), math.cos(hour))


def normalize_latlon(bounds):
    bounds = shapely.from_wkt(bounds).bounds
    lon = bounds[0] + (bounds[2] - bounds[0]) / 2
    lat = bounds[1] + (bounds[3] - bounds[1]) / 2

    lat = lat * np.pi / 180
    lon = lon * np.pi / 180

    return (math.sin(lat), math.cos(lat)), (math.sin(lon), math.cos(lon))


def write_chip(
    data_bucket: str,
    chip_bucket: str,
    row: int,
    platform: str,
    item_id: str,
    date: Union[datetime.date, datetime.datetime],
    chip_index_x: str,
    chip_index_y: str,
):
    print(
        "Writing chip",
        data_bucket,
        chip_bucket,
        row,
        platform,
        item_id,
        date,
        chip_index_x,
        chip_index_y,
    )

    chipper = Chipper(
        bucket=data_bucket,
        platform=platform,
        item_id=item_id,
        chip_index_x=chip_index_x,
        chip_index_y=chip_index_y,
    )
    chip = chipper.chip

    if platform == "naip":
        pixels = chip["image"]
        bands = ["red", "green", "blue", "nir"]
    elif platform == "linz":
        pixels = chip["asset"]
        # Some imagery has an alpha band which we won't keep
        if pixels.shape[0] != 3:
            pixels = pixels[:3]
        bands = ["red", "green", "blue"]
    elif platform in ["landsat-c2l2-sr", "landsat-c2l1", "sentinel-2-l2a"]:
        pixels = np.vstack(list(chip.values()))
        bands = list(chip.keys())

    if len(pixels) != len(bands):
        raise ValueError(
            f"Pixels shape {pixels.shape} is not equal to nr of bands {bands} for item {item_id}"
        )

    if isinstance(date, datetime.date):
        # Assume noon for dates without timestamp
        date = datetime.datetime(date.year, date.month, date.day, 12)
    week_norm, hour_norm = normalize_timestamp(date)

    bounds = chipper.indexer.get_chip_bbox(chip_index_x, chip_index_y)
    lon_norm, lat_norm = normalize_latlon(bounds)

    key = f"{VERSION}/{platform}/{date.year}/chip_{row}.npz"
    # target.parent.mkdir(parents=True, exist_ok=True)

    client = boto3.client("s3")
    with BytesIO() as bytes:
        np.savez_compressed(
            file=bytes,
            pixels=pixels,
            platform=platform,
            bands=bands,
            bounds=bounds,
            lon_norm=lon_norm,
            lat_norm=lat_norm,
            date=date,
            week_norm=week_norm,
            hour_norm=hour_norm,
            gsd=abs(chipper.indexer.transform[0]),
        )
        bytes.seek(0)
        client.upload_fileobj(Fileobj=bytes, Bucket=chip_bucket, Key=key)


def process() -> None:

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
    platform = os.environ.get("STACCHIP_PLATFORM")
    chips_per_job = int(os.environ.get("CHIPS_PER_JOB", 1000))

    # Open table
    table = da.dataset(indexpath, format="parquet").to_table(
        columns=["chipid", "platform", "item", "date", "chip_index_x", "chip_index_y"]
    )
    if platform:
        table = table.filter(pa.compute.field("platform") == platform)

    # Extract chips data for this job
    range_upper_limit = min(table.shape[0], (index + 1) * chips_per_job)
    all_chips = []
    for row in range(index * chips_per_job, range_upper_limit):
        all_chips.append(
            (
                data_bucket,
                chip_bucket,
                row,
                table.column("platform")[row].as_py(),
                table.column("item")[row].as_py(),
                table.column("date")[row].as_py(),
                table.column("chip_index_x")[row].as_py(),
                table.column("chip_index_y")[row].as_py(),
            )
        )

    # for i in range(5):
    #     write_chip(*all_chips[i])

    with Pool(cpu_count() * 2) as pl:
        pl.starmap(
            write_chip,
            all_chips,
        )
