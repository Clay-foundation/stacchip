import datetime
import math
import os
from multiprocessing import Pool
from pathlib import Path

import geoarrow.pyarrow.dataset as gads
import numpy as np
import shapely

from stacchip.chipper import Chipper

VERSION = "mode_v1_chipper_v1"


class Prechipper:

    def __init__(self, mountpath: str, indexpath: str, targetpath: str):
        self.mountpath = mountpath
        self.targetpath = Path(targetpath)
        self.table = gads.dataset(indexpath, format="parquet").to_table()

    def __len__(self):
        return self.table.shape[0]

    def normalize_timestamp(self, date):

        week = date.isocalendar().week * 2 * np.pi / 52
        hour = date.hour * 2 * np.pi / 24

        return (math.sin(week), math.cos(week)), (math.sin(hour), math.cos(hour))

    def normalize_latlon(self, bounds):
        bounds = shapely.from_wkt(bounds).bounds
        lon = bounds[0] + (bounds[2] - bounds[0]) / 2
        lat = bounds[1] + (bounds[3] - bounds[1]) / 2

        lat = lat * np.pi / 180
        lon = lon * np.pi / 180

        return (math.sin(lat), math.cos(lat)), (math.sin(lon), math.cos(lon))

    def write_chip(self, row: int):
        chip_index_x = self.table.column("chip_index_x")[row].as_py()
        chip_index_y = self.table.column("chip_index_y")[row].as_py()
        platform = str(self.table.column("platform")[row])

        chipper = Chipper(
            mountpath=self.mountpath,
            platform=platform,
            item_id=self.table.column("item")[row],
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
                f"Pixels shape {pixels.shape} is not equal to nr of bands {bands} for item {self.table.column('item')[row]}"
            )

        date = self.table.column("date")[row].as_py()
        if isinstance(date, datetime.date):
            # Assume noon for dates without timestamp
            date = datetime.datetime(date.year, date.month, date.day, 12)
        week_norm, hour_norm = self.normalize_timestamp(date)

        bounds = chipper.indexer.get_chip_bbox(chip_index_x, chip_index_y)
        lon_norm, lat_norm = self.normalize_latlon(bounds)

        target = self.targetpath / f"{VERSION}/{platform}/{date.year}/chip_{row}.npz"
        target.parent.mkdir(parents=True, exist_ok=True)
        np.savez_compressed(
            file=target,
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


def process() -> None:

    CHIPS_PER_BATCH = 10000

    if "AWS_BATCH_JOB_ARRAY_INDEX" not in os.environ:
        raise ValueError("AWS_BATCH_JOB_ARRAY_INDEX env var not set")
    if "STACCHIP_MOUNTPATH" not in os.environ:
        raise ValueError("STACCHIP_MOUNTPATH env var not set")
    if "STACCHIP_INDEXPATH" not in os.environ:
        raise ValueError("STACCHIP_INDEXPATH env var not set")
    if "STACCHIP_TARGETPATH" not in os.environ:
        raise ValueError("STACCHIP_TARGETPATH env var not set")

    index = int(os.environ["AWS_BATCH_JOB_ARRAY_INDEX"])

    prech = Prechipper(
        mountpath=os.environ["STACCHIP_MOUNTPATH"],
        indexpath=os.environ["STACCHIP_INDEXPATH"],
        targetpath=os.environ["STACCHIP_TARGETPATH"],
    )

    # for i in range(len(prech)):
    #     prech.write_chip(i)

    range_upper_limit = min(len(prech), (index + 1) * CHIPS_PER_BATCH)

    with Pool(10) as pl:
        pl.map(
            prech.write_chip,
            range(index * CHIPS_PER_BATCH, range_upper_limit),
        )


os.environ["STACCHIP_TARGETPATH"] = "/home/tam/Desktop/clay-v1-data-small/prechip-tiles"
os.environ["STACCHIP_INDEXPATH"] = "/home/tam/Desktop/clay-v1-data-small/index-combined"
os.environ["STACCHIP_MOUNTPATH"] = "/home/tam/Desktop/clay-v1-data-small"
os.environ["AWS_BATCH_JOB_ARRAY_INDEX"] = "0"
process()
