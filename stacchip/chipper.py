import json
import math

import boto3
import geoarrow.pyarrow.dataset as gads
import rasterio
from pystac import Item
from rasterio.enums import Resampling
from rasterio.windows import Window

from stacchip.indexer import ChipIndexer

ASSET_BLACKLIST = ["scl", "qa_pixel"]


def chip(index: str, row: int) -> dict:

    ds = gads.dataset(index, format="parquet")
    table = ds.to_table()

    platform = table.column("platform")[row]
    item_id = table.column("item")[row]

    bucket = "clay-v1-data"
    s3 = boto3.resource("s3")
    s3_bucket = s3.Bucket(name=bucket)
    content_object = s3_bucket.Object(f"{platform}/{item_id}/stac_item.json")
    file_content = content_object.get()["Body"].read().decode("utf-8")
    json_content = json.loads(file_content)
    item = Item.from_dict(json_content)

    indexer = ChipIndexer(item)
    chip_index_x = table.column("chip_index_x")[row].as_py()
    chip_index_y = table.column("chip_index_y")[row].as_py()

    stac_item_shape = indexer.shape

    data = {}

    for key, asset in indexer.item.assets.items():
        if key in ASSET_BLACKLIST:
            continue

        with rasterio.open(asset.href) as src:
            # Currently assume that different assets may be at different
            # resolutions, but are aligned and the gsd differs by an integer
            # multiplier.
            if stac_item_shape[0] % src.height:
                raise ValueError(
                    "Asset height {src.height} is not a multiple of highest resolution height {stac_item_shape[0]}"  # noqa: E501
                )

            if stac_item_shape[1] % src.width:
                raise ValueError(
                    "Asset width {src.width} is not a multiple of highest resolution height {stac_item_shape[1]}"  # noqa: E501
                )

            factor = stac_item_shape[0] / src.height
            if factor != 1:
                print(
                    f"Asset {key} is not at highest resolution using scaling factor of {factor}"  # noqa: E501
                )

            chip_window = Window(
                math.floor(chip_index_y * indexer.chip_size / factor),
                math.floor(chip_index_x * indexer.chip_size / factor),
                math.ceil(indexer.chip_size / factor),
                math.ceil(indexer.chip_size / factor),
            )

            print(f"Chip window for asset {key} is {chip_window}")
            pixels = src.read(
                window=chip_window,
                out_shape=(src.count, indexer.chip_size, indexer.chip_size),
                resampling=Resampling.nearest,
            )

            data[key] = pixels

    return data


data = chip("/home/tam/Desktop/output", 23)
print(data)
