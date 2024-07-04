import json
from pathlib import Path
from tempfile import TemporaryDirectory

import numpy as np
import rasterio
from numpy.testing import assert_array_equal
from pystac import Item

from stacchip.chipper import Chipper
from stacchip.indexer import NoStatsChipIndexer


def test_no_stats_indexer():
    with TemporaryDirectory() as dirname:
        mountpath = Path(dirname)
        target_dir = mountpath / "naip/item1"
        target_dir.mkdir(parents=True, exist_ok=True)
        item = Item.from_file("tests/data/stacchip_test_item.json")
        shape = item.properties["proj:shape"]
        size = shape[0] * shape[1]
        trsf = item.properties["proj:transform"]
        bands = 2
        with rasterio.open(
            mountpath / "naip/item1/asset.tif",
            "w",
            width=shape[1],
            height=shape[0],
            count=bands,
            dtype="uint8",
            transform=[trsf[2], trsf[0], trsf[1], trsf[5], trsf[4], trsf[3]],
        ) as rst:
            raster_data = np.random.randint(
                0, 255, bands * size, dtype="uint8"
            ).reshape((bands, *shape))
            rst.write(raster_data)

        item.assets["asset"].href = "s3://example-bucket/naip/item1/asset.tif"
        with open(mountpath / "naip/item1/stac_item.json", "w") as dst:
            dst.write(json.dumps(item.to_dict()))
        indexer = NoStatsChipIndexer(item)
        index = indexer.create_index()
        chipper = Chipper(indexer, mountpath=mountpath)
        x = index.column("chip_index_x")[1].as_py()
        y = index.column("chip_index_y")[2].as_py()
        chip = chipper.chip(x, y)
        assert chip["asset"].shape[0] == raster_data.shape[0]
        assert_array_equal(
            chip["asset"][0],
            raster_data[
                0,
                (y * indexer.chip_size) : ((y + 1) * indexer.chip_size),
                (x * indexer.chip_size) : ((x + 1) * indexer.chip_size),
            ],
        )
        # Test magic functions
        assert len(chipper) == indexer.size
        x_index, y_index, chipper_1 = chipper[1]
        assert x == x_index
        assert y == y_index
        assert_array_equal(chip["asset"][0], chipper_1["asset"][0])
        counter = 0
        for _chip in chipper:
            counter += 1
        assert counter == len(chipper)
