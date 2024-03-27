import datetime

import mock
import numpy
import pyarrow as pa
import pytest
from pystac import Item
from rasterio import Affine
from rasterio.io import MemoryFile

from stacchip.indexer import (ChipIndexer, LandsatIndexer, NoStatsChipIndexer,
                              Sentinel2Indexer)


def test_get_stats_error():
    item = Item.from_file("tests/data/naip_m_4207009_ne_19_060_20211024.json")
    indexer = ChipIndexer(item)
    with pytest.raises(NotImplementedError):
        indexer.create_index()


def rasterio_open_ls_mock(href: str) -> numpy.ndarray:
    meta = {
        "driver": "GTiff",
        "dtype": "uint16",
        "nodata": None,
        "width": 8331,
        "height": 8271,
        "count": 1,
        "crs": "EPSG:3031",
        "transform": Affine(30.0, 0.0, 1517085.0, 0.0, -30.0, -1811685.0),
    }
    memfile = MemoryFile()
    with memfile.open(**meta) as dst:
        dst.write(numpy.ones((1, 8331, 8271), dtype="uint16"))
    return memfile.open()


def rasterio_open_sentinel_mock(href: str) -> numpy.ndarray:
    meta = {
        "driver": "GTiff",
        "dtype": "uint8",
        "nodata": 0.0,
        "width": 5490,
        "height": 5490,
        "count": 1,
        "crs": "EPSG:32720",
        "transform": Affine(20.0, 0.0, 499980.0, 0.0, -20.0, 6400000.0),
    }
    memfile = MemoryFile()
    with memfile.open(**meta) as dst:
        dst.write(5 * numpy.ones((1, 5490, 5490), dtype="uint16"))
    return memfile.open()


def test_no_stats_indexer():
    item = Item.from_file("tests/data/naip_m_4207009_ne_19_060_20211024.json")
    indexer = NoStatsChipIndexer(item)
    assert indexer.shape == [12666, 9704]
    index = indexer.create_index()
    assert str(index.column("chipid")[0]) == "m_4207009_ne_19_060_20211024.tif-0-0"
    assert index.column("date")[0] == pa.scalar(
        datetime.date(2021, 10, 24), pa.date32()
    )
    assert index.column("bbox_x_max")[0] == index.column("bbox_x_min")[1]
    assert index.column("bbox_y_min")[0] == index.column("bbox_y_min")[1]
    assert (
        index.column("bbox_y_max")[0]
        != index.column("bbox_y_min")[int(indexer.shape[1] / indexer.chip_size) - 1]
    )
    assert (
        index.column("bbox_y_max")[0]
        == index.column("bbox_y_min")[int(indexer.shape[1] / indexer.chip_size)]
    )


@mock.patch("stacchip.indexer.rasterio.open", rasterio_open_sentinel_mock)
def test_sentinel_2_indexer():
    item = Item.from_file(
        "tests/data/sentinel-2-l2a-S2A_T20HNJ_20240311T140636_L2A.json"
    )
    indexer = Sentinel2Indexer(item)
    assert indexer.shape == [10980, 10980]
    index = indexer.create_index()
    assert str(index.column("chipid")[0]) == "S2A_T20HNJ_20240311T140636_L2A-0-0"


@mock.patch("stacchip.indexer.rasterio.open", rasterio_open_ls_mock)
def test_landsat_indexer():
    item = Item.from_file(
        "tests/data/landsat-c2l2-sr-LC09_L2SR_086107_20240311_20240312_02_T2_SR.json"
    )
    indexer = LandsatIndexer(item)
    assert indexer.shape == [8271, 8331]
    index = indexer.create_index()
    assert isinstance(index, pa.Table)
    assert (
        str(index.column("chipid")[0])
        == "LC09_L2SR_086107_20240311_20240312_02_T2_SR-0-0"
    )
