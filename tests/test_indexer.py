import datetime

import mock
import numpy as np
import pyarrow as pa
import pytest
from pystac import Item
from rasterio import Affine
from rasterio.io import MemoryFile

from stacchip.indexer import (ChipIndexer, LandsatIndexer, NoStatsChipIndexer,
                              Sentinel2Indexer)


def get_ls_mock(nodata: bool = False) -> MemoryFile:
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
    data = np.zeros((1, 8331, 8271))
    if nodata:
        data[0, :200, :200] = 1
    memfile = MemoryFile()
    with memfile.open(**meta) as dst:
        dst.write(data)
    return memfile.open()


def rasterio_open_ls_mock(href: str) -> MemoryFile:
    return get_ls_mock()


def rasterio_open_ls_nodata_mock(href: str) -> MemoryFile:
    return get_ls_mock(True)


def rasterio_open_sentinel_mock(href: str) -> MemoryFile:
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
    data = 5 * np.ones((1, 5490, 5490), dtype="uint16")
    # Make first chip nodata
    data[0, :100, :100] = 0
    # Make second chip cloudy
    data[0, :128, 128:192] = 1
    memfile = MemoryFile()
    with memfile.open(**meta) as dst:
        dst.write(data)
    return memfile.open()


def test_get_stats_error():
    item = Item.from_file("tests/data/naip_m_4207009_ne_19_060_20211024.json")
    indexer = ChipIndexer(item)
    with pytest.raises(NotImplementedError):
        indexer.create_index()


def test_no_stats_indexer():
    item = Item.from_file("tests/data/naip_m_4207009_ne_19_060_20211024.json")
    indexer = NoStatsChipIndexer(item)
    assert indexer.shape == [12666, 9704]
    index = indexer.create_index()
    assert str(index.column("chipid")[0]) == "m_4207009_ne_19_060_20211024.tif-0-0"
    assert index.column("date")[0] == pa.scalar(
        datetime.date(2021, 10, 24), pa.date32()
    )
    print(index.column("geometry")[0].wkt)
    assert (
        index.column("geometry")[0].wkt
        == "POLYGON ((-70.94268962889282 42.80920310538916, -70.94070808681735 42.80920310538916, -70.94070808681735 42.8106232038024, -70.94268962889282 42.8106232038024, -70.94268962889282 42.80920310538916))"
    )
    assert (
        min([dat["x"] for dat in index.column("geometry")[0].as_py()[0]])
        == item.bbox[0]
    )


@mock.patch("stacchip.indexer.rasterio.open", rasterio_open_sentinel_mock)
def test_sentinel_2_indexer():
    item = Item.from_file(
        "tests/data/sentinel-2-l2a-S2A_T20HNJ_20240311T140636_L2A.json"
    )
    indexer = Sentinel2Indexer(item)
    assert indexer.shape == [10980, 10980]
    index = indexer.create_index()
    assert index.shape == (1763, 8)
    assert str(index.column("chipid")[0]) == "S2A_T20HNJ_20240311T140636_L2A-1-0"
    assert index.column("cloud_cover_percentage")[0].as_py() == 0.5


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
    assert index.shape == (1024, 8)


@mock.patch("stacchip.indexer.rasterio.open", rasterio_open_ls_nodata_mock)
def test_landsat_indexer_nodata():
    item = Item.from_file(
        "tests/data/landsat-c2l2-sr-LC09_L2SR_086107_20240311_20240312_02_T2_SR.json"
    )
    indexer = LandsatIndexer(item)
    index = indexer.create_index()
    assert index.shape == (1023, 8)
    assert (
        str(index.column("chipid")[0])
        == "LC09_L2SR_086107_20240311_20240312_02_T2_SR-1-0"
    )

    indexer = LandsatIndexer(item, chip_max_nodata=0.95)
    index = indexer.create_index()
    assert index.shape == (1024, 8)
