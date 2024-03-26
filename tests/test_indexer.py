from pystac import Item
from stacchip.indexer import NoStatsChipIndexer, Sentinel2Indexer, LandsatIndexer


def test_no_stats_indexer():
    item = Item.from_file("tests/data/naip_m_4207009_ne_19_060_20211024.json")
    indexer = NoStatsChipIndexer(item)
    assert indexer.get_shape() == [12666, 9704]

def test_sentinel_2_indexer():
    item = Item.from_file("tests/data/sentinel-2-l2a-S2A_T20HNJ_20240311T140636_L2A.json")
    indexer = Sentinel2Indexer(item)
    assert indexer.get_shape() == [10980, 10980]

def test_landsat_indexer():
    item = Item.from_file("tests/data/landsat-c2l2-sr-LC09_L2SR_086107_20240311_20240312_02_T2_SR.json")
    indexer = LandsatIndexer(item)
    assert indexer.get_shape() == [8271, 8331]
