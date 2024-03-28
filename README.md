# stacchip

Dynamically create image chips from STAC items

## The indexer

The indexer class is build to create a chip index based on only a STAC
item as input. The indexer will calculate the number of available chips
given a chip size. The resulting chip index is returned as a geoparquet
table.

The index also calculates cloud cover and nodata percentages for each tile.
This is specific for each system. So the base class has to be subclassed
and the `get_stats` method overridden to produce the right statistics.

The following example creates an index the Landsat-9 STAC item from the tests

```python
from pystac import Item
from stacchip.indexer import LandsatIndexer

item = Item.from_file(
    "tests/data/landsat-c2l2-sr-LC09_L2SR_086107_20240311_20240312_02_T2_SR.json"
)
indexer = LandsatIndexer(item)
index = indexer.create_index()
```

## Processors

stacchip comes with processors that can be used to collect and index
imagery from multiple data sources.

### Sentinel-2

The `stacchip-sentinel-2` processor CLi command processes Sentinel-2
data. It will process MGRS tiles from a list of tiles from a layer
that can be opened by geopandas.

Each MGRS tile will be processed by the row index in the source file.

The script uses environment variables to determine all inputs:

1. The index of the MGRS tile to be processes from the source file
2. The source file for the MGRS tile sample
3. A target bucket for writing the assets, stac items, and stacchip index.

```bash
export AWS_BATCH_JOB_ARRAY_INDEX=0
export STACCHIP_MGRS_SOURCE=https://clay-mgrs-samples.s3.amazonaws.com/mgrs_sample_v02.fgb
export STACCHIP_BUCKET=clay-v1-data
```

## Batch processing

The following base image can be used for batch processing.

```dockerfile
FROM python:3.11

RUN pip install https://github.com/Clay-foundation/stacchip/archive/refs/tags/0.1.1.zip
```
