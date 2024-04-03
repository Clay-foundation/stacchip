# stacchip

Create a chip index based on STAC items to dynamically create image
chips for machine learning applications.

## Overview

Stacchip is composed of three steps:

1. Create a stacchip index from a set of STAC items that contain data
   one wants to use for ML training.
2. Merge the indexes from each STAC item into a general index
3. Obtain pixels for any chip in the stacchip index

The mechanism is purposefully kept as generic as possible. The index creation
is done based on a STAC item alone, no other input is needed. Obtaining image
data for a chip that is registered in a stacchip index only requires a few
lines of code.

The following sections briefly describe the different components.

## The indexer

The [indexer](stacchip/indexer.py) class is build to create a chip index based on only a STAC
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

## Merger

The [merger](stacchip/merger.py) utility can be used to merge the STAC item
level stacchip indices into a single geopqarquet file. This is quite simple
thanks to parquet dataset partitioning.

## Chipper

The [Chipper](stacchip/chipper.py) class can be used to get pixel values for a single chip from
a stacchip index. The following code snippet gives an example.

```python
import geoarrow.pyarrow.dataset as gads

# Load a stacchip index table
dataset = gads.dataset("/path/to/parquet/index", format="parquet")
table = dataset.to_table()

# Get data for a single chip
row = 42
chipper = Chipper(
    bucket="clay-v1-data",
    platform=table.column("platform")[row],
    item_id = table.column("item")[row],
    chip_index_x = table.column("chip_index_x")[row].as_py(),
    chip_index_y = table.column("chip_index_y")[row].as_py()
)
data = chipper.chip
```

## Processors

To use stacchip for an existing imagery archive, the indexes need to be
created for each scene or STAC item.

There are stacchip comes with processors that can be used to collect and
index imagery from multiple data sources. This will be extended as the
package grows.

Each processor is registered as a command line utility so that it can be
scaled easily.

### Sentinel-2

The `stacchip-sentinel-2` processor CLi command processes Sentinel-2
data. It will process MGRS tiles from a list of tiles from a layer
that can be opened by geopandas.

Each MGRS tile will be processed by the row index in the source file.

The script uses environment variables to determine all inputs:

1. The index of the MGRS tile to be processes from the source file
2. The source file for the MGRS tile sample
3. A target bucket for writing the assets, stac items, and stacchip index.

An example set of environment variables to run this script is:

```bash
export AWS_BATCH_JOB_ARRAY_INDEX=0
export STACCHIP_MGRS_SOURCE=https://clay-mgrs-samples.s3.amazonaws.com/mgrs_sample_v02.fgb
export STACCHIP_BUCKET=clay-v1-data
```

## Batch processing

The following base image can be used for batch processing. Installing the package
will include the command line utilities for each processor.

```dockerfile
FROM python:3.11

RUN pip install https://github.com/Clay-foundation/stacchip/archive/refs/heads/main.zip
```
