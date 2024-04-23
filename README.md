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

Stacchip comes with [processors](stacchip/processors/) that
can be used to collect and index imagery from multiple data sources.
This will be extended as the package grows.

Each processor is registered as a command line utility so that it can be
scaled easily.

### Sentinel-2

The [`stacchip-sentinel-2`](stacchip/processors/sentinel_2_processor.py)
processor CLi command processes Sentinel-2 data. It will process MGRS
tiles from a list of tiles from a layer that can be opened by geopandas.

Each MGRS tile will be processed by the row index in the source file.

For each tile it will process the least cloudy image in each quartal
from two random years between 2018 and 2023.

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

### Landsat

The [`stacchip-landsat`](stacchip/processors/landsat_processor.py)
processor CLI command processes Landsat data. It will process a list
of geometries from a layer that can be opened by geopandas. For each
row, it will use the centroid of the geometry to search for landsat
scenes.

For each geometry it will process the least cloudy image in each quartal
from two random years between 2018 and 2023. For one year it will collect
L1 data, and for the other year L2 data. The platform is either Landsat-8
or Landsat-9, depending on availability and cloud cover.

The script uses environment variables to determine all inputs:

1. The index of geometry to be processes from the source file
2. The source file for the source sample file
3. A target bucket for writing the assets, stac items, and stacchip index.

An example set of environment variables to run this script is:

```bash
export AWS_BATCH_JOB_ARRAY_INDEX=0
export STACCHIP_SAMPLE_SOURCE=https://clay-mgrs-samples.s3.amazonaws.com/mgrs_sample_v02.fgb
export STACCHIP_BUCKET=clay-v1-data
```

### NAIP

The [`stacchip-naip`](stacchip/processors/naip_processor.py) processor CLI
command processes imagery from the National Imagery Program (NAIP).

The sample locations were created using the [Natural Earth](https://www.naturalearthdata.com)
database as a source. The sample includes all popluated places, protected
areas and parks, airports, and ports. In addition, we sampled one random point 
along each river, and one random location within each lake that is registered
in Natural Earth. Finally, we sampled 4000 random points. All data was 
filtered to be within the CONUS region.

Similar to the other processors, the input variables are provided using env vars.

An example set of environment variables to run this script is:

```bash
export AWS_BATCH_JOB_ARRAY_INDEX=0
export STACCHIP_SAMPLE_SOURCE=https://clay-mgrs-samples.s3.amazonaws.com/clay_v1_naip_sample_natural_earth.fgb
export STACCHIP_BUCKET=clay-v1-data
```

### LINZ

The [`stacchip-linz`](stacchip/processors/linz_processor.py) processor CLI
processes data from the New Zealand high resolution open aerial imagery.

As a sample, we randomly select 50% the scenes, whith a minimum of 10
and a maximum of 2000 scenes for each catalog that was included.
We selected the latest imagery for each of the available regions
of new zealand. The list of catalogs is in the linz processor file.

We also resample all the imagery to 30cm so that the data
is consistent.

Similar to the other processors, the input variables are provided using env vars.

An example set of environment variables to run this script is:

```bash
export AWS_BATCH_JOB_ARRAY_INDEX=0
export STACCHIP_BUCKET=clay-v1-data
```

## Batch processing

The following base image can be used for batch processing. Installing the package
will include the command line utilities for each processor.

```dockerfile
FROM python:3.11

RUN pip install https://github.com/Clay-foundation/stacchip/archive/refs/heads/main.zip
```

## Prechip

In cases where chips need to be computed in advance, the
[`stacchip-prechip`](stacchip/processors/naip_processor.py) cli script
is a helper to create npz files from the chips.
