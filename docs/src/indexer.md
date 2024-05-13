The [Indexer](https://github.com/Clay-foundation/stacchip/blob/main/stacchip/indexer.py) class is build to create a chip index for
data registered in a a STAC item. The indexer will calculate the number of available
chips in a STAC item given a chip size. The resulting chip index is stored as a geoparquet table.

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

## Nodata and cloud coverage

Earth observation data often comes in scenes that contain
nodata pixels, and the imagery might contain clouds. Statistics on nodata and cloud cover is  relevant information for model training. Typically a model is trained with limited amounts nodata and cloud pixels.

The indexer therefore needs to be track these two variables so that the modeler can choose how much or how little nodata pixels and cloudy pixels should be passed to the model. However, how this information is stored varies for different image sources.

The indexer class might need adaption for new data sources. In these cases,
the base class has to be subclassed and the `get_stats` method overridden to produce the right statistics.

The stacchip library has a generic indexer for sources that have neither nodata or cloudy pixels in them. It has one indexer that takes a nodata mask as input, but assumes that there are no cloudy pixels (useful for sentinel-1). It also contains specific indexers for Landsat and Sentinel-2. For more information consult the reference documentation.

## Merging indexes

Stacchip indexes are geoparquet tables, and as such they can be merged quite
easily in to a single table. The recommendation is to store each stacchip index
for a single STAC item in a subfolder, then the files can be merged and the 
STAC item can be tracked using the folder structure using partitioning feature
from pyarrow.

The following example assumes that each index file from a single STAC item is
in a subfolder that is named after the STAC item id.

```python
from pyarrow import dataset as ds

part = ds.partitioning(field_names=["item_id"])
data = ds.dataset(
    "/path/to/stacchip/indices",
    format="parquet",
    partitioning=part,
)
ds.write_dataset(
    data,
    "/path/to/combined-index",
    format="parquet",
)
```