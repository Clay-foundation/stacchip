The [Chipper](https://github.com/Clay-foundation/stacchip/blob/main/stacchip/chipper.py) class can be used to create chips based on
an existing stacchip index.

The chipper class takes as input an Indexer class object. The indexer class can be instantiated using
the `load_indexer_s3` and `load_indexer_local` utils functions for indexes that have been
previously created using stacchip processors.

For local stacchip indexes, the mountpath can be passed. Asset links in the STAC items are then patched
with the local mountpath.

The chipper also has an `asset_blacklist` argument that allows skipping assets
from the chip retrieval process. This can be used to exclude unnecessary assets
and through that increase loading speed.

The following code snippet gives an example using a local path.

```python
import geoarrow.pyarrow.dataset as gads

from stacchip.chipper import Chipper
from stacchip.utils import load_indexer_s3

# Load a stacchip index table
dataset = gads.dataset("/path/to/parquet/index", format="parquet")
table = dataset.to_table()

# Use util to load indexer using data from a 
# remote S3 bucket.
indexer = load_indexer_s3(
    bucket="clay-v1-data",
    platform=table.column("platform")[row],
    item_id = table.column("item")[row],
)

# Instantiate chipper
chipper = Chipper(indexer)

# Get data for a single chip as registered
# in row 42 of the index.
row = 42
chip_index_x = table.column("chip_index_x")[row].as_py()
chip_index_y = table.column("chip_index_y")[row].as_py()
data = chipper.chip(chip_index_x, chip_index_y)
```