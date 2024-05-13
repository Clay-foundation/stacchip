The [Chipper](https://github.com/Clay-foundation/stacchip/blob/main/stacchip/chipper.py) class can be used to create chips based on
an existing stacchip index. 

There are multiple ways to instanciate the chipper class. Either point to a parquete file on S3, to a local parquet file, or pass a geoparquet table object to the instanciator. Once instantiated, any chip can be generated for a chip index, or all the chips can be returned by iterating over the chipper.

The following code snippet gives an example using a local path.

```python
from stacchip.chipper import Chipper
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
)
chip_index_x = table.column("chip_index_x")[row].as_py()
chip_index_y = table.column("chip_index_y")[row].as_py()
data = chipper.chip(chip_index_x, chip_index_y)
```