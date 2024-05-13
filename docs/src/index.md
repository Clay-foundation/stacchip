# stacchip

Dynamically create image chips for eath observation machine learning
applications using a custom chip index based on STAC items.

Get a STAC item, index its contents, and create chips dynamically
like so

```python
# Get item from an existing STAC catalog
item = stac.search(...)

# Index all chips that could be derived from the STAC item
index = Indexer(item).create_index()

# Use the index to get RGB array for a specific chip
chip = Chipper(index).chip(x=23, y=42)
```

## Overview

Stacchip relies on three cloud oriented technologies. Cloud Optimized Geotiffs
(COG), Spatio Temporal Asset Catalogs (STAC), and GeoParquet. Instead of pre-creating millions of files of a fixed size, chips are indexed first in tables, and then created dynamically from the index files when needed. The imagery data itsel is kept in its original format and referenced in STAC items.

Creating chips with stacchip is composed of two steps:

1. Create a stacchip index from a set of STAC
2. Dynamically create pixel arrays for any chip in the stacchip index

Indexes can be created separately for different imagery sources, and combined
into larger indexes when needed. This makes mixing different imagery sources
simple, and allows for flexibility during the modeling process, as imagery sources
can be added and removed by only updating the combined index.

The mechanism is purposefully kept as generic as possible. The index creation
is done based on a STAC item alone, no other input is needed. Obtaining image
data for a chip that is registered in a stacchip index only requires a few
lines of code.

## License

See [LICENSE](https://github.com/developmentseed/titiler/blob/main/LICENSE)
