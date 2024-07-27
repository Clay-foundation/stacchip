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

## Installation

Stacchip is available on pypi

```bash
pip install stacchip
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

## Motivation

Remote sensing imagery is typically distributed in large files (scenes)
that typically have the order of 10 thousand of pixels in both the x and y
directions. This is true for systems like Landsat, Sentinel 1 and 2, and
aerial imagery such as NAIP.

Machine learning models operate on much smaller image sizes. Many use
256x256 pixels, and the largest inputs are in the range of 1000 pixels.

This poses a challenge to modelers, as they have to cut the larger scenes
into pieces before passing them to their models. The smaller image snippets
are typically referred to as "chips". A term we will use throughout this
documentation.

Creating imagery chips tends to be a tedious and slow process, and it is
specific for each model. Models will have different requirements on image
sizes, datatypes, and the spectral bands to include. A set of chips that
works for one model might be useless for the next.

Systemizing how chips are tracked, and making the chip creation more dynamic
is a way to work around these difficulties. This is the goal fo stacchip. It
presents an approach that leverages cloud optimized technology to make chipping
simpler, faster, and less static.

## License

This repository is released under an Apache 2.0 license. For more details see
[LICENSE](https://github.com/clay-foundation/stacchip/blob/main/LICENSE)
