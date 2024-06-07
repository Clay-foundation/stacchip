# Module stacchip.indexer

## Classes

### ChipIndexer

```python3
class ChipIndexer(
    item: pystac.item.Item,
    chip_size: int = 256,
    chip_max_nodata: float = 0.5
)
```

Indexer base class

#### Descendants

* stacchip.indexer.NoStatsChipIndexer
* stacchip.indexer.NoDataMaskChipIndexer
* stacchip.indexer.LandsatIndexer
* stacchip.indexer.Sentinel2Indexer

#### Instance variables

```python3
bbox
```

Bounding box that covers all tiles

This is different from the bounding box of the STAC item
if the tiles don't fit into the number of pixels perfectly.

```python3
size
```

Number of tiles in this STAC item

```python3
x_size
```

Number of tiles vailable in x direction

```python3
y_size
```

Number of tiles vailable in y direction

#### Methods

    
#### assert_units_metre

```python3
def assert_units_metre(
    self
) -> None
```

Ensure input data has meters as units

    
#### create_index

```python3
def create_index(
    self
) -> pyarrow.lib.Table
```

The index for this STAC item

    
#### get_chip_bbox

```python3
def get_chip_bbox(
    self,
    x: int,
    y: int
) -> str
```

Bounding box for a chip

    
#### get_stats

```python3
def get_stats(
    self,
    x: int,
    y: int
) -> Tuple[float, float]
```

A function to write for each indexer that returns nodata and

cloud statistics for a chip

    
#### reproject

```python3
def reproject(
    self,
    geom
) -> shapely._geometry.GeometryType
```

Reproject a geometry into WGS84

    
#### setup_projector

```python3
def setup_projector(
    self
)
```

Prepare projection function to project geometries into WGS84

    
#### shape

```python3
def shape(
    ...
)
```

Shape of the STAC item data

Obtains the shape of the highest resolution band from
all the available bands.

    
#### transform

```python3
def transform(
    ...
)
```

The transform property from the STAC item

### LandsatIndexer

```python3
class LandsatIndexer(
    item: pystac.item.Item,
    chip_size: int = 256,
    chip_max_nodata: float = 0.5
)
```

Chip indexer for Landsat 8 and 9 STAC items

#### Ancestors (in MRO)

* stacchip.indexer.ChipIndexer

#### Instance variables

```python3
bbox
```

Bounding box that covers all tiles

This is different from the bounding box of the STAC item
if the tiles don't fit into the number of pixels perfectly.

```python3
size
```

Number of tiles in this STAC item

```python3
x_size
```

Number of tiles vailable in x direction

```python3
y_size
```

Number of tiles vailable in y direction

#### Methods

    
#### assert_units_metre

```python3
def assert_units_metre(
    self
) -> None
```

Ensure input data has meters as units

    
#### create_index

```python3
def create_index(
    self
) -> pyarrow.lib.Table
```

The index for this STAC item

    
#### get_chip_bbox

```python3
def get_chip_bbox(
    self,
    x: int,
    y: int
) -> str
```

Bounding box for a chip

    
#### get_stats

```python3
def get_stats(
    self,
    x: int,
    y: int
) -> Tuple[float, float]
```

Cloud and nodata percentage for a chip

Uses the qa band to compute these values.

    
#### qa

```python3
def qa(
    ...
)
```

The quality band data for the STAC item

    
#### reproject

```python3
def reproject(
    self,
    geom
) -> shapely._geometry.GeometryType
```

Reproject a geometry into WGS84

    
#### setup_projector

```python3
def setup_projector(
    self
)
```

Prepare projection function to project geometries into WGS84

    
#### shape

```python3
def shape(
    ...
)
```

Shape of the STAC item data

Obtains the shape of the highest resolution band from
all the available bands.

    
#### transform

```python3
def transform(
    ...
)
```

The transform property from the STAC item

### NoDataMaskChipIndexer

```python3
class NoDataMaskChipIndexer(
    item: pystac.item.Item,
    nodata_mask: Union[numpy._typing._array_like._SupportsArray[numpy.dtype[Any]], numpy._typing._nested_sequence._NestedSequence[numpy._typing._array_like._SupportsArray[numpy.dtype[Any]]], bool, int, float, complex, str, bytes, numpy._typing._nested_sequence._NestedSequence[Union[bool, int, float, complex, str, bytes]]],
    chip_size: int = 256,
    chip_max_nodata: float = 0.5
)
```

Chip indexer that takes the nodata mask as input and assumes that

there are no clouds in the image

#### Ancestors (in MRO)

* stacchip.indexer.ChipIndexer

#### Instance variables

```python3
bbox
```

Bounding box that covers all tiles

This is different from the bounding box of the STAC item
if the tiles don't fit into the number of pixels perfectly.

```python3
size
```

Number of tiles in this STAC item

```python3
x_size
```

Number of tiles vailable in x direction

```python3
y_size
```

Number of tiles vailable in y direction

#### Methods

    
#### assert_units_metre

```python3
def assert_units_metre(
    self
) -> None
```

Ensure input data has meters as units

    
#### create_index

```python3
def create_index(
    self
) -> pyarrow.lib.Table
```

The index for this STAC item

    
#### get_chip_bbox

```python3
def get_chip_bbox(
    self,
    x: int,
    y: int
) -> str
```

Bounding box for a chip

    
#### get_stats

```python3
def get_stats(
    self,
    x: int,
    y: int
) -> Tuple[float, float]
```

Cloud and nodata percentage for a chip

Assumes there are no cloudy pixels and computes nodata from mask

    
#### reproject

```python3
def reproject(
    self,
    geom
) -> shapely._geometry.GeometryType
```

Reproject a geometry into WGS84

    
#### setup_projector

```python3
def setup_projector(
    self
)
```

Prepare projection function to project geometries into WGS84

    
#### shape

```python3
def shape(
    ...
)
```

Shape of the STAC item data

Obtains the shape of the highest resolution band from
all the available bands.

    
#### transform

```python3
def transform(
    ...
)
```

The transform property from the STAC item

### NoStatsChipIndexer

```python3
class NoStatsChipIndexer(
    item: pystac.item.Item,
    chip_size: int = 256,
    chip_max_nodata: float = 0.5
)
```

Indexer that assumes that none of the chips have any clouds or nodata

#### Ancestors (in MRO)

* stacchip.indexer.ChipIndexer

#### Instance variables

```python3
bbox
```

Bounding box that covers all tiles

This is different from the bounding box of the STAC item
if the tiles don't fit into the number of pixels perfectly.

```python3
size
```

Number of tiles in this STAC item

```python3
x_size
```

Number of tiles vailable in x direction

```python3
y_size
```

Number of tiles vailable in y direction

#### Methods

    
#### assert_units_metre

```python3
def assert_units_metre(
    self
) -> None
```

Ensure input data has meters as units

    
#### create_index

```python3
def create_index(
    self
) -> pyarrow.lib.Table
```

The index for this STAC item

    
#### get_chip_bbox

```python3
def get_chip_bbox(
    self,
    x: int,
    y: int
) -> str
```

Bounding box for a chip

    
#### get_stats

```python3
def get_stats(
    self,
    x: int,
    y: int
) -> Tuple[float, float]
```

Cloud and nodata percentage for a chip

    
#### reproject

```python3
def reproject(
    self,
    geom
) -> shapely._geometry.GeometryType
```

Reproject a geometry into WGS84

    
#### setup_projector

```python3
def setup_projector(
    self
)
```

Prepare projection function to project geometries into WGS84

    
#### shape

```python3
def shape(
    ...
)
```

Shape of the STAC item data

Obtains the shape of the highest resolution band from
all the available bands.

    
#### transform

```python3
def transform(
    ...
)
```

The transform property from the STAC item

### Sentinel2Indexer

```python3
class Sentinel2Indexer(
    item: pystac.item.Item,
    chip_size: int = 256,
    chip_max_nodata: float = 0.5
)
```

Indexer for Sentinel-2 STAC items

#### Ancestors (in MRO)

* stacchip.indexer.ChipIndexer

#### Class variables

```python3
nodata_value
```

```python3
scl_filter
```

#### Instance variables

```python3
bbox
```

Bounding box that covers all tiles

This is different from the bounding box of the STAC item
if the tiles don't fit into the number of pixels perfectly.

```python3
size
```

Number of tiles in this STAC item

```python3
x_size
```

Number of tiles vailable in x direction

```python3
y_size
```

Number of tiles vailable in y direction

#### Methods

    
#### assert_units_metre

```python3
def assert_units_metre(
    self
) -> None
```

Ensure input data has meters as units

    
#### create_index

```python3
def create_index(
    self
) -> pyarrow.lib.Table
```

The index for this STAC item

    
#### get_chip_bbox

```python3
def get_chip_bbox(
    self,
    x: int,
    y: int
) -> str
```

Bounding box for a chip

    
#### get_stats

```python3
def get_stats(
    self,
    x: int,
    y: int
) -> Tuple[float, float]
```

Cloud and nodata percentage for a chip

Uses the SCL band to compute these values.

    
#### reproject

```python3
def reproject(
    self,
    geom
) -> shapely._geometry.GeometryType
```

Reproject a geometry into WGS84

    
#### scl

```python3
def scl(
    ...
)
```

The Scene Classification (SCL) band data for the STAC item

    
#### setup_projector

```python3
def setup_projector(
    self
)
```

Prepare projection function to project geometries into WGS84

    
#### shape

```python3
def shape(
    ...
)
```

Shape of the STAC item data

Obtains the shape of the highest resolution band from
all the available bands.

    
#### transform

```python3
def transform(
    ...
)
```

The transform property from the STAC item