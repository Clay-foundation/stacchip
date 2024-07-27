# Module stacchip.chipper

## Classes

### Chipper

```python3
class Chipper(
    indexer: stacchip.indexer.ChipIndexer,
    mountpath: Optional[str] = None,
    assets: Optional[List[str]] = None,
    asset_blacklist: Optional[List[str]] = None
)
```

Chipper class for managing and processing raster data chips.

#### Methods

    
#### chip

```python3
def chip(
    self,
    x: int,
    y: int
) -> dict
```

Retrieves chip pixel array for the specified x and y index numbers.

**Parameters:**

| Name | Type | Description | Default |
|---|---|---|---|
| x | int | The x index of the chip. | None |
| y | int | The y index of the chip. | None |

**Returns:**

| Type | Description |
|---|---|
| dict | A dictionary where keys are asset names and values are arrays of pixel values. |

    
#### get_pixels_for_asset

```python3
def get_pixels_for_asset(
    self,
    key: str,
    x: int,
    y: int
) -> Union[numpy._typing._array_like._SupportsArray[numpy.dtype[Any]], numpy._typing._nested_sequence._NestedSequence[numpy._typing._array_like._SupportsArray[numpy.dtype[Any]]], bool, int, float, complex, str, bytes, numpy._typing._nested_sequence._NestedSequence[Union[bool, int, float, complex, str, bytes]]]
```

Extracts chip pixel values for one asset.

**Parameters:**

| Name | Type | Description | Default |
|---|---|---|---|
| key | str | The asset key to extract pixels from. | None |
| x | int | The x index of the chip. | None |
| y | int | The y index of the chip. | None |

**Returns:**

| Type | Description |
|---|---|
| ArrayLike | Array of pixel values for the specified asset. |

**Raises:**

| Type | Description |
|---|---|
| ValueError | If asset dimensions are not multiples of the highest resolution dimensions. |