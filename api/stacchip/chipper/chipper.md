# Module stacchip.chipper

## Classes

### Chipper

```python3
class Chipper(
    indexer: stacchip.indexer.ChipIndexer,
    mountpath: Optional[str] = None,
    asset_blacklist: Optional[List[str]] = None
)
```

Chipper class

#### Methods

    
#### chip

```python3
def chip(
    self,
    x: int,
    y: int
) -> dict
```

Chip pixel array for the x and y index numbers

    
#### get_pixels_for_asset

```python3
def get_pixels_for_asset(
    self,
    key: str,
    x: int,
    y: int
) -> Union[numpy._typing._array_like._SupportsArray[numpy.dtype[Any]], numpy._typing._nested_sequence._NestedSequence[numpy._typing._array_like._SupportsArray[numpy.dtype[Any]]], bool, int, float, complex, str, bytes, numpy._typing._nested_sequence._NestedSequence[Union[bool, int, float, complex, str, bytes]]]
```

Extract chip pixel values for one asset