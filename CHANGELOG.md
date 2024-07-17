# Stacchip change log

## 0.1.34

- Add option to manually specify indexer shape. Some STAC items
  may not have the property `proj:shape` specified.

## 0.1.33

- Breaking change: `get_chip_bbox` returns shapely polygon instead of wkt

## 0.1.32

- Breacking change: chip iterator returns chip index values, not only image data.

## 0.1.31

- Breaking change: simplify chipper class. Indexer has to be instantiated by the user.