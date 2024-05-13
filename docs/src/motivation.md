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
