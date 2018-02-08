# pycblosc2



A simple Python/CFFI interface for the C-Blosc2 library.

This tries to be a low level interface for C-Blosc2.  Maybe in the future more high-level function could be added too.

This package is meant to be used in Python3.  Python is not tested at all, so use it at your own risk.

## Simple usage

```

In [1]: import numpy as np

In [2]: import pycblosc2 as cb2

# Create an input buffer
In [3]: a = np.arange(1000_000, dtype=np.int32)

# Create a buffer for compression
In [4]: b = np.empty(1000_000, dtype=np.int32)

# Create a buffer for decompression
In [5]: c = np.empty(1000_000, dtype=np.int32)

# Compress!
In [6]: cb2.blosc_compress(7, 1, a.dtype.itemsize, a.size * a.dtype.itemsize, a, b, b.size * b.dtype.itemsize)
Out[6]: 36256

# Decompress!
In [7]: cb2.blosc_decompress(b, c, c.size * c.dtype.itemsize)
Out[7]: 4000000

# Check for equality
In [8]: np.testing.assert_array_equal(a, c)

```

## Testing

```
$ python tests.py
```


## Authors

* **Aleix Alcacer**

* **Francesc Alted**

