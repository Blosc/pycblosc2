import pycblosc2 as cb2
import numpy as np
import unittest


class TestUM(unittest.TestCase):

    def setUp(self):
        self.arr_1 = np.random.randint(5, size=(1000, 1000), dtype=np.int32)
        self.arr_2 = np.random.randint(5, size=(1000, 1000), dtype=np.int32)
        self.arr_3 = np.random.randint(5, size=(1000, 1000), dtype=np.int32)
        self.arr_aux = np.random.randint(5, size=(100, 100), dtype=np.int32)

    def test_compress_decompress(self):
        cb2.blosc_compress(5, 1, 4, 1000000 * 4, self.arr_1, self.arr_2, 1000000 * 4)
        cb2.blosc_decompress(self.arr_2, self.arr_3, 1000000 * 4)
        np.testing.assert_array_equal(self.arr_1, self.arr_3)

    def test_compress_getitem(self):
        cb2.blosc_compress(5, 0, 4, 1000000 * 4, self.arr_1, self.arr_2, 1000000 * 4)
        cb2.blosc_getitem(self.arr_2, 1000, 10000, self.arr_aux)
        arr_1 = self.arr_1[1:11, :].reshape(100, 100)
        np.testing.assert_array_equal(arr_1, self.arr_aux)

    def test_threads(self):
        cb2.blosc_set_nthreads(2)
        n = cb2.blosc_get_nthreads()
        self.assertEqual(n, 2)

    def test_set_compressor(self):
        cb2.blosc_set_compressor('lz4hc')
        n = cb2.blosc_get_compressor()
        self.assertEqual(n, 'lz4hc')
        cb2.blosc_set_compressor(b'lz4')
        n = cb2.blosc_get_compressor()
        self.assertEqual(n, 'lz4')

    def test_blosc_options(self):
        cb2.blosc_compress(5, 0, 4, 1000000 * 4, self.arr_1, self.arr_2, 1000000 * 4)
        _, flag = cb2.blosc_cbuffer_metainfo(self.arr_2)
        self.assertEqual(flag, [0, 0, 0, 0])
        cb2.blosc_set_delta(1)
        cb2.blosc_compress(5, 1, 4, 1000000 * 4, self.arr_1, self.arr_2, 1000000 * 4)
        _, flag = cb2.blosc_cbuffer_metainfo(self.arr_2)
        self.assertEqual(flag, [1, 0, 0, 1])
        cb2.blosc_set_delta(0)
        cb2.blosc_compress(5, 2, 4, 1000000 * 4, self.arr_1, self.arr_2, 1000000 * 4)
        _, flag = cb2.blosc_cbuffer_metainfo(self.arr_2)
        self.assertEqual(flag, [0, 1, 0, 0])

    def test_context(self):
        cparams = cb2.blosc2_create_cparams(compcode=1, clevel=5, use_dict=0, typesize=4,
                                            nthreads=1, blocksize=0, schunk=None,
                                            filters=[0, 0, 0, 0, 1],
                                            filters_meta=[0, 0, 0, 0, 0])
        cctx = cb2.blosc2_create_cctx(cparams)
        dparams = cb2.blosc2_create_dparams(nthreads=1, schunk=None)
        dctx = cb2.blosc2_create_dctx(dparams)
        cb2.blosc2_compress_ctx(cctx, 1000000 * 4, self.arr_1, self.arr_2, 1000000 * 4)
        cb2.blosc2_decompress_ctx(dctx, self.arr_2, self.arr_3, 1000000 * 4)
        np.testing.assert_array_equal(self.arr_1, self.arr_3)
        cb2.blosc2_getitem_ctx(dctx, self.arr_2, 1000, 10000, self.arr_aux)
        arr_1 = self.arr_1[1:11, :].reshape(100, 100)
        np.testing.assert_array_equal(arr_1, self.arr_aux)

    def test_schunk(self):
        cparams = cb2.blosc2_create_cparams(compcode=1, clevel=5, use_dict=0, typesize=4,
                                            nthreads=1, blocksize=0, schunk=None,
                                            filters=[0, 0, 0, 0, 1],
                                            filters_meta=[0, 0, 0, 0, 0])
        cparams.typesize = 4
        cparams.filters[0] = 3
        cparams.clevel = 9
        dparams = cb2.blosc2_create_dparams(nthreads=1, schunk=None)
        schunk = cb2.blosc2_new_schunk(cparams, dparams)
        nchunks = cb2.blosc2_append_buffer(schunk, 4 * 1000000, self.arr_1)
        cb2.blosc2_decompress_chunk(schunk, nchunks - 1, self.arr_3, 4 * 1000000)
        np.testing.assert_array_equal(self.arr_1, self.arr_3)

    def test_blocksize(self):
        n = cb2.blosc_get_blocksize()
        self.assertEqual(n, 0)
        cb2.blosc_set_blocksize(32 * 1024)
        m = cb2.blosc_get_blocksize()
        self.assertEqual(m, 32 * 1024)

    def test_string_decode(self):
        cb2.blosc_compress(5, 1, 4, 1000000 * 4, self.arr_1, self.arr_2, 1000000 * 4)
        cb2.blosc_decompress(self.arr_2, self.arr_3, 1000000 * 4)

        compcode1 = cb2.blosc_compname_to_compcode(b'lizard')
        compcode2 = cb2.blosc_compname_to_compcode('lizard')
        self.assertEqual(compcode1, compcode2)
        compname = cb2.blosc_compcode_to_compname(1)[1]
        self.assertEqual(compname, 'lz4')
        complib1 = cb2.blosc_get_complib_info(b'lz4')
        complib2 = cb2.blosc_get_complib_info('lz4')
        self.assertEqual(complib1, complib2)


if __name__ == '__main__':
    unittest.main()
