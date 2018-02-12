# Simple CFFI wrapper for the C-Blosc2 library

from pkg_resources import get_distribution, DistributionNotFound
try:
    __version__ = get_distribution(__name__).version
except DistributionNotFound:
    # package is not installed
    from setuptools_scm import get_version
    __version__ = get_version()


from cffi import FFI

ffi = FFI()
ffi.cdef(
    """
    void blosc_init(void);

    void blosc_destroy(void);

    int blosc_compress(int clevel, int doshuffle, size_t typesize, size_t nbytes, const void* src, void* dest,
                       size_t destsize);

    int blosc_decompress(const void* src, void* dest, size_t destsize);

    int blosc_getitem(const void* src, int start, int nitems, void* dest);

    int blosc_get_nthreads(void);

    int blosc_set_nthreads(int nthreads);

    char* blosc_get_compressor(void);

    int blosc_set_compressor(const char* compname);

    void blosc_set_delta(int dodelta);

    int blosc_compcode_to_compname(int compcode, char** compname);

    int blosc_compname_to_compcode(const char* compname);

    char* blosc_list_compressors(void);

    char* blosc_get_version_string(void);

    int blosc_get_complib_info(char* compname, char** complib, char** version);

    int blosc_free_resources(void);

    void blosc_cbuffer_sizes(const void* cbuffer, size_t* nbytes, size_t* cbytes, size_t* blocksize);

    void blosc_cbuffer_metainfo(const void* cbuffer, size_t* typesize, int* flags);

    void blosc_cbuffer_versions(const void* cbuffer, int* version, int* versionlz);

    char* blosc_cbuffer_complib(const void* cbuffer);

    enum {
      BLOSC_MAX_FILTERS = 5, /* Maximum number of filters in the filter pipeline */
    };

    typedef struct blosc2_context_s blosc2_context;

    typedef struct {
      int compcode; /* the compressor codec */
      int clevel; /* the compression level (5) */
      int use_dict; /* use dicts or not when compressing (only for ZSTD) */
      size_t typesize; /* the type size (8) */
      uint32_t nthreads; /* the number of threads to use internally (1) */
      size_t blocksize; /* the requested size of the compressed blocks (0; meaning automatic) */
      void* schunk; /* the associated schunk, if any (NULL) */
      uint8_t filters[BLOSC_MAX_FILTERS]; /* the (sequence of) filters */
      uint8_t filters_meta[BLOSC_MAX_FILTERS]; /* metadata for filters */
    } blosc2_cparams;

    typedef struct {
      int32_t nthreads; /* the number of threads to use internally (1) */
      void* schunk; /* the associated schunk, if any (NULL) */
    } blosc2_dparams;

    blosc2_context* blosc2_create_cctx(blosc2_cparams cparams);

    blosc2_context* blosc2_create_dctx(blosc2_dparams dparams);

    void blosc2_free_ctx(blosc2_context* context);

    int blosc2_compress_ctx( blosc2_context* context, size_t nbytes, const void* src, void* dest, size_t destsize );

    int blosc2_decompress_ctx(blosc2_context* context, const void* src,  void* dest, size_t destsize);

    int blosc2_getitem_ctx(blosc2_context* context, const void* src, int start, int nitems, void* dest);

    typedef struct {
      uint8_t version;
      uint8_t flags1;
      uint8_t flags2;
      uint8_t flags3;
      uint8_t compcode;  // starts at 4 bytes
      /* The default compressor.  Each chunk can override this. */
      uint8_t clevel;  // starts at 6 bytes
      /* The compression level and other compress params */
      uint32_t typesize;
      /* the type size */
      int32_t blocksize;
      /* the requested size of the compressed blocks (0; meaning automatic) */
      uint32_t chunksize;   // starts at 8 bytes
      /* Size of each chunk.  0 if not a fixed chunksize. */
      uint8_t filters[BLOSC_MAX_FILTERS];  // starts at 12 bytes
      /* The (sequence of) filters.  8-bit per filter. */
      uint8_t filters_meta[BLOSC_MAX_FILTERS];
      /* Metadata for filters. 8-bit per meta-slot. */
      int64_t nchunks;  // starts at 28 bytes
      /* Number of chunks in super-chunk */
      int64_t nbytes;  // starts at 36 bytes
      /* data size + metadata size + header size (uncompressed) */
      int64_t cbytes;  // starts at 44 bytes
      /* data size + metadata size + header size (compressed) */
      uint8_t* filters_chunk;  // starts at 52 bytes
      /* Pointer to chunk hosting filter-related data */
      uint8_t* codec_chunk;
      /* Pointer to chunk hosting codec-related data */
      uint8_t* metadata_chunk;
      /* Pointer to schunk metadata */
      uint8_t* userdata_chunk;
      /* Pointer to user-defined data */
      uint8_t** data;
      /* Pointer to chunk data pointers */
      //uint8_t* ctx;
      /* Context for the thread holder.  NULL if not acquired. */
      blosc2_context* cctx;
      blosc2_context* dctx;
      /* Contexts for compression and decompression */
      uint8_t* reserved;
      /* Reserved for the future. */
    } blosc2_schunk;

    blosc2_schunk* blosc2_new_schunk(blosc2_cparams cparams, blosc2_dparams dparams);

    int blosc2_free_schunk(blosc2_schunk* sheader);

    size_t blosc2_append_buffer(blosc2_schunk* sheader, size_t nbytes, void* src);

    int blosc2_decompress_chunk(blosc2_schunk* sheader, size_t nchunk, void* dest, size_t nbytes);

    int blosc_get_blocksize(void);

    void blosc_set_blocksize(size_t blocksize);

    void blosc_set_schunk(blosc2_schunk* schunk);
    """
)

C = ffi.dlopen("blosc")


def blosc_init():
    return C.blosc_init()


def blosc_destroy():
    return C.blosc_destroy()


def blosc_compress(clevel, doshuffle, typesize, nbytes, src, dest, destsize):
    src = ffi.from_buffer(src)
    dest = ffi.from_buffer(dest)
    return C.blosc_compress(clevel, doshuffle, typesize, nbytes, src, dest, destsize)


def blosc_decompress(src, dest, destsize):
    src = ffi.from_buffer(src)
    dest = ffi.from_buffer(dest)
    return C.blosc_decompress(src, dest, destsize)


def blosc_getitem(src, start, nitems, dest):
    src = ffi.from_buffer(src)
    dest = ffi.from_buffer(dest)
    return C.blosc_getitem(src, start, nitems, dest)


def blosc_get_nthreads():
    return C.blosc_get_nthreads()


def blosc_set_nthreads(nthreads):
    return C.blosc_set_nthreads(nthreads)


def blosc_get_compressor():
    return ffi.string(C.blosc_get_compressor()).decode("utf-8")


def blosc_set_compressor(compname):
    if type(compname) == str:
        compname = ffi.new("char[]", compname.encode("utf-8"))
    else:
        compname = ffi.new("char[]", compname)
    return C.blosc_set_compressor(compname)


def blosc_set_delta(dodelta):
    return C.blosc_set_delta(dodelta)


def blosc_compcode_to_compname(compcode):
    compname = ffi.new("char **")
    return (C.blosc_compcode_to_compname(compcode, compname), ffi.string(compname[0]).decode("utf-8"))


def blosc_compname_to_compcode(compname):
    if type(compname) == str:
        compname = ffi.new("char[]", compname.encode("utf-8"))
    else:
        compname = ffi.new("char[]", compname)
    return C.blosc_compname_to_compcode(compname)


def blosc_list_compressors():
    return ffi.string(C.blosc_list_compressors()).decode('utf8')


def blosc_get_version_string():
    return ffi.string(C.blosc_get_version_string()).decode('utf8')


def blosc_get_complib_info(compname):
    if type(compname) == str:
        compname = ffi.new("char[]", compname.encode("utf-8"))
    else:
        compname = ffi.new("char[]", compname)
    complib = ffi.new("char **")
    version = ffi.new("char **")
    return (C.blosc_get_complib_info(compname, complib, version), ffi.string(complib[0]).decode("utf-8"),
            ffi.string(version[0]))


def blosc_free_resources():
    return C.blosc_free_resources()


def blosc_cbuffer_sizes(cbuffer):
    nbytes = ffi.new("size_t *")
    cbytes = ffi.new("size_t *")
    blocksize = ffi.new("size_t *")
    cbuffer = ffi.from_buffer(cbuffer)
    C.blosc_cbuffer_sizes(cbuffer, nbytes, cbytes, blocksize)
    return nbytes[0], cbytes[0], blocksize[0]


def blosc_cbuffer_metainfo(cbuffer):
    typesize = ffi.new("size_t *")
    flags = ffi.new("int *")
    cbuffer = ffi.from_buffer(cbuffer)
    C.blosc_cbuffer_metainfo(cbuffer, typesize, flags)
    return typesize[0], list((0, 1)[flags[0] >> j & 1] for j in range(3, -1, -1))


def blosc_cbuffer_versions(cbuffer):
    version = ffi.new("int *")
    versionlz = ffi.new("int *")
    cbuffer = ffi.from_buffer(cbuffer)
    C.blosc_cbuffer_versions(cbuffer, version, versionlz)
    return (version[0], versionlz[0])


def blosc_cbuffer_complib(cbuffer):
    cbuffer = ffi.from_buffer(cbuffer)
    return ffi.string(C.blosc_cbuffer_complib(cbuffer)).decode('utf8')


#####################
# STRUCTS FUNCTIONS #
#####################
def blosc2_create_cparams(compcode, clevel, use_dict, typesize, nthreads, blocksize, schunk, filters, filters_meta):
    cp = ffi.new("blosc2_cparams*")
    cp.compcode = compcode
    cp.clevel = clevel
    cp.use_dict = use_dict
    cp.typesize = typesize
    cp.nthreads = nthreads
    cp.blocksize = blocksize
    cp.schunk = ffi.NULL if schunk is None else schunk
    cp.filters = filters
    cp.filters_meta = filters_meta
    return cp[0]


def blosc2_create_dparams(nthreads, schunk):
    dp = ffi.new("blosc2_dparams*")
    dp.nthreads = nthreads
    dp.schunk = ffi.NULL if schunk is None else schunk
    return dp[0]


def blosc2_create_cctx(cparams):
    return C.blosc2_create_cctx(cparams)


def blosc2_create_dctx(dparams):
    return C.blosc2_create_dctx(dparams)


def blosc2_free_ctx(context):
    return C.blosc2_free_ctx(context)


def blosc2_compress_ctx(context, nbytes, src, dest, destsize):
    src = ffi.from_buffer(src)
    dest = ffi.from_buffer(dest)
    return C.blosc2_compress_ctx(context, nbytes, src, dest, destsize)


def blosc2_decompress_ctx(context, src, dest, destsize):
    src = ffi.from_buffer(src)
    dest = ffi.from_buffer(dest)
    return C.blosc2_decompress_ctx(context, src, dest, destsize)


def blosc2_getitem_ctx(context, src, start, nitems, dest):
    src = ffi.from_buffer(src)
    dest = ffi.from_buffer(dest)
    return C.blosc2_getitem_ctx(context, src, start, nitems, dest)


def blosc2_new_schunk(cparams, dparams):
    return C.blosc2_new_schunk(cparams, dparams)


def blosc2_free_schunk(schunk):
    return C.blosc2_free_schunk(schunk)


def blosc2_append_buffer(schunk, nbytes, src):
    src = ffi.from_buffer(src)
    return C.blosc2_append_buffer(schunk, nbytes, src)


def blosc2_decompress_chunk(schunk, nchunk, dest, nbytes):
    dest = ffi.from_buffer(dest)
    return C.blosc2_decompress_chunk(schunk, nchunk, dest, nbytes)


def blosc_get_blocksize():
    return C.blosc_get_blocksize()


def blosc_set_blocksize(blocksize):
    return C.blosc_set_blocksize(blocksize)


def blosc_set_schunk(schunk):
    return C.blosc_set_schunk(schunk)
