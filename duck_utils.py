import sys
import os
import os.path
import glob
import gzip

import duckdb


def init_duckdb_httpfs(verbose=0):
    # default is 3, 10 should be 1024 * wait
    # I used to have 1000 retries and that definitely wasn't happening
    duckdb.sql('SET http_retries = 10')

    duckdb.sql('SET http_retry_wait_ms = 2000')

    # this defaults off, but seems like a good idea for httpfs
    duckdb.sql('SET enable_object_cache = true')

    if verbose > 1:
        duckdb.sql('SET enable_http_logging = true')
        print('writing duckdb_http log to duckdb_http.log', file=sys.stderr)
        duckdb.sql("SET http_logging_output = './duckdb_http.log'")


def expand_paths(paths, bucket, verbose=0):
    if not paths:
        if 'HOST_INDEX' in os.environ:
            paths = os.environ['HOST_INDEX']
    if not paths:
        raise ValueError('do not know where the parquet files are')
    if not isinstance(paths, str):
        raise ValueError('paths needs to be a string: '+repr(paths))

    if os.path.isfile(paths):
        if paths.endswith('.gz'):
            if verbose:
                print('paths from file.gz', paths)
            paths = gzip.open(paths, mode='rt').readlines()
        else:
            if verbose:
                print('paths from file', paths)
            paths = open(paths).readlines()
        # readlines leaves the \n on the end
        paths = [(bucket.rstrip() + '/' + p.rstrip()) for p in paths]
        return paths
    if os.path.isdir(paths):
        if verbose:
            print('paths is a directory', paths)
        # discards the passed in bucket
        bucket = paths.rstrip('/')
        return glob.glob(bucket + '/*.parquet') + glob.glob(bucket + '/**/*.parquet')
    if '*' in paths:
        if verbose:
            print('paths is a glob', paths)
        # discards the passed in bucket
        return glob.glob(paths)

    raise ValueError('do not know how to interpret paths='+paths)


def open_host_index(paths=None, bucket='https://data.commoncrawl.org', grep=None, verbose=0):
    bucket = os.environ.get('HOST_INDEX_BUCKET', bucket).rstrip('/')

    paths = expand_paths(paths, bucket, verbose=verbose)
    if grep:
        paths = [p for p in paths if grep in p]

    if len(paths) < 1:
        print(paths)
        raise ValueError('no parquet files found')
    else:
        if verbose:
            print(f'{len(paths)} paths found')
    return duckdb.read_parquet(paths, hive_partitioning=True)
