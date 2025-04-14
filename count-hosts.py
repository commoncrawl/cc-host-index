import sys
import time

from surt import surt
import duckdb

import duck_utils

surts = []

duck_utils.init_duckdb_httpfs()
host_index = duck_utils.open_host_index()

for fname in sys.argv[1:]:
    with open(fname, 'r') as fd:
        for line in fd:
            surt_host_name, extra = surt(line).split(')/', 1)
            if extra:
                print('skipping because extra is', repr(extra))
                # discard everything that is a subset of a web host
                continue
            surts.append(surt_host_name)
print('read surts:')
print(surts)
title = sys.argv[1]

host_sql = '''
SELECT
  crawl,
  CAST(SUM(fetch_200) AS INT64) AS sum_fetch_200
FROM host_index
WHERE contains(ARRAY [{surt_list}], surt_host_name)
AND url_host_tld = 'gov'
GROUP BY crawl
ORDER BY crawl ASC
'''

surt_list = ','.join(f"'{s}'" for s in surts)
sql = host_sql.format(surt_list=surt_list)

start = time.time()
print('start query')
res = duckdb.sql(sql)
print('print fetchall', round(time.time()-start, 3))
print('to df', round(time.time()-start, 3))
df = res.df()
print('print df', round(time.time()-start, 3))
print(df)
print('plot', round(time.time()-start, 3))

lines = [
    ['crawl', 'sum_fetch_200', 1.0, 'o', 'fetch_200'],
]
print('end', round(time.time()-start, 3))
