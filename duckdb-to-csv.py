import duckdb
import pyarrow.csv as csv

import duck_utils


fname = 'out.csv'
sql = '''
SELECT
  crawl, surt_host_name, hcrank10, fetch_200_lote_pct, fetch_200_lote
FROM host_index
WHERE crawl = 'CC-MAIN-2025-13'
  AND fetch_200_lote_pct > 90
ORDER BY hcrank10 DESC
LIMIT 10
'''

host_index = duck_utils.open_host_index()
table = duckdb.sql(sql).fetch_arrow_table()
with open(fname, 'wb') as fd:
    csv.write_csv(table, fd)
