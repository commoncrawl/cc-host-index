# cc-host-index

This repo contains examples using Common Crawl's host index. The host
index for a crawl is a table which contains a single row for each
web host, combining aggregated information from the columnar index,
the web graph, and our raw crawler logs.

*This document discusses the testing v2 version of this dataset -- it
will change before its final release.*

## Example questions this index can answer

- What's our history of crawling a particular website, or group of websites?
- What popular websites have a lot of non-English content?
- What popular websites seem to have so little content that we might need to execute javascript to crawl them?

## Example questions that we'll use to improve our crawl

- What's the full list of websites where more than half of the webpages are primarily not English?
- What popular websites end our crawls with most of their crawl budget left uncrawled?

## Example questions that future versions of this host index can answer

- What websites have a lot of content in particular languages?
- What websites have a lot of content with particular Unicode scripts?

## Highlights of the schema

- there is a Hive-style partition on `crawl`, which is a crawl name like `CC-MAIN-2025-13`.
- there is one row for every webhost in the web graph, even if we didn't crawl that website in that particular crawl
- the primary key is `surt_host_name`, which is quirky (commoncrawl.org -> org,commoncrawl)
- there is also `url_host_tld`, which we recommend that you use whenever possible ("org" for commoncrawl.org)
- there are counts of what we stored in our archive (warc, crawldiagnostics, robots)
 - `fetch_200, fetch_3xx, fetch_4xx, fetch_5xx, fetch_gone, fetch_notModified, fetch_other, fetch_redirPerm, fetch_redirTemp`
 - `robots_200, robots_3xx, robots_4xx, robots_5xx, robots_gone, robots_notModified, robots_other, robots_redirPerm, robots_redirTemp`
- there is ranking information from the web graph: harmonic centrality, page rank, and both normalized to a 0-10 scale
  - `hcrank`, `prank`, `hcrank10`, `prank10`
- there is a language summary (for now, just the count of languages other than English (LOTE))
  - `fetch_200_lote, fetch_200_lote_pct`
- for a subset of the numbers, there is a `foo_pct`, which can help you avoid doing math in SQL. It is an integer 0-100.
- there are raw numbers from our crawler logs, which also reveal the crawl budget and if we exhausted it
  - `nutch_fetched, nutch_gone, nutch_notModified, nutch_numRecords, nutch_redirPerm, nutch_redirTemp, nutch_unfetched`
  - `nutch_fetched_pct, nutch_gone_pct, nutch_notModified_pct, nutch_redirPerm_pct, nutch_redirTemp_pct, nutch_unfetched_pct`
- there is a size summary (average and median size (compressed))
  - `warc_record_length_median, warc_record_length_av` (will be renamed to _avg in v3)
- the full schema is at `athena_schema.v2.sql`

## Examples

US Federal government websites in the *.gov domain (about 1,400 domains, y-axis scale is millions):

![current-federal.txt_sum.png](https://commoncrawl.github.io/cc-host-index-media/current-federal.txt_sum.png)

[See all graphs from this dataset](https://commoncrawl.github.io/cc-host-index-media/current-federal.txt.html)

commoncrawl.org fetch. You can see that we revamped our website in CC-2023-14, which caused a lot of
permanent redirects to be crawled in the next few crawls:

![commoncrawl.org_fetch.png](https://commoncrawl.github.io/cc-host-index-media/commoncrawl.org_fetch.png)

[See all graphs from this dataset](https://commoncrawl.github.io/cc-host-index-media/commoncrawl.org.html)

## Setup

The host index can either be used in place at AWS, or you can download
it and use it from local disk. The size is about 7 gigabytes per
crawl, and the most recent 24 crawls are currently indexed
(testing-v2).

### Setup -- local development environment

```
pip install -r requirements.txt
```

### Setup -- duckdb from outside AWS

```
wget https://data.commoncrawl.org/projects/host-index-testing/v2.paths.gz
export HOST_INDEX=v2.paths.gz
```

### Setup -- duckdb from inside AWS -- us-east-1

```
aws s3 cp s3://commoncrawl/projects/host-index-testing/v2.paths.gz .
export HOST_INDEX=v2.paths.gz
export HOST_INDEX_BUCKET=s3://commoncrawl/
```

### Setup -- duckdb with local files

Install [cc-downloader](https://github.com/commoncrawl/cc-downloader/)

Then, NOT TESTED!

```
wget https://data.commoncrawl.org/projects/host-index-testing/v2.paths.gz
cc-downloader download v2.paths.gz
```

Then, wherever you move the downloaded files, point at the top directory:

```
export HOST_INDEX=/home/cc-pds/commoncrawl/projects/host-index-testing/v2/
```

### Setup -- AWS Athena -- us-east-1

```
CREATE DATABASE cchost_index_testing_v2
```

Select the new database in the GUI.

Paste the contents of `athena_schema.v2.sql` into a query and run it.

Then,

```
MSCK REPAIR TABLE host_index_testing_v2
```

Now check that it's working:

```
SELECT COUNT(*) FROM cchost_index_testing_v2
```

## Python code examples

The included script `graph.py` knows how to make csvs, png images, and webpages containing
these images. It runs in 3 styles:

- one web host: `python ./graph.py example.com`
- wildcared subdomains: `python ./graph.py *.example.com`
- a list of hosts: `python ./graph.py -f list_of_hosts.txt`

Yes, these commands take a while to run.

This repo also has `duckdb-to-csv.py`, which you can use
to run a single SQL command and get csv output.

## Example SQL queries

Host fetches, non-English count, and ranking. Note the use of `url_host_tld` ... that is recommended
to make the SQL query optimizer's life easier.

```
SELECT
  crawl, fetch_200, fetch_200_lote, prank10, hcrank10
FROM cchost_index_testing_v2
WHERE surt_host_name = 'org,commoncrawl'
  AND url_host_tld = 'org'
ORDER BY crawl ASC
```

Counts of web captures. This includes captures that you
will find in the crawldiagnostics warcs.

```
SELECT
  crawl, fetch_200, fetch_gone, fetch_redirPerm, fetch_redirTemp, fetch_notModified, fetch_3xx, fetch_4xx, fetch_5xx, fetch_other
FROM cchost_index_testing_v2
WHERE surt_host_name = 'org,commoncrawl'
  AND url_host_tld = 'org'
ORDER BY crawl ASC
```

Per-host page size stats, average and median.

```
SELECT
  crawl, warc_record_length_av, warc_record_length_median
FROM cchost_index_testing_v2
WHERE surt_host_name = 'org,commoncrawl'
  AND url_host_tld = 'org'
ORDER BY crawl ASC
```

Raw crawler logs counts. `nutch_numrecords` gives an idea of what the crawl budget was,
and if it was exhausted.

```
SELECT
  crawl, nutch_numRecords, nutch_fetched, nutch_unfetched, nutch_gone, nutch_redirTemp, nutch_redirPerm, nutch_notModified
FROM cchost_index_testing_v2
WHERE surt_host_name = 'org,commoncrawl'
  AND url_host_tld = 'org'
ORDER BY crawl ASC
```

Robots.txt fetch details.

```
SELECT
  crawl, robots_200, robots_gone, robots_redirPerm, robots_redirTemp, robots_notModified, robots_3xx, robots_4xx, robots_5xx, robots_other
FROM cchost_index_testing_v2
WHERE surt_host_name = 'org,commoncrawl'
  AND url_host_tld = 'org'
ORDER BY crawl ASC
```

Top 10 Vatican websites from crawl CC-MAIN-2025-13 that are > 50% languages other than English (LOTE).

```
SELECT
  crawl, surt_host_name, hcrank10, fetch_200_lote_pct, fetch_200_lote
FROM cchost_index_testing_v2
WHERE crawl = 'CC-MAIN-2025-13'
  AND url_host_tld = 'va'
  AND fetch_200_lote_pct > 50
ORDER BY hcrank10 DESC
LIMIT 10
```

| # | crawl | surt\_host\_name | hcrank10 | fetch\_200\_lote\_pct | fetch\_200\_lote |
| --- |  --- |  --- |  --- |  --- |  --- |
| 1 | CC-MAIN-2025-13 | va,vaticannews | 5.472 | 89 | 18872 |
| 2 | CC-MAIN-2025-13 | va,vatican | 5.164 | 73 | 14549 |
| 3 | CC-MAIN-2025-13 | va,museivaticani | 4.826 | 77 | 568 |
| 4 | CC-MAIN-2025-13 | va,vatican,press | 4.821 | 67 | 3804 |
| 5 | CC-MAIN-2025-13 | va,clerus | 4.813 | 79 | 68 |
| 6 | CC-MAIN-2025-13 | va,osservatoreromano | 4.783 | 98 | 3305 |
| 7 | CC-MAIN-2025-13 | va,vaticanstate | 4.738 | 73 | 509 |
| 8 | CC-MAIN-2025-13 | va,migrants-refugees | 4.732 | 67 | 2055 |
| 9 | CC-MAIN-2025-13 | va,iubilaeum2025 | 4.73 | 85 | 672 |
| 10 | CC-MAIN-2025-13 | va,cultura | 4.724 | 67 | 80 |

Top 10 websites from crawl CC-MAIN-2025-13 that are > 90% languages other than English (LOTE).

```
SELECT
  crawl, surt_host_name, hcrank10, fetch_200_lote_pct, fetch_200_lote
FROM cchost_index_testing_v2
WHERE crawl = 'CC-MAIN-2025-13'
  AND fetch_200_lote_pct > 90
ORDER BY hcrank10 DESC
LIMIT 10
```

| # | crawl | surt\_host\_name | hcrank10 | fetch\_200\_lote\_pct | fetch\_200\_lote |
| --- |  --- |  --- |  --- |  --- |  --- |
| 1 | CC-MAIN-2025-13 | org,wikipedia,fr | 5.885 | 99 | 55334 |
| 2 | CC-MAIN-2025-13 | org,wikipedia,es | 5.631 | 100 | 48527 |
| 3 | CC-MAIN-2025-13 | com,chrome,developer | 5.62 | 92 | 29298 |
| 4 | CC-MAIN-2025-13 | ar,gob,argentina | 5.613 | 95 | 16580 |
| 5 | CC-MAIN-2025-13 | fr,ebay | 5.579 | 100 | 24633 |
| 6 | CC-MAIN-2025-13 | org,wikipedia,ja | 5.55 | 100 | 49008 |
| 7 | CC-MAIN-2025-13 | ru,gosuslugi | 5.535 | 100 | 1560 |
| 8 | CC-MAIN-2025-13 | org,wikipedia,de | 5.508 | 100 | 48223 |
| 9 | CC-MAIN-2025-13 | com,acidholic | 5.477 | 100 | 356 |
| 10 | CC-MAIN-2025-13 | ph,telegra | 5.455 | 92 | 57153 |

## Known bugs

- When the S3 bucket is under heavy use, AWS Athena will sometimes throw 503 errors. We have yet to figure out how to increase the retry limit.
- duckdb's https retries don't seem to work as advertised, so duckdb talking to AWS is also affected during periods of heavy usage.
- Hint: https://status.commoncrawl.org/ has graphs of S3 performance for the last day, week, and month.
- The sort order is a bit messed up, so database queries take more time than they should.

## Expected changes in test v3

- `warc_record_length_av` will be renamed to `_avg` (that was a typo)
- more `_pct` columns
- addition of indegree and outdegree for all hosts from the web graph
- add unicode block information, similar to languages
- improve language details to be more than only LOTE and LOTE\_pct
- prank10 needs its power law touched up (hcrank10 might change too)
- there's a sort problem that .com shards have a smattering of not-.com hosts. This hurts performance.
- add domain prank/hcrank
- CI running against S3

## Contributing

We'd love to get code contributions! Here are some clues:

- We'd love to have python examples using Athena, similar to duckdb
- We'd love to have more python examples
- Please use pyarrow whenever possible
