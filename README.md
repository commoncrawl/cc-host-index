# cc-host-index

This repo contains examples using Common Crawl's host index. The host
index for a crawl is a table which contains a single row for each
web host, combining aggregated information from the columnar index,
the web graph, and our raw crawler logs.

These examples are expressed in SQL syntax, and we will run them
in duckdb and AWS Athena.

## Example questions

- How well are we crawling a particular website?
- How well are we crawling groups of websites?
- What popular websites have a lot of non-English content
- What popular websites appear to need javascript to find content

## Highlights of the schema

- the primary key is `surt_host_name`, which is quirky (commoncrawl.org -> org,commoncrawl)
- there is also `url_host_tld`, which we recommend that you use whenever possible ("org" for commoncrawl.org)
- there is one row for every webhost in the web graph, even if we didn't crawl it this month
- there are numbers for what we stored in our archive (fetch_*)
- there are raw numbers from our crawler logs, which also reveal the crawl budget and if we exhausted it (nutch_*)
- there is a language summary (for now, just the count of languages other than English (LOTE))
- there is a size summary (average and median compressed size)
- for a subset of the numbers, there is a `foo_pct`, which can help you avoid doing math in SQL
- the full schema is at `athena_schema.v2.sql`

## Setup

The host index can either be used in place, or you can download it
and use it from local disk. It is about 7 gigabytes per crawl, and
the most recent 24 crawls are currently indexed (testing-v2).

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

```
MSCK REPAIR TABLE host_index_testing_v2
```

Now check that it's working:

```
SELECT COUNT(*) FROM cchost_index_testing_v2
```

## Python code examples

As a quirk of v2 that will eventually be fixed, the inputs to these
example programs need to be surt_host_names -- e.g. org,commoncrawl
for commoncrawl.org, or org,commoncrawl, for *.commoncrawl.org.

- host.py -- generates a bunch of pngs, csvs, and a webpage summarizing a host or wildcarded host
- count-hosts.py -- given a file containing a bunch of surt hostnames, plot fetch\_200 over time

## Example SQL queries

Host fetches, non-English, and ranking.

```
SELECT
  crawl, fetch_200, fetch_200_lote, prank10, hcrank10
FROM host_index
WHERE surt_host_name = 'org,commoncrawl'
ORDER BY crawl ASC
```

Host fetches of all kinds. This includes captures that you
will find in the crawldiagnostics warcs.

```
SELECT
  crawl, fetch_200, fetch_gone, fetch_redirPerm, fetch_redirTemp, fetch_notModified, fetch_3xx, fetch_4xx, fetch_5xx, fetch_other
FROM host_index
WHERE surt_host_name = 'org,commoncrawl'
ORDER BY crawl ASC
```

Per-host page size stats, average and median.

```
SELECT
  crawl, warc_record_length_av, warc_record_length_median
FROM host_index
WHERE surt_host_name = 'org,commoncrawl'
ORDER BY crawl ASC
```

Raw crawler logs counts. `nutch_numrecords` gives an idea of what the crawl budget was,
and if it was exhausted.

```
SELECT
  crawl, nutch_numRecords, nutch_fetched, nutch_unfetched, nutch_gone, nutch_redirTemp, nutch_redirPerm, nutch_notModified
FROM host_index
WHERE surt_host_name = 'org,commoncrawl'
ORDER BY crawl ASC
```

Robots.txt fetch details.

```
SELECT
  crawl, robots_200, robots_gone, robots_redirPerm, robots_redirTemp, robots_notModified, robots_3xx, robots_4xx, robots_5xx, robots_other
FROM host_index
WHERE surt_host_name = 'org,commoncrawl'
ORDER BY crawl ASC
```

## Known bugs

- When the S3 bucket is under heavy use, AWS Athena will sometimes throw 503 errors. We have yet to figure out how to increase the retry limit.
- duckdb's https retries don't seem to work as advertised, so duckdb talking to AWS is also affected during periods of heavy usage.
- Hint: https://status.commoncrawl.org/ has graphs of S3 performance for the last day, week, and month.

## Expected changes in test v3

- warc\_record\_length\_av will be renamed to \_avg (that was a typo)
- more _pct columns
- addition of indegree and outdegree for all hosts from the web graph
- add unicode block information, similar to languages
- improve language details to be more than only LOTE and LOTE_pct
- prank10 needs its power law touched up (hcrank10 is much better)
- there's a sort problem that .com shards have a smattering of not-.com hosts. This hurts performance.
- CI running against S3

## Contributing

We'd love to get code contributions! Here are some clues:

- We'd love to have python examples using Athena, similar to duckdb
- We'd love to have more python examples
- Please use pyarrow whenever possible
