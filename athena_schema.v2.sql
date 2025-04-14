CREATE EXTERNAL TABLE cchost_index_testing_v2 (
  surt_host_name string,
  url_host_registered_domain string,
  url_host_tld string,
  hcrank10 double,
  prank10 double,
  prank double,
  hcrank double,
  warc_record_length_av int,
  warc_record_length_median int,
  fetch_200 int,
  fetch_200_lote int,
  fetch_200_lote_pct int,
  fetch_3xx int,
  fetch_4xx int,
  fetch_5xx int,
  fetch_gone int,
  fetch_notModified int,
  fetch_other int,
  fetch_redirPerm int,
  fetch_redirTemp int,
  nutch_fetched int,
  nutch_fetched_pct int,
  nutch_gone int,
  nutch_gone_pct int,
  nutch_notModified int,
  nutch_notModified_pct int,
  nutch_numRecords int,
  nutch_redirPerm int,
  nutch_redirPerm_pct int,
  nutch_redirTemp int,
  nutch_redirTemp_pct int,
  nutch_unfetched int,
  nutch_unfetched_pct int,
  robots_200 int,
  robots_3xx int,
  robots_4xx int,
  robots_5xx int,
  robots_gone int,
  robots_notModified int,
  robots_other int,
  robots_redirPerm int,
  robots_redirTemp int
)
PARTITIONED BY (
  `crawl` string
)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  's3://commoncrawl/projects/host-index-testing/v2/'
;
