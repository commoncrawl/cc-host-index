"""read JSON schema and convert to Athena SQL schema, output to stdout"""
import argparse
import json
from string import Template

# all types are mapped even if there is no change, for simplicity
type_map = {
    "integer": "int",
    "double": "double",
    "string": "string"
}

schema_template_string = """
CREATE EXTERNAL TABLE ${table_name} (
  ${table_columns}
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
  '${table_location}'
;
"""
template = Template(schema_template_string)

# defaults left to match original schema
# TODO: reconsider defaults and remove if necessary
parser = argparse.ArgumentParser()
parser.add_argument('json_file', help='path to schema in JSON format')
parser.add_argument('-n', '--name', help='name of table created by schema', default='cchost_index_testing_v2')
parser.add_argument('-l', '--location', help='location of table created by schema', default='s3://commoncrawl/projects/host-index-testing/v2/')
args = parser.parse_args()

with open(args.json_file) as f:
    json_schema = json.load(f)
# json to sql type conversion is hard-coded dict
cols_list = [f"{entry['name'].strip()} {type_map[entry['type']].strip()}" for entry in json_schema['fields']]
columns = ',\n  '.join(cols_list)

athena_schema = template.substitute(
    {'table_name': args.name,
     'table_location': args.location,
     'table_columns': columns})

print(athena_schema)

