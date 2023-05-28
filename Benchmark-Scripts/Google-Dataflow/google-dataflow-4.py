# benchmark 4 - data cleaning
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import pyarrow as pa

options = PipelineOptions(
    runner='DataflowRunner',
    project='cos20015-hd-research',
    temp_location='gs://dataflow__bucket/temp',
    region='us-central1', 
)

fields = [
    pa.field('event_time', pa.string()),
    pa.field('event_type', pa.string()),
    pa.field('product_id', pa.int64()),
    pa.field('category_id', pa.int64()),
    pa.field('category_code', pa.string()),
    pa.field('brand', pa.string()),
    pa.field('price', pa.float64()),
    pa.field('user_id', pa.int64()),
    pa.field('user_session', pa.string()),
]
schema = pa.schema(fields)

field_names = [field.name for field in fields]

def filter_positive_price(element):
    return element['price'] > 0

p = beam.Pipeline(options=options)

cleaned = (
    p
    | "Read from GCS" >> beam.io.ReadFromParquet('gs://dataflow__bucket/2019-Oct.parquet/')
    | "Remove nulls 1" >> beam.Filter(lambda row: all(val is not None for val in row.values()))
    | "Filter positive price" >> beam.Filter(filter_positive_price)
    
    | "Convert to tuple" >> beam.Map(lambda row: tuple(row.items()))
    | "Drop duplicates" >> beam.Distinct()
    | "Convert to dict" >> beam.Map(lambda row: dict(row))

    | "Write to GCS" >> beam.io.WriteToParquet('gs://dataflow__bucket/2019-Oct-cleaned.parquet/', schema=schema)
)


result = p.run()
result.wait_until_finish()