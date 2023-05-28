# benchmark 2 - convert data (csv-> parquet)
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

class ConvertToDictFn(beam.DoFn):
    def __init__(self, field_names):
        self.field_names = field_names

    def process(self, element):
        return [{self.field_names[i]: value for i, value in enumerate(element)}]

field_names = [field.name for field in fields]

p = beam.Pipeline(options=options)

(
    p
    | "Read from GCS" >> beam.io.ReadFromCsv('gs://dataflow__bucket/2019-Oct.csv')
    | "Tuple to Dict" >> beam.ParDo(ConvertToDictFn(field_names))
    | "Write to GCS" >> beam.io.WriteToParquet('gs://dataflow__bucket/2019-Oct.parquet/', schema=schema)
)

result = p.run()
result.wait_until_finish()