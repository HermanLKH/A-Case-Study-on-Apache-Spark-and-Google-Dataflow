# benchmark 1 - load data
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

options = PipelineOptions(
    runner='DataflowRunner',
    project='cos20015-hd-research',
    temp_location='gs://dataflow__bucket/temp',
    region='us-central1', 
)

p = beam.Pipeline(options=options)

(
    p
    | "Read from GCS" >> beam.io.ReadFromCsv('gs://dataflow__bucket/2019-Oct.csv')
)

result = p.run()
result.wait_until_finish()