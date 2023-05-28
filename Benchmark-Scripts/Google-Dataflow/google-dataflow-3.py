# benchmark 3 - data exploration
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions


options = PipelineOptions(
    runner='DataflowRunner',
    project='cos20015-hd-research',
    temp_location='gs://dataflow__bucket/temp',
    region='us-central1', 
)

p = beam.Pipeline(options=options)

parquet = (
    p 
    | 'ReadParquet' >> beam.io.ReadFromParquet('gs://dataflow__bucket/2019-Oct.parquet/')
)

# Calculate distinct user_id and product_id
unique_user_ids = (
    parquet
    | 'ExtractUserId' >> beam.Map(lambda x: x['user_id']) 
    | 'DistinctUsers' >> beam.Distinct()
)

unique_product_ids = (
    parquet 
    | 'ExtractProductId' >> beam.Map(lambda x: x['product_id']) 
    | 'DistinctProducts' >> beam.Distinct()
)

# Find the most viewed and purchased product
most_viewed_product = (
    parquet  
    | 'FilterViewEvents' >> beam.Filter(lambda x: x['event_type'] == 'view')
    | 'ExtractProductIdForView' >> beam.Map(lambda x: x['product_id'])
    | 'CountPerProductView' >> beam.combiners.Count.PerElement()
    | 'MostViewedProduct' >> beam.transforms.combiners.Top.Of(1, key=lambda x: x[1])
)

most_purchased_product = (
    parquet 
    | 'FilterPurchaseEvents' >> beam.Filter(lambda x: x['event_type'] == 'purchase') 
    | 'ExtractProductIdForPurchase' >> beam.Map(lambda x: x['product_id'])
    | 'CountPerProductPurchase' >> beam.combiners.Count.PerElement()
    | 'MostPurchasedProduct' >> beam.transforms.combiners.Top.Of(1, key=lambda x: x[1])
)


# Find total revenue
total_revenue = (
    parquet 
    | 'FilterPurchaseForRevenue' >> beam.Filter(lambda x: x['event_type'] == 'purchase') 
    | 'ExtractPrice' >> beam.Map(lambda x: x['price'])
    | 'SumPrice' >> beam.CombineGlobally(sum).without_defaults()
)


# Count the number of events per type
events_per_type = (
    parquet 
    | 'ExtractEventType' >> beam.Map(lambda x: x['event_type'])
    | 'CountPerEventType' >> beam.combiners.Count.PerElement()
)

# Find the most popular category
popular_category = (
    parquet
    | 'ExtractCategory' >> beam.Map(lambda x: x['category_code'])
    | 'CountPerCategory' >> beam.combiners.Count.PerElement()
    | 'MostPopularCategory' >> beam.transforms.combiners.Top.Of(1, key=lambda x: x[1])
)

result = p.run()
result.wait_until_finish()