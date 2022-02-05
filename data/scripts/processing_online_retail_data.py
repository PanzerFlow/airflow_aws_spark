import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def OnlineRetailDataProcessor(source_path, output_path):
  """
  Simple function for demonstration purposes.
  :param data_source: S3 KEY
  :param output_uri: S3 KEY
  """
  # Load the CSV data
  if source_path is not None:
      Input_DF = (spark.read
                .option("inferSchema",True)
                .option("header",True)
                .option("sep",",")
                .csv(source_path)
                )
  # Process the data
  Processed_DF = (Input_DF
              .groupBy("Description")
              .agg(F.sum("Quantity").alias("TotalQuantity")
              ,F.count("_c0").alias("InvoiceCount")
              ,F.avg("UnitPrice").alias("UnitPrice"))
              )
  
  # Write the results to the specified output path
  Processed_DF.write.mode("overwrite").parquet(output_path)
  

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", type=str, help="HDFS input", default="/movie")
    parser.add_argument("--output", type=str, help="HDFS output", default="/output")
    args = parser.parse_args()
    spark = SparkSession.builder.appName("Random Text Classifier").getOrCreate()
    OnlineRetailDataProcessor(source_path=args.input, output_path=args.output)