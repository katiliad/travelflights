import sys
import json
from amadeus import Client, ResponseError
from pyspark.sql import SparkSession
from pyspark import SparkConf, SparkContext

# conf = SparkConf().setMaster("local").setAppName("My App")
# sc = SparkContext(conf = conf)
raw_filename = "reponse_raw.json"
processed_filename = "response_processed.json"

def writeRawResponseToFile(response):
    with open("../output/"+raw_filename, 'w') as f:
        json.dump(response.data, f)

def downloadData(from_field, to_field, date):
    amadeus = Client(
        client_id='xx',
        client_secret='xx'
    )

    try:
        response = amadeus.shopping.flight_offers_search.get(
              originLocationCode=from_field, destinationLocationCode=to_field, departureDate=date, adults=1
        )
        writeRawResponseToFile(response)
    except ResponseError as error:
        raise error

def runSparkOnRawFile():

    if __name__ == "__main__":
        spark = SparkSession.builder.master("local[*]").appName("Read corrupted json").getOrCreate()

        dfFromTxt=spark.read.text(raw_filename)
        dfFromTxt.show(truncate=False)

        from pyspark.sql.functions import regexp_replace
        dfFromTxt = dfFromTxt.withColumn('value', regexp_replace('value', 'False', '"FALSE"'))
        dfFromTxt.count()

        dfFromTxt = dfFromTxt.withColumn('value', regexp_replace('value', '\},\ \{', '\}__SPLIT_POINT__\{'))
        dfFromTxt.show(truncate=False)

        from pyspark.sql.types import StructType,StructField,IntegerType,StringType,DoubleType,BooleanType

        schema = StructType([
            StructField("Brand",StringType(),True),
            StructField("Days", IntegerType(), True),
            StructField("Model", StringType(), True),
            StructField("Rate", DoubleType(), True),
            StructField("Available", StringType(), True)
        ])

        from pyspark.sql.functions import col,from_json
        dfJSON = dfFromTxt.withColumn("jsonData",from_json(col("value"),schema)).select("jsonData.*").dropna()
        dfJSON.show()


        from pyspark.sql.functions import split, explode
        dfFromTxt = dfFromTxt.withColumn('value',explode(split('value','__SPLIT_POINT__')))
        dfFromTxt.count()

        dfFromTxt.show(truncate=False)


        dfJSON = dfFromTxt.withColumn("jsonData",from_json(col("value"),schema)).select("jsonData.*").dropna()
        dfJSON.show()


        dfFromTxt = dfFromTxt.withColumn('value', regexp_replace('value', '\[', '')).withColumn('value', regexp_replace('value', '\]', ''))
        dfFromTxt.show(truncate=False)

        dfJSON = dfFromTxt.withColumn("jsonData",from_json(col("value"),schema)).select("jsonData.*").dropna()
        dfJSON.show()

        dfJSON.write.format('json').mode('overwrite').save("../output/"+processed_filename)

def main():
    from_field = sys.argv[1]
    to_field = sys.argv[2]
    date = sys.argv[3]
    print("Python we are in main!")
    print("Python " + to_field)
    downloadData(from_field, to_field, date)
    runSparkOnRawFile()

main()