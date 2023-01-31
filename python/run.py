import sys
import os
import json
from amadeus import Client, ResponseError
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, min, max, first, col, count, expr, concat

raw_filename = "reponse_raw.json"

def writeRawResponseToFile(response):
    with open("../input_file/"+raw_filename, 'w') as f:
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

def runSpark():

    spark = SparkSession.builder.master("local[*]").appName("Streaming corrupted json").getOrCreate()
    spark.sparkContext.setLogLevel("OFF")
    raw_filename = "reponse_raw.json"
    output_folder = "output_file"
    rawDF=spark.read.json("../input_file/"+raw_filename, multiLine="true")
    # rawDF.printSchema()

    sampleDF = rawDF.withColumnRenamed("id", "unique_travel_id")

    priceDF = sampleDF.select("unique_travel_id", concat("price.grandTotal","price.currency").alias("Price"))
    # priceDF.show()


    batDF = sampleDF.select("unique_travel_id", "itineraries")
    # batDF.printSchema()
    # batDF.show(1, False)
    bat2DF = batDF.select("unique_travel_id", explode("itineraries").alias("new_itineraries"))
    # bat2DF.printSchema()
    # bat2DF.show()

    bat3DF =  bat2DF.select("unique_travel_id", "new_itineraries.segments")
    # bat3DF.printSchema()
    # bat3DF.show()

    bat4DF = bat3DF.select("unique_travel_id", explode("segments").alias("exploded_segments"))
    # bat4DF.printSchema()
    # bat4DF.show()

    bat5DF = bat4DF.select("unique_travel_id", col("exploded_segments.operating.carrierCode").alias("IATA Code"), col("exploded_segments.departure.at").alias("departure"), col("exploded_segments.arrival.at").alias("arrival"))
    # bat5DF.show()

    bat6DF = bat5DF.groupBy("unique_travel_id").agg(
        min("departure").alias("departure"),
        max("arrival").alias("arrival"),
        first("IATA Code").alias("IATA Code"),
        count('*').alias('Stops')
    ).dropDuplicates(["unique_travel_id"])

    bat6DF = bat6DF.withColumn("Stops", expr("Stops - 1"))
    # bat6DF.show()

    result_df = bat6DF.join(priceDF, on="unique_travel_id", how="inner")
    # result_df.show(150)

    result_without_id_df = result_df.drop(col("unique_travel_id"))
    # result_without_id_df.show(150)
    result_without_id_df.write.format('json').mode('overwrite').save("../"+output_folder)


def main():
    from_field = sys.argv[1]
    to_field = sys.argv[2]
    date = sys.argv[3]
    downloadData(from_field, to_field, date)
    runSpark()

main()