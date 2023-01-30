from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[*]").appName("Streaming corrupted json").getOrCreate()

#dfFromTxt=spark.read.text("corrupted.json")
#dfFromTxt.show(truncate=False)

# ------------------------------------------------------
from pyspark.sql.functions import explode, min, max, first, col, count, expr, concat

raw_filename = "reponse_raw.json"
processed_filename = "output_jsn.json"
rawDF=spark.read.json(raw_filename, multiLine="true")
#rawDF.printSchema()

sampleDF = rawDF.withColumnRenamed("id", "unique_travel_id")

priceDF = sampleDF.select("unique_travel_id", concat("price.grandTotal","price.currency").alias("Price"))
#priceDF.show()


batDF = sampleDF.select("unique_travel_id", "itineraries")
#batDF.printSchema()
#batDF.show(1, False)
bat2DF = batDF.select("unique_travel_id", explode("itineraries").alias("new_itineraries"))
#bat2DF.printSchema()
#bat2DF.show()

bat3DF =  bat2DF.select("unique_travel_id", "new_itineraries.segments")
#bat3DF.printSchema()
#bat3DF.show()

bat4DF = bat3DF.select("unique_travel_id", explode("segments").alias("exploded_segments"))
#bat4DF.printSchema()
#bat4DF.show()

bat5DF = bat4DF.select("unique_travel_id", col("exploded_segments.operating.carrierCode").alias("IATA Code"), col("exploded_segments.departure.at").alias("departure"), col("exploded_segments.arrival.at").alias("arrival"))
#bat5DF.show()

bat6DF = bat5DF.groupBy("unique_travel_id").agg(
    min("departure").alias("Departure"),
    max("arrival").alias("Arrival"),
    first("IATA Code").alias("IATA Code"),
	count('*').alias('Stops')
).dropDuplicates(["unique_travel_id"])

bat6DF = bat6DF.withColumn("Stops", expr("Stops - 1"))
#bat6DF.show()

result_df = bat6DF.join(priceDF, on="unique_travel_id", how="inner")
#result_df.show(150)

result_without_id_df = result_df.drop(col("unique_travel_id"))
result_without_id_df.show(150)

#Get the size of a column to create anotehr column
#bat3DF.withColumn("segments_length",size(col("segments"))).show(False)
#bat3DF.selectExpr("*", "size(array_distinct(flatten(transform(segments, (v, i) -> array(v._1, v._2))))) as count").show(False)
#dfJSON.write.format('json').mode('overwrite').save(processed_filename)

# ----------------------------------------------------------------------------------------
# dfFromTxt = spark.readStream.format("text").option("path","./input_stream").load()

# from pyspark.sql.functions import regexp_replace

# dfFromTxt = dfFromTxt.withColumn('value', regexp_replace('value', 'False', '"FALSE"'))
#dfFromTxt.count()

#dfFromTxt = dfFromTxt.withColumn('value', regexp_replace('value', '\},\ \{', '\}__SPLIT_POINT__\{'))
#dfFromTxt.show(truncate=False)

# from pyspark.sql.types import StructType,StructField,IntegerType,StringType,DoubleType,BooleanType

# schema = StructType([
# 	StructField("Price Grand Total",StringType(),True),
# 	StructField("Carrier Code", StringType(), True),
# 	StructField("DepartureDate", StringType(), True),
# 	StructField("ArrivalDate", StringType(), True),
# 	StructField("Number of Stops", IntegerType(), True)
# ])


# # from pyspark.sql.functions import col,from_json
# # dfJSON = dfFromTxt.withColumn("jsonData",from_json(col("value"),schema)).select("jsonData.*").dropna()
# # #dfJSON.show()


# # from pyspark.sql.functions import split, explode
# # dfFromTxt = dfFromTxt.withColumn('value',explode(split('value','__SPLIT_POINT__')))
# # #dfFromTxt.count()

# # #dfFromTxt.show(truncate=False)


# # dfJSON = dfFromTxt.withColumn("jsonData",from_json(col("value"),schema)).select("jsonData.*").dropna()
# # #dfJSON.show()


# # dfFromTxt = dfFromTxt.withColumn('value', regexp_replace('value', '\[', '')).withColumn('value', regexp_replace('value', '\]', ''))
# # #dfFromTxt.show(truncate=False)

# # dfJSON = dfFromTxt.withColumn("jsonData",from_json(col("value"),schema)).select("jsonData.*").dropna()
# # #dfJSON.show()

# # batDF = sampleDF.select("type", "price.total")
# # batDF = sampleDF.select("type", "iteneraries.segment.")

# #query = dfJSON.writeStream.outputMode("append").format("console").start()
# query = dfJSON.writeStream.outputMode("append").format("json").option("checkpointLocation", "C:/Users/Katerina/AppData/Local/Temp/tmpforspark").option("path", "./output_stream").start()

# query.awaitTermination()

