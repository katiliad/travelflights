from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[*]").appName("Streaming corrupted json").getOrCreate()

#dfFromTxt=spark.read.text("corrupted.json")
#dfFromTxt.show(truncate=False)

dfFromTxt = spark.readStream.format("text").option("path","./input_stream").load()

from pyspark.sql.functions import regexp_replace
dfFromTxt = dfFromTxt.withColumn('value', regexp_replace('value', 'False', '"FALSE"'))
#dfFromTxt.count()

dfFromTxt = dfFromTxt.withColumn('value', regexp_replace('value', '/},/ /{', '/}__SPLIT_POINT__/{'))
#dfFromTxt.show(truncate=False)

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
#dfJSON.show()


from pyspark.sql.functions import split, explode
dfFromTxt = dfFromTxt.withColumn('value',explode(split('value','__SPLIT_POINT__')))
#dfFromTxt.count()

#dfFromTxt.show(truncate=False)


dfJSON = dfFromTxt.withColumn("jsonData",from_json(col("value"),schema)).select("jsonData.*").dropna()
#dfJSON.show()


dfFromTxt = dfFromTxt.withColumn('value', regexp_replace('value', '/[', '')).withColumn('value', regexp_replace('value', '/]', ''))
#dfFromTxt.show(truncate=False)

dfJSON = dfFromTxt.withColumn("jsonData",from_json(col("value"),schema)).select("jsonData.*").dropna()
#dfJSON.show()

#query = dfJSON.writeStream.outputMode("append").format("console").start()
query = dfJSON.writeStream.outputMode("append").format("json").option("checkpointLocation", "C:/Users/Katerina/AppData/Local/Temp/tmpforspark").option("path", "./output_stream").start()

query.awaitTermination()

