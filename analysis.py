from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType, BooleanType, DoubleType
import pymongo

client = pymongo.MongoClient("mongodb+srv://theresatvan:UEi8751OX1jaT9lz@cluster0.6jfc5iw.mongodb.net/")
mydatabase = client["gfw"]
mycollection = mydatabase["gfw"]

data = list(mycollection.find({}))
spark = SparkSession.builder.appName("myApp").getOrCreate()
schema = StructType([
    StructField("year", IntegerType(), True),
    StructField("month", StringType(), True),
    StructField("country", StringType(), True),
    StructField("record", StringType(), True)
])
df = spark.createDataFrame(data, schema=schema)
df.show()

client.close()
