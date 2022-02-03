from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession

path = "hdfs://master1.t1.soc.ecssec.ru:8020/tmp/test/input/word_count"
spark = SparkSession.builder.appName("ReadFromHDFS").getOrCreate()
sparkcont = SparkContext.getOrCreate(SparkConf().setAppName("ReadFromHDFS"))
logs = sparkcont.setLogLevel("ERROR")

logData = spark.read.text(path).cache()
logData.show()

spark.stop()
