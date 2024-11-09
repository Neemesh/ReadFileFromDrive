from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
import sys
import os

python_path = sys.executable
os.environ['PYSPARK_PYTHON'] = python_path

os.environ['JAVA_HOME'] = r'C:\Users\nnamb\.jdks\corretto-1.8.0_432'

conf = SparkConf().setAppName("pyspark").setMaster("local[*]").set("spark.driver.host","localhost").set("spark.default.parallelism", "1")
sc = SparkContext(conf=conf)
spark = SparkSession.builder.getOrCreate()

sc.textFile("D:\Coding\FirstPython\state.txt", 1).foreach(print)
spark.read.csv("D:\Coding\FirstPython\state.txt").show()