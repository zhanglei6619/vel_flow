'''
2015年1-6月
收费站车流量排序
320站下午1点每分钟车流量汇总
分别用时
'''
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
import time
start_time = time.time()
spark = SparkSession.builder.getOrCreate()
schema = StructType([StructField("exstation", IntegerType(), True),StructField("extime", DateType(), True)])
l = '/usr/local/python2.7/ex20160'
txtpath = [l+'1.txt',l+'2.txt',l+'3.txt',l+'4.txt',l+'5.txt',l+'6.txt']
s = spark.read.csv(txtpath,sep="|",schema=schema,dateFormat='yyyy-MM-dd hh:mm:ss')
s.createOrReplaceTempView('df')
sx = s.withColumn("hour",hour("extime")).withColumn("minute",minute("extime")).withColumn("day",date_format("extime",'yyyy-MM-dd'))
sx.createOrReplaceTempView('dfx')
start_time1 = time.time()
s1 = spark.sql('select exstation,count(*) veh_flow from df group by exstation order by count(*) desc')
s1.show()
total_duration1 = (time.time() - start_time1)/60
print("Tests passed in %f miniutes", total_duration1)
start_time2 = time.time()
s2 = spark.sql("select minute,count(*) veh_flow from dfx where exstation=320 and hour=13 group by minute order by minute")
s2.show()
total_duration2 = (time.time() - start_time2)/60
print("Tests passed in %f miniutes", total_duration2)
start_time3=time.time()

total_duration3 = (time.time() - start_time3)/60
print("Tests passed in %i miniutes", total_duration3)
total_duration = (time.time() - start_time)/60
print("Tests passed in %f miniutes", total_duration)
