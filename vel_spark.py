'''
pyspark分析交通：
1、sparkSQL
2、pandas显示结果（文章配图）
'''
# -*- coding: utf-8 -*-
import pandas as pd
from pandas import DataFrame
import matplotlib.pyplot as plt
import cPickle
import pyspark
from pyspark import SparkContext
from pyspark.sql import SparkSession

sc = SparkContext()
spark = SparkSession.builder \
           .master('local[*]') \
           .appName('My App') \
           .config('spark.sql.warehouse.dir', 'C:\Users\Administrator') \
           .getOrCreate()
                     
f7= open('e:\ex07_m','rb')
df = cPickle.load(f7)
f7.close()
s = spark.createDataFrame(df)#此法效率低，不建议使用
s.createOrReplaceTempView('df')
sqlresult = spark.sql('select exstation,count(*) veh_flow from df group by exstation order by count(*) desc')
k = sqlresult.toPandas()
k = pd.DataFrame(data = list(k['veh_flow']),columns = ['veh_flow'],index=list(k['exstation']))
k.plot.bar()

sqlresult = spark.sql('select exstation,count(*) veh_flow from df where hour= 12 group by exstation order by count(*) desc')
k = sqlresult.toPandas()
k = pd.DataFrame(data = list(k['veh_flow']),columns = ['veh_flow'],index=list(k['exstation']))
k.plot.bar()

sqlresult = spark.sql("select exstation,count(*) veh_flow from df where date= '20150708' group by exstation order by count(*) desc")
k = sqlresult.toPandas()
k = pd.DataFrame(data = list(k['veh_flow']),columns = ['veh_flow'],index=list(k['exstation']))
k.plot.bar()

sqlresult = spark.sql("select exstation,count(*) veh_flow from df where date= '20150708' and  hour= 12 group by exstation order by veh_flow desc")
k = sqlresult.toPandas()
k = pd.DataFrame(data = list(k['veh_flow']),columns = ['veh_flow'],index=list(k['exstation']))
k.plot.bar()

sqlresult = spark.sql('select date, count(*) veh_flow from df where exstation= 220 group by date')
k = sqlresult.toPandas()
k = pd.DataFrame(data = list(k['veh_flow']),columns = ['veh_flow'],index=list(k['date']))
k.plot.bar()

sqlresult = spark.sql('select date,count(*) veh_flow from df where exstation= 220 and hour=14 group by date order by date')
k = sqlresult.toPandas()
k.plot.bar()

sqlresult = spark.sql("select hour,count(*) veh_flow from df where exstation= 220 and date='20150709' group by hour order by hour")
k = sqlresult.toPandas()
k = pd.DataFrame(data = list(k['veh_flow']),columns = ['veh_flow'],index=list(k['hour']))
k.plot.bar()

sqlresult = spark.sql("select minute,count(*) veh_flow from df where exstation= 220 and date='20150709' and hour=14 group by minute order by minute")
k = sqlresult.toPandas()
k = pd.DataFrame(data = list(k['veh_flow']),columns = ['veh_flow'],index=list(k['minute']))
k.plot.bar()

#df = pd.DataFrame({'a':[1,2,3,6],'b':[2,3,4,5]},index=(0,1,'a','z'))
##################结果绘图
import sys
reload(sys)
sys.getdefaultencoding()
sys.setdefaultencoding('utf-8')

df = pd.DataFrame({u'站统计排名':[0.125,4.68],u'分钟车流量':[1.59,21.14]},index=[18,110])
df.plot(kind='bar',title=u'Python车流量统计')
plt.xlabel(u'数据量（百万）')
plt.ylabel(u'时间（秒）')

df = pd.DataFrame({u'单机Spark':[3.32,3.64,6.81],u'Spark集群-3台':[1.07,1.20,1.97]},index=[18,110,200])
df.plot(kind='bar')
plt.xlabel(u'数据量（百万）')
plt.ylabel(u'时间（分钟）')
plt.title(u'Spark单机与集群收费金额查询时间')





