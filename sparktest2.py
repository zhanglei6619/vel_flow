'''
2015年、2016年非节假日期间
主线站收费金额、车流量查询（审计）
用时
'''
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
import time
import sys  
reload(sys)  
sys.setdefaultencoding('utf8')
start_time = time.time()
spark = SparkSession.builder.getOrCreate()
schema= StructType([StructField("exstation", IntegerType(), True),\
StructField("exstationname", StringType(), True),\
StructField("exvehclass", IntegerType(), True),\
StructField("paytype", IntegerType(), True),\
StructField("vehcount", IntegerType(), True),\
StructField("realmoney", DecimalType(12,2), True),\
StructField("extruckflag", IntegerType(), True),\
StructField("exshiftdate", DateType(), True),\
StructField("yearflag", IntegerType(), True)])
l = '/usr/local/python2.7/exmoney201'
txtpath = [l+'5.unl',l+'6.unl']
s = spark.read.csv(txtpath,sep="|",schema=schema,dateFormat='yyyy/MM/dd')
s.createOrReplaceTempView('df')
sql="select exstation,exstationname,exvehclass,\
case when paytype=0 then 'NONETC' else 'ETC' end,\
sum(vehcount) vehcount,sum(realmoney) realmoney \
from df where \
exstation in (0220,3020,2020,0320,1920,1620,2520) \
and (exshiftdate between '2015-10-08'\
and '2016-02-07'\
or exshiftdate>='2016-02-14'\
or exshiftdate <'2015-10-01')\
and exshiftdate > '2015-07-18'\
and realmoney>0 \
and extruckflag=0 \
and yearflag=0 \
group by exstation,exstationname,exvehclass,paytype"
s1= spark.sql(sql)
s1.show()
total_duration = (time.time() - start_time)/60
print("Tests passed in %f miniutes", total_duration)
