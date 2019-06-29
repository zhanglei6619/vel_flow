# -*- coding: utf-8 -*-
'''
利用python pandas分析交通
1、ETL
2、分析语句
3、封装函数
4、显示
'''
# -*- coding: utf-8 -*-
import pandas as pd
from pandas import DataFrame
import matplotlib.pyplot as plt
from pyspark import SparkContext
import pickle
import cPickle

# 改字符集
import sys
reload(sys)
sys.getdefaultencoding()
sys.setdefaultencoding('utf-8')
#读取mysql数据库
import pandas.io.sql as sql
import pymysql.cursors
config = {
          'host':'127.0.0.1',
          'port':3306,
          'user':'root',
          'password':'gfszl',
          'db':'test',
          'charset':'utf8',
          'cursorclass':pymysql.cursors.DictCursor,
          }
          
conn = pymysql.connect(**config)
#cur = conn.cursor()
#cur.execute('select count(*) from exlist20151')
#val = cur.fetchall()
#1.1 各站车流量排名
ex07 = sql.read_sql('select exstation,exstationname from exlist201507',conn)
gr = ex07.groupby('exstationname')
lar07 = gr.size().nlargest(10)
lar07.plot(kind='bar')
#1.2 各站分时段车流量统计
import pandas as pd
from pandas import DataFrame
from dateutil.parser import parse
from datetime import datetime
ex07 = sql.read_sql('select extime,exstation from exlist201507',conn)
ex08 = sql.read_sql('select extime,exstation from exlist201508',conn)
ex09 = sql.read_sql('select extime,exstation from exlist201509',conn)
ex10 = sql.read_sql('select extime,exstation from exlist201510',conn)
ex11 = sql.read_sql('select extime,exstation from exlist201511',conn)
ex12 = sql.read_sql('select extime,exstation from exlist201512',conn)

# 无法直接对Serie操作：ex08.extime.head(4).strftime('%H-%M-%S')
#通过apply函数截取时间
def to_date(x):
    return x.strftime('%Y-%m-%d')
def to_hour(x):
    return int(x.strftime('%H'))
def to_minute(x):
    return int(x.strftime('%M'))
def to_second(x):
    return int(x.strftime('%S'))
date = ex09.extime.apply(to_date)
hour = ex09.extime.apply(to_hour)
#minute = ex08.extime.apply(to_minute)
#second = ex08.extime.apply(to_second)
#合并为dataframe(考虑直接从数据库读取转换后形成)
data = {'exstation':ex09.exstation,'date':date,'hour':hour}
ex09_m = pd.DataFrame(data)
#序列化
import pickle
f = open('e:\ex09_m','wb')
pickle.dump(ex09_m, f)
f.close()
#反序列化
f7= open('e:\ex07_m','rb')
ex07_m = cPickle.load(f7)
f7.close()
f8= open('e:\ex08_m','rb')
ex08_m = cPickle.load(f8)
f8.close()
f9= open('e:\ex09_m','rb')
ex09_m = cPickle.load(f9)
f9.close()
f10= open('e:\ex10_m','rb')
ex10_m = cPickle.load(f10)
f10.close()
f11= open('f:\ex11_m','rb')
ex11_m = cPickle.load(f11)
f11.close()
f12= open('e:\ex12_m','rb')
ex12_m = cPickle.load(f12)
f12.close()

#G93沙坪坝下午6时车流量
import pandas as pd
from pandas import DataFrame
s=ex11_m
grouped = s[(s['hour'] == 21)&(s['exstation'] == 3020)].groupby('date')
gr=grouped.size()
gr.plot(kind='bar')
#读入数据文件，形成时间序列
ex12 = sql.read_sql('select extime,exstation from exlist201512',conn)
ex12.to_csv('e:\CQGSdata\ex12.txt',header=None,sep='|')
#每小时车流量变化-每周固定日期
ex16 = pd.read_csv('e:\listex2016.txt',usecols=[0,1],names=['exstation','extime'],delimiter='|',parse_dates=[1],index_col=[1],header=None,encoding='gb2312')
ex16.index.tz_localize('Asia/Shanghai')
#2016年某站每天每小时车流量汇总表，再取抽样
ts = ex16[(ex16.index.hour == 0)&(ex16['exstation'] == 3020)].resample('D').count()
for i in range(23):
    ts = pd.merge(ts,ex16[(ex16.index.hour == i+1)&(ex16['exstation'] == 3020)].resample('D').count(),left_index=True,right_index=True)
ts.columns=['00','01','02','03','04','05','06','07','08','09','10','11','12','13','14','15','16','17','18','19','20','21','22','23']
#按星期-小时采样
ts['18'].describe
#按天每小时采样
ts.ix['2016-10-31']
   
ts = ex16[(ex16.index.hour == 14)&(ex16['exstation'] == 3020)&(ex16.index.weekday==4)].resample('D').count().replace(0,NaN).dropna()

ts.to_html()
ts.plot(kind='bar')

#每月流水合并
s = pd.concat([ex07_m,ex08_m,ex09_m,ex10_m,ex11_m])
#收费站车流量(每月，不分组的size需要除以列数)
ex08_1 = pd.value_counts(ex08_m.exstation)[:10]
ex08_l[:20].plot(kind='bar')
#分站每天车流量
grouped = ex08_m.groupby(['exstation','date'])
grouped.size()[:500]
#路网每天车流量
grouped2 = s3.groupby('date')
gr = grouped2.size()
gr.plot()
#收费站分时段车流量(每月)
ex08_t = ex08_m[(ex08_m['hour'] == 9)].groupby('exstation').size().sort_values(ascending = False)
ex08_s = ex08_m[ex08_m['exstation'] == 411].groupby('hour').size()
#ex08_s = pd.value_counts(ex08_m[ex08_m['exstation'] == 411].hour)[:24] -自动排序
##某站某时车流量总图
ex08_s = ex08_m[(ex08_m['exstation'] == 220)&((ex08_m['hour'] == 17))].groupby('date').size()
ex08_s.plot(kind='bar')

######function to calculate veh_flow in different condictions:#####
def veh_flow(station='重庆',date='20990911',hour=24):
    exstation=pd.read_csv('e:\CQGSdata\station.txt',names = ['exstation','name','blank'],delimiter = '|',header = None, encoding = 'gb2312')
    exstation = DataFrame(exstation,columns = ['name','exstation'],index = None)
    exstation = exstation[exstation['name'] == station.decode('utf8')].iloc[0,1]
    if exstation == 202:
        if date == '20990911':
            if hour == 24:
                grouped = s.groupby('exstation')
                gr = grouped.size().sort_values(ascending = False)
                return gr
            else:##需修改
                grouped = s[(s['hour'] == hour)].groupby('exstation')
                gr = grouped.size().sort_values(ascending = False)
                return gr
        else:
            if hour == 24:
                grouped = s[(s['date'] == date)].groupby('exstation')
                gr = grouped.size().sort_values(ascending = False)
                return gr
            else:
                grouped = s[(s['hour'] == hour)&(s['date'] == date)].groupby('exstation')
                gr = grouped.size().sort_values(ascending = False)
                return gr
    else:
        if date == '20990911':
            if hour == 24:
                grouped = s[(s['exstation'] == exstation)].groupby('date')
                gr = grouped.size()
                return gr
            else:
                grouped = s[(s['exstation'] == exstation)&(s['hour'] == hour)].groupby('date')
                gr = grouped.size()
                return gr
        else:
            if hour == 24:
                grouped = s[(s['exstation'] == exstation)&(s['date'] == date)].groupby('hour')
                gr = grouped.size()
                return gr
            else:#给出所有参数，则绘出分钟车流量
                grouped = s[(s['hour'] == hour)&(s['date'] == date)&(s['exstation'] == exstation)].groupby('minute')
                gr = grouped.size()
                return gr
                
if __name__ == '__main__':  
    
    k = veh_flow()
    plt.figure(); k.plot.bar()
    
                grouped = s.groupby('exstation')
                gr = grouped.size().sort_values(ascending = False)
                gr.head(10).plot(kind='bar')
                plt.xlabel(u'站编号')
                plt.ylabel(u'车流量')
                plt.title(u'前10名收费站车流量')
                
                grouped = s[(s['hour'] == 14)&(s['date'] == '20150709')&(s['exstation'] == 220)].groupby('minute')
                gr = grouped.size()
                gr.plot(kind='bar')
                plt.xlabel(u'分钟')
                plt.ylabel(u'车流量')
                plt.title(u'220收费站2015.7.9下午2时车流量')