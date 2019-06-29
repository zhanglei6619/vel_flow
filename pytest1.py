'''
2015年11月
收费站车流量排序
320站某天某时每分钟车流量
查询用时
'''
import pandas as pd
from pandas import DataFrame
import pickle
import time

start_time = time.time()
f12= open('/usr/local/python2.7/ex11_m','rb')
s = pickle.load(f12)
f12.close()
start_time1 = time.time()
grouped = s.groupby('exstation')
gr = grouped.size().sort_values(ascending = False)
total_duration1 = (time.time() - start_time1)/60
print("Tests passed in %f miniutes", total_duration1)
start_time2 = time.time()
grouped = s[(s['hour'] == 12)&(s['date'] == '20150707')&(s['exstation'] == 320)].groupby('minute')
gr = grouped.size()
total_duration2 = (time.time() - start_time2)/60
print("Tests passed in %f miniutes", total_duration2)
total_duration = (time.time() - start_time)/60
print("Tests passed in %f miniutes", total_duration)
