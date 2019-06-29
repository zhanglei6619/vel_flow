'''
2015年7-12月
收费站车流量排序
320站某天某时每分钟车流量
查询用时
'''
import pandas as pd
from pandas import DataFrame
import pickle
import time

start_time = time.time()
f07= open('/usr/local/python2.7/ex07_m','rb')
ex07_m = pickle.load(f07)
f07.close()
f08= open('/usr/local/python2.7/ex08_m','rb')
ex08_m = pickle.load(f08)
f08.close()
f09= open('/usr/local/python2.7/ex09_m','rb')
ex09_m = pickle.load(f09)
f09.close()
f10= open('/usr/local/python2.7/ex10_m','rb')
ex10_m = pickle.load(f10)
f10.close()
f11= open('/usr/local/python2.7/ex11_m','rb')
ex11_m = pickle.load(f11)
f11.close()
f12= open('/usr/local/python2.7/ex12_m','rb')
ex12_m = pickle.load(f12)
f12.close()
s = pd.concat([ex07_m,ex08_m,ex09_m,ex10_m,ex11_m,ex12_m])
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
