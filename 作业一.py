import numpy as np
import pandas as pd

# 有两行除了电话号码外一模一样。
df = pd.read_csv('./HTTP_20130313143750.dat', delimiter='\t', header=None, index_col=None, usecols=None, dtype=None, engine='c', skiprows=None, nrows=None, encoding='utf-8', low_memory=True)
df.columns = ['时间戳', '电话号码', '基站的物理地址', '访问网址的 ip', '网站域名', '网站类型', '数据包', '接包数', '上行 / 传流量', '下行 / 载流量', '响应码']

# 聚合的电话号码比示例中多一个。
df_gb = df[['电话号码', '上行 / 传流量', '下行 / 载流量']].groupby('电话号码').sum(min_count=1)
df_gb['总流量'] = df_gb['上行 / 传流量'] + df_gb['下行 / 载流量']

df_gb.to_csv('./part-r-00000', sep='\t', header=False, index=True, encoding='gb18030')