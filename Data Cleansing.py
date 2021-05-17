import pandas as pd
import numpy as np
import glob, math

# 合併月營收資料
val = pd.read_excel(r"./Data/營收/上市電子業_營收.xlsx")
val['年份'] = val['年月'].apply(lambda x: x[:4])
val['月份'] = val['年月'].apply(lambda x: int(x[5:]))
val['季'] = val['年月'].apply(lambda x: math.ceil(int(x[5:]) / 3))
val_season = val.groupby(['代號', '年份', '季'])['單月營收(千元)'].count().to_frame('紀錄').reset_index()
val_season = val_season[val_season['紀錄']==3]
save_list = list(zip(val_season['代號'], val_season['年份'], val_season['季']))

new_val = pd.DataFrame()
for x, y, z in save_list:
    a = val['代號']==x
    b = val['年份']==y
    c = val['季']==z
    tmp = val[(a)&(b)&(c)]
    new_val = pd.concat([new_val, tmp], 0)

season = new_val.groupby(['代號', '年份', '季'])['單月營收(千元)'].sum().to_frame('單季營收(千元)').reset_index()
new_val = pd.merge(new_val, season, on=['代號', '年份', '季'], how='left')
new_val['單季營收成長'] = new_val['單月營收(千元)'].shift(-2)
new_val['單季營收成長'] = (new_val['單季營收成長'] - new_val['單月營收(千元)']) / new_val['單月營收(千元)']
new_val['單季營收成長'] = new_val.apply(lambda r:r['單季營收成長'] if r['月份'] in [1, 4, 7, 10] else None, 1)
new_val = new_val[['代號', '年份', '季', '單季營收(千元)', '單季營收成長']][new_val['單季營收成長'].notnull()]
new_val.to_csv(r'./Data/完整季營收.csv', index=0, header=True, encoding='utf-8')

# 統整TEJ資料
for i, x in enumerate(glob.glob(r"./Data/上市電子族群財報DATA/*.xlsx")):
    print(x, end='\r')
    if i == 0:
        df = pd.read_excel(x)
    else:
        df1 = pd.read_excel(x)
        df = pd.merge(df, df1, on=['代號', '名稱', '年/月'], how='left')

df['年份'] = df['年/月'].apply(lambda x: x[:4])
df['季'] = df['年/月'].apply(lambda x: math.ceil(int(x[5:]) / 3))

df1 = pd.merge(df, new_val, on=['代號', '年份', '季'], how='left')
df1.replace({np.nan:None}, inplace=True)
df1.to_csv(r'./Data/overall_20210514.csv', index=0, header=True, encoding='utf-8')

# 計算季K
share = pd.read_csv(r"./Data/電子族群股價資料.csv")
share['stock_id'] = share['stock_id'].astype(int).astype(str)
share['年份'] = share['date'].apply(lambda x:x[:4])
share['季'] = share['date'].apply(lambda x: math.ceil(int(x[5:7]) / 3))

min_date = share.groupby(['stock_id', '年份', '季'])['date'].min().to_frame('date').reset_index()
max_date = share.groupby(['stock_id', '年份', '季'])['date'].max().to_frame('date').reset_index()
min_date['Openday'] = 1
max_date['Closeday'] = 1

share = pd.merge(share, min_date, on=['stock_id', '年份', '季', 'date'], how='left')
share = pd.merge(share, max_date, on=['stock_id', '年份', '季', 'date'], how='left')
share['Openday'] = share['Openday'].fillna(0)
share['Closeday'] = share['Closeday'].fillna(0)

s_share = share.groupby(['stock_id', '年份', '季']).agg({'max':'max', 'min':'min', 'Trading_Volume':'sum', 'Trading_turnover':'sum'}).reset_index()
s_share = pd.merge(s_share, share[['stock_id', '年份', '季', 'open']][share['Openday']==1], on=['stock_id', '年份', '季'], how='left')
s_share = pd.merge(s_share, share[['stock_id', '年份', '季', 'close']][share['Closeday']==1], on=['stock_id', '年份', '季'], how='left')
s_share['spread'] = s_share['close'].shift(1)
s_share['spread'][(s_share['年份']=='2009')&(s_share['季']==4)] = None
s_share.replace({np.nan:None}, inplace=True)
s_share['spread'] = s_share.apply(lambda r:(r['close'] - r['spread']) / r['spread'] if r['spread'] else None, 1)
s_share.rename(columns={'stock_id':'代號', 'max':'最高價', 'min':'最低價',
                        'Trading_Volume':'成交量(股)', 'Trading_turnover':'交易額(萬)',
                        'open':'開盤價', 'close':'收盤價', 'spread':'漲跌幅(%)'}, inplace=True)
s_share.to_csv(r"./Data/電子族群股價資料_季K.csv", index=0, header=True, encoding='utf-8')

# 合併
df = pd.read_csv(r'./Data/overall_20210514.csv')
s_share = pd.read_csv(r"./Data/電子族群股價資料_季K.csv")
s_share['季'] = s_share['季'].apply(lambda x:4 if x == 1 else x - 1)
s_share['年份'] = s_share.apply(lambda r:int(r['年份']) - 1 if r['季'] == 4 else r['年份'], 1)

df1 = pd.merge(df, s_share, on=['代號', '年份', '季'], how='left')
df1 = df1[df1['漲跌幅(%)'].notnull()]

# 標記
df1['進場訊號'] = df1['漲跌幅(%)'].apply(lambda x:1 if x >= 0.1 else 0)
df1 = df1[df1['單季營收(千元)'].notnull()]
df1.columns = [c.replace(' ', '') for c in df1.columns]
df1.to_csv(r"./Data/overall_20210515.csv", index=0, header=True, encoding='utf-8')

# Features Cleansing
df = pd.read_csv(r"./Data/overall_20210515.csv")
df.replace({np.nan:None}, inplace=True)
df['營收變動率'][df['營收變動率'].isnull()] = 39.36
df['營業利益變動率'][df['營業利益變動率'].isnull()] = 229.06
df['淨利變動率（單季）'][df['淨利變動率（單季）'].isnull()] = 50.029
df['稅前盈餘變動率'][df['稅前盈餘變動率'].isnull()] = -37.83

tmp = pd.read_excel(r"./Data/overall_空值統計_20210515(最新).xlsx")
fillzero = list(tmp['index'][tmp['空值處理']=='補0'])
for name in fillzero:
    df[name] = df[name].fillna(0)

deletecolumns = list(tmp['index'][tmp['空值處理']=='Delete'])
df.drop(deletecolumns, 1, inplace=True)

df['營業毛利率'] = df['營業毛利'] / df['營業收入淨額']
df['營業利益率'] = df['營業利益'] / df['營業收入淨額']
df['稅前淨利率'] = df['稅前淨利'] / df['營業收入淨額']
df['本期綜合損益總額'] = df.apply(lambda r: r['本期綜合損益總額'] if r['本期綜合損益總額'] else r['合併總損益'], 1)
df.to_csv(r"./Data/overall_20210515_V2.csv", index=0, header=True, encoding='utf-8')
