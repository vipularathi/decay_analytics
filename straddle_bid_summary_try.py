import glob
import os
import pandas as pd
# from tqdm import tqdm
import datetime
import time
import matplotlib.pyplot as plt
import numpy as np
from common import pickle_dir, table_dir

# l = os.listdir(pickle_dir)
# print(l)
# l1 = [name for name in l if 'bids' in name.lower()]
# print(l1)

# underlying_chosen = str(input("Enter the name of underlying (nf, bn, fn, mn) - "))
# file = f'{underlying_chosen.upper()}.pkl'

df = pd.read_pickle(os.path.join(pickle_dir, 'BN.pkl'))
# df.to_csv(os.path.join(pickle_dir, f'BNBids.csv'), index=False)
# print(df)
#
# # pickle_folder='C:/Users/rahuljayanth/Desktop/Python work/Pickle/Plots/'
# # file='BN.pkl'
# #
# # df=pd.read_pickle(pickle_folder+file)
straddle_name='Banknifty CW'
date=datetime.date(2024,9,6)

df_cummulative=pd.DataFrame(columns=['Date'])


def bid_summary(df,date,straddle_name):
    columns=['Date','TIMESTAMP','Straddle']
    time=datetime.time(9,14,00)
    df=df.rename(columns={straddle_name:'Straddle'})
    dfbid=df.query("Date==@date and TIMESTAMP != @time and Straddle!=0")[columns].reset_index(drop=True)
    dfbid['cummax']=dfbid['Straddle'].cummax()
    dfbid['cummin']=dfbid['Straddle'].cummin()
    dfbid['Bid']=dfbid['Straddle']-dfbid['cummin']
    dfbid['Decay']=dfbid['cummax']-dfbid['Straddle']
    bid=dfbid['Bid'].max()
    decay=dfbid['Decay'].max()
    bid_time=dfbid.loc[dfbid['Bid']==bid].reset_index()['TIMESTAMP']
    bid_price=dfbid.loc[dfbid['Bid']==bid].reset_index()['Straddle']
    decay_price=dfbid.loc[dfbid['Decay']==decay].reset_index()['Straddle']
    decay_time=dfbid.loc[dfbid['Decay']==decay].reset_index()['TIMESTAMP']
    df_new = pd.DataFrame([[date,straddle_name,bid,bid_time[0],bid_price[0],decay,decay_time[0],decay_price[0]]],
                columns=['Date','Straddle_Name','bid','bid_time','bid_price','decay','decay_time','decay_price'])
    # df_cummulative = pd.concat([df_cummulative, dfbid])
    dfbid = dfbid.drop(['cummax', 'cummin'], axis=1)
    dfbid = dfbid.query('TIMESTAMP >= @bid_time[0] and TIMESTAMP <= @decay_time[0]')
    dfbid.sort_values(by=['Decay'], ascending = False, inplace=True)
    return df_new, dfbid
#
date=datetime.date(2024,9,4)
# res = bid_summary(df,date,straddle_name)
# print(res)
# #
df_bids=pd.DataFrame(columns=['Date','Straddle_Name','bid','bid_time','bid_price','decay','decay_time','decay_price'])
for date in df['Date'].unique():
    try:
        df_row, dfbid = bid_summary(df,date,straddle_name)
        df_cummulative = pd.concat([df_cummulative, dfbid])
        # df_bids=pd.concat([df_bids,bid_summary(df,date,straddle_name)])
        df_bids = pd.concat([df_bids, df_row])
    except:
        print(date)
df_bids.to_csv(os.path.join(table_dir, 'df_bids.csv'), index=False)
print('df_bids csv made')
df_cummulative.to_csv(os.path.join(table_dir, 'df_cummulative.csv'), index=False)
print('df_cummulative csv made')
# print(df_bids)
# #
# # df_bids=df_bids.sort_values(by='Date',ascending=False)
# # df_bids.query('bid>150')