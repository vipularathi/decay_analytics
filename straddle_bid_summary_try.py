import glob
import os
import pandas as pd
# from tqdm import tqdm
from datetime import datetime
import time
# import matplotlib.pyplot as plt
import numpy as np
from common import pickle_dir, table_dir, data_path

# l = os.listdir(pickle_dir)
# print(l)
# l1 = [name for name in l if 'bids' in name.lower()]
# print(l1)
# change as per analysis required
for_date = datetime(year=2024, month=9, day=20)
file_name = f'iv_chart_table_data_{for_date.strftime("%d_%m_%Y")}.csv'
df = pd.read_csv(os.path.join(data_path, file_name))
df_cummulative = pd.DataFrame(columns=['Date'])
df_bids=pd.DataFrame(columns=['Date','Straddle_Name','bid','bid_time','bid_price','decay','decay_time','decay_price'])

df_copy = df.drop(df.columns[~df.columns.isin(set(['timestamp', 'underlying', 'expiry', 'strike', 'combined_premium']))], axis=1).copy()
df_copy['timestamp'] = pd.to_datetime(df_copy.timestamp)
df_copy['Date'] = df_copy.timestamp.dt.date.astype(str)
df_copy['Timestamp'] = df_copy.timestamp.dt.time.astype(str)
df_copy = df_copy.drop('timestamp', axis=1)
df_copy = df_copy.reindex(['Date', 'Timestamp', 'underlying', 'expiry', 'strike', 'combined_premium'], axis=1)
grouped_df = df_copy.groupby('underlying')['expiry'].unique()

# underlying_chosen = str(input("Enter the name of underlying (nf, bn, fn, mn) - "))
# file = f'{underlying_chosen.upper()}.pkl'
# df = pd.read_pickle(os.path.join(pickle_dir, file))
# # df.to_csv(os.path.join(pickle_dir, f'BNBids.csv'), index=False)
# # print(df)
# #
# # # pickle_folder='C:/Users/rahuljayanth/Desktop/Python work/Pickle/Plots/'
# # # file='BN.pkl'
# # #
# # # df=pd.read_pickle(pickle_folder+file)
# if underlying_chosen == 'nf':
#     straddle_name='NIFTY CW'
# elif underlying_chosen == 'bn':
#     straddle_name = 'Banknifty CW'
# elif underlying_chosen == 'fn':
#     straddle_name = 'FINNIFTY CW'
# else:
#     straddle_name = 'MIDCPNIFTY'

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

for symbol in grouped_df.index.tolist():
    for each_expiry in grouped_df[symbol]:
        new_df = df_copy.query('underlying == @symbol and expiry == @each_expiry')
        print(new_df)
        try:
            df_row, dfbid = bid_summary(df,date,straddle_name)
            df_cummulative = pd.concat([df_cummulative, dfbid])
            # df_bids=pd.concat([df_bids,bid_summary(df,date,straddle_name)])
            df_bids = pd.concat([df_bids, df_row])
        except:
            print(date)

# for date in df['Date'].unique():
# df_bids.to_csv(os.path.join(table_dir, f'df_bids_{underlying_chosen.upper()}.csv'), index=False)
# print('df_bids csv made')
# df_cummulative.to_csv(os.path.join(table_dir, f'df_cummulative_{underlying_chosen.upper()}.csv'), index=False)
# print('df_cummulative csv made')
# # print(df_bids)
# # #
# # # df_bids=df_bids.sort_values(by='Date',ascending=False)
# # # df_bids.query('bid>150')