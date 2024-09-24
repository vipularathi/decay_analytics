import glob
import os
import pandas as pd
# from tqdm import tqdm
from datetime import datetime
import time
# import matplotlib.pyplot as plt
import numpy as np
from common import pickle_dir, table_dir, data_path, b_days

# l = os.listdir(pickle_dir)
# print(l)
# l1 = [name for name in l if 'bids' in name.lower()]
# print(l1)
# change as per analysis required
# for_date = datetime(year=2024, month=9, day=10)

def bid_summary(df):
    # columns=['Date','TIMESTAMP','Straddle']
    # time=datetime.time(9,14,00)
    use_date = df.get('Date').unique()[0]
    straddle_name = df.get('underlying').unique()[0]
    dfbid = df.copy()
    # dfbid=df.rename(columns={'combined_premium':'Straddle'})
    # dfbid=df.query("Date==@date and TIMESTAMP != @time and Straddle!=0")[columns].reset_index(drop=True)
    dfbid['cummax']=dfbid['Straddle'].cummax()
    dfbid['cummin']=dfbid['Straddle'].cummin()
    dfbid['Bid']=dfbid['Straddle']-dfbid['cummin']
    dfbid['Decay']=dfbid['cummax']-dfbid['Straddle']
    bid=dfbid['Bid'].max()
    decay=dfbid['Decay'].max()
    bid_time=dfbid.loc[dfbid['Bid']==bid].reset_index()['Timestamp']
    bid_price=dfbid.loc[dfbid['Bid']==bid].reset_index()['Straddle']
    decay_price=dfbid.loc[dfbid['Decay']==decay].reset_index()['Straddle']
    decay_time=dfbid.loc[dfbid['Decay']==decay].reset_index()['Timestamp']
    each_row = pd.DataFrame([[use_date,straddle_name,bid,bid_time[0],bid_price[0],decay,decay_time[0],decay_price[0]]],
                columns=['Date','Straddle_Name','bid','bid_time','bid_price','decay','decay_time','decay_price'])
    # df_cummulative = pd.concat([df_cummulative, dfbid])
    dfbid = dfbid.drop(['cummax', 'cummin'], axis=1)
    if bid_time[0] < decay_time[0]:
        start_time = bid_time[0]
        end_time = decay_time[0]
    else:
        start_time = decay_time[0]
        end_time = bid_time[0]
    dfbid = dfbid.query('Timestamp >= @start_time and Timestamp <= @end_time')
    # ----------------------------------------------------------------
    # can be modified as per requirement
    dfbid.sort_values(by=['Decay'], ascending = False, inplace=True)
    # ----------------------------------------------------------------
    return each_row, dfbid
# ----------------------------------------------------------------
# Enter date range
df_cummulative = pd.DataFrame(columns=['Date'])
df_bids=pd.DataFrame(columns=['Date','Straddle_Name','bid','bid_time','bid_price','decay','decay_time','decay_price'])
start_date = str(input('Enter start date:\t'))
start_date = pd.to_datetime(start_date)
end_date = str(input('Enter end date:\t'))
end_date = pd.to_datetime(end_date)
if start_date > end_date:
    raise ValueError('Start date must be less than end date')
use_date_range = [use_date.date() for use_date in b_days if start_date<=use_date<=end_date]
# ----------------------------------------------------------------
for each_date in use_date_range:
    file_name = f'iv_chart_table_data_{each_date.strftime("%d_%m_%Y")}.csv'
    df = pd.read_csv(os.path.join(data_path, file_name)) # if we have range of dates, then for each date, concat df
    # df_cummulative = pd.DataFrame(columns=['Date'])
    # df_bids=pd.DataFrame(columns=['Date','Straddle_Name','bid','bid_time','bid_price','decay','decay_time','decay_price'])

    df_copy = df.drop(df.columns[~df.columns.isin(set(['timestamp', 'underlying', 'expiry', 'strike', 'combined_premium']))], axis=1).copy()
    df_copy['timestamp'] = pd.to_datetime(df_copy.timestamp)
    df_copy['Date'] = df_copy.timestamp.dt.date.astype(str)
    df_copy['Timestamp'] = df_copy.timestamp.dt.time.astype(str)
    df_copy = df_copy.drop('timestamp', axis=1)
    df_copy.rename(columns = {'combined_premium':'Straddle'}, inplace=True)
    df_copy = df_copy.reindex(['Date', 'Timestamp', 'underlying', 'expiry', 'strike', 'Straddle'], axis=1)
    grouped_df = df_copy.groupby(['Date','underlying'])['expiry'].unique().reset_index()

    # underlying_chosen = str(input("Enter the name of underlying (nf, bn, fn, mn) - "))
    # file = f'{underlying_chosen.upper()}.pkl'
    # df = pd.read_pickle(os.path.join(pickle_dir, file))
    # df.to_csv(os.path.join(pickle_dir, f'BNBids.csv'), index=False)
    # print(df)
    #
    # # pickle_folder='C:/Users/rahuljayanth/Desktop/Python work/Pickle/Plots/'
    # # file='BN.pkl'
    # #
    # # df=pd.read_pickle(pickle_folder+file)
    # if underlying_chosen == 'nf':
    #     straddle_name='NIFTY CW'
    # elif underlying_chosen == 'bn':
    #     straddle_name = 'Banknifty CW'
    # elif underlying_chosen == 'fn':
    #     straddle_name = 'FINNIFTY CW'
    # else:
    #     straddle_name = 'MIDCPNIFTY'

    #
    # date=datetime.date(2024,9,4)
    # # res = bid_summary(df,date,straddle_name)
    # # print(res)
    # # #
    #
    # for symbol in grouped_df.index.tolist():
    #     for each_expiry in grouped_df[symbol]:
    #         new_df = df_copy.query('underlying == @symbol and expiry == @each_expiry')
    #         print(new_df)
    #         try:
    #             df_row, dfbid = bid_summary(df,date,straddle_name)
    #             df_cummulative = pd.concat([df_cummulative, dfbid])
    #             # df_bids=pd.concat([df_bids,bid_summary(df,date,straddle_name)])
    #             df_bids = pd.concat([df_bids, df_row])
    #         except:
    #             print(date)

    for index, row in grouped_df.iterrows():
        use_date = row['Date']
        symbol = row['underlying']
        exp_list = sorted(row['expiry'])
        for i in range(len(exp_list)):
            each_exp = exp_list[i]
            df1 = df_copy.query('Date == @use_date and underlying == @symbol and expiry == @each_exp')
            if i == 0:
                df1['underlying'] = df1['underlying'].astype(str) + '_CW'
            elif i == 1:
                df1['underlying'] = df1['underlying'].astype(str) + '_NW'
            elif i == 2:
                df1['underlying'] = df1['underlying'].astype(str) + '_CM'
            else:
                df1['underlying'] = df1['underlying'].astype(str) + '_NM'

            # code to find df_bids and df_cummulative
            try:
                each_row, dfbid = bid_summary(df1)
                df_cummulative = pd.concat([df_cummulative, dfbid])
                df_bids = pd.concat([df_bids, each_row])
                print(f'{symbol} {each_exp} done')
            except Exception as e:
                print(f'Error is {e}')

# for date in df['Date'].unique():
# df_bids.to_csv(os.path.join(table_dir, f'df_bids_{underlying_chosen.upper()}.csv'), index=False)
# print('df_bids csv made')
# df_cummulative.to_csv(os.path.join(table_dir, f'df_cummulative_{underlying_chosen.upper()}.csv'), index=False)
# print('df_cummulative csv made')
# # print(df_bids)
# # #
# # # df_bids=df_bids.sort_values(by='Date',ascending=False)
# # # df_bids.query('bid>150')
print(f'df_bids is \n{df_bids}')
print(f'df_cummulative is \n{df_cummulative}')
