import glob
import os
import pandas as pd
# from tqdm import tqdm
from datetime import datetime
import time
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from matplotlib.widgets import Cursor
import numpy as np
from common import data_path, b_days

pd.set_option('display.max_columns', None)
# pd.set_option("display.max_rows", None)

df_cummulative = pd.DataFrame(columns=['Date'])
df_bids=pd.DataFrame(columns=['Date','Straddle_Name','bid','bid_time','bid_price','decay','decay_time','decay_price'])
start_date = str(input('Enter start date in YYYY-MM-DD format:\t'))
start_date = pd.to_datetime(start_date)
end_date = str(input('Enter end date in YYYY-MM-DD format:\t'))
end_date = pd.to_datetime(end_date)
if start_date > end_date:
    raise ValueError('Start date must be less than end date')
use_date_range = [use_date.date() for use_date in b_days if start_date<=use_date<=end_date]

def calc_business_days(chk_date, exp_date):
    holidays_23 = ['2023-01-26', '2023-03-07', '2023-03-30', '2023-04-04', '2023-04-07', '2023-04-14', '2023-05-01',
                   '2023-06-29', '2023-08-15', '2023-09-19', '2023-10-02', '2023-10-24', '2023-11-14', '2023-11-27',
                   '2023-12-25']
    holidays_24 = ['2024-01-22', '2024-01-26', '2024-03-08', '2024-03-25', '2024-03-29', '2024-04-11', '2024-04-17',
                   '2024-05-01', '2024-05-20', '2024-06-17', '2024-07-17', '2024-08-15', '2024-10-02', '2024-11-01',
                   '2024-11-15', '2024-12-25']
    holidays = holidays_23 + holidays_24
    holidays = pd.to_datetime(holidays)  # Convert string dates to datetime objects
    excluded_dates = ['2024-01-20', '2024-03-02', '2024-05-04', '2024-05-18', '2024-06-01', '2024-07-06', '2024-08-03',
                      '2024-09-14', '2024-10-05', '2024-11-09', '2024-12-07']
    excluded_dates = pd.to_datetime(excluded_dates)
    holidays = holidays[~holidays.isin(excluded_dates)]  # Remove the excluded dates from the holidays list

    exp_date = pd.to_datetime(exp_date)
    chk_date = pd.to_datetime(chk_date)

    business_days_left = pd.bdate_range(start=chk_date, end=exp_date, holidays=holidays, freq='C', weekmask='1111100')
    actual_bus_days = len(business_days_left) - 1
    return actual_bus_days

def bid_summary(df):
    use_date = df.get('Date').unique()[0]
    straddle_name = df.get('straddle_name').unique()[0]
    expiry_date = df.get('expiry').unique()[0]
    dte = calc_business_days(pd.to_datetime(use_date), pd.to_datetime(expiry_date))
    dfbid = df.copy()
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
    each_row = pd.DataFrame([[use_date,straddle_name, expiry_date, dte, bid, bid_time[0],bid_price[0],decay,decay_time[0],decay_price[0]]],
                columns=['Date','Straddle_Name', 'expiry', 'dte', 'bid','bid_time','bid_price','decay','decay_time','decay_price'])
    # df_cummulative = pd.concat([df_cummulative, dfbid])
    dfbid = dfbid.drop(['cummax', 'cummin'], axis=1)
    dfbid['dte'] = dte
    if bid_time[0] < decay_time[0]:
        start_time = bid_time[0]
        end_time = decay_time[0]
    else:
        start_time = decay_time[0]
        end_time = bid_time[0]
    dfbid = dfbid.query('Timestamp >= @start_time and Timestamp <= @end_time')
    dfbid.sort_values(by=['Decay'], ascending = False, inplace=True)
    return each_row, dfbid

def calc_bid_and_cummulative():
    global df_bids, df_cummulative
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
        df_copy.rename(columns = {'combined_premium':'Straddle', 'underlying':'straddle_name'}, inplace=True)
        df_copy = df_copy.reindex(['Date', 'Timestamp', 'straddle_name', 'expiry', 'strike', 'Straddle'], axis=1)
        grouped_df = df_copy.groupby(['Date','straddle_name'])['expiry'].unique().reset_index()

        for index, row in grouped_df.iterrows():
            use_date = row['Date']
            symbol = row['straddle_name']
            exp_list = sorted(row['expiry'])
            for i in range(len(exp_list)):
                each_exp = exp_list[i]
                df1 = df_copy.query('Date == @use_date and straddle_name == @symbol and expiry == @each_exp')
                if i == 0:
                    df1.loc[:,'straddle_name'] = df1['straddle_name'].astype(str) + '_CW'
                elif i == 1:
                    df1.loc[:,'straddle_name'] = df1['straddle_name'].astype(str) + '_NW'
                elif i == 2:
                    df1.loc[:,'straddle_name'] = df1['straddle_name'].astype(str) + '_CM'
                else:
                    df1.loc[:,'straddle_name'] = df1['straddle_name'].astype(str) + '_NM'

                # code to find df_bids and df_cummulative
                try:
                    each_row, dfbid = bid_summary(df1)
                    df_cummulative = pd.concat([df_cummulative, dfbid])
                    df_bids = pd.concat([df_bids, each_row])
                    print(f'{symbol} {each_exp} done')
                except Exception as e:
                    print(f'Error is {e}')
    df_bids.columns = map(str.lower, df_bids.columns)
    df_cummulative.columns = map(str.lower, df_cummulative.columns)
    # no of strikes(col) in df_bids for each straddle_name for each date and expiry
    df_cummulative['strike'] = df_cummulative['strike'].apply(lambda row: int(row))
    bid_date_list = df_bids['date'].unique().tolist()
    strd_name_list = df_bids['straddle_name'].unique().tolist()
    for each_bid_date in bid_date_list:
        for each_strd_name in strd_name_list:
            num_strikes = len(df_cummulative.query("date == @each_bid_date and straddle_name == @each_strd_name"))
            df_bids.loc[(df_bids['date'] == each_bid_date) & (df_bids['straddle_name'] == each_strd_name), 'num_strikes'] = num_strikes
            # ---------------------------------------------------------------------------------------
            strike_list = df_cummulative.query("date == @each_bid_date and straddle_name == @each_strd_name")['strike'].unique().tolist()
            strike_decay_list = []
            for each_strike in strike_list:
                max_decay = df_cummulative.query(
                    "date == @each_bid_date and straddle_name == @each_strd_name and strike == @each_strike"
                )['decay'].max()
                min_decay = df_cummulative.query(
                    "date == @each_bid_date and straddle_name == @each_strd_name and strike == @each_strike"
                )['decay'].min()

                # if max_decay == min_decay == 0:
                #     continue
                if max_decay == min_decay:
                    final_decay = max_decay
                else:
                    final_decay = max_decay - min_decay
                strike_decay_list.append(round((final_decay),2))
            st_num = len(strike_list)
            st_str = ','.join(map(str, sorted(strike_list)))
            decay_str = ','.join(map(str, strike_decay_list))
            df_bids.loc[(df_bids['date'] == each_bid_date) & (df_bids['straddle_name'] == each_strd_name),'decay'] = sum(strike_decay_list)
            df_bids.loc[(df_bids['date'] == each_bid_date) & (df_bids['straddle_name'] == each_strd_name), 'num_strikes'] = int(st_num)
            df_bids.loc[(df_bids['date'] == each_bid_date) & (df_bids['straddle_name'] == each_strd_name), 'distinct_strikes'] = st_str
            df_bids.loc[(df_bids['date'] == each_bid_date) & (df_bids['straddle_name'] == each_strd_name), 'strike_decay'] = decay_str
    df_bids['num_strikes'] = df_bids['num_strikes'].astype(int)
    # df_bids['decay'] = df_bids['bid_price'] - df_bids['decay_price']
    df_cummulative['dte'] = df_cummulative['dte'].apply(lambda row: int(row))
    df_bids['dte'] = df_bids['dte'].apply(lambda row: int(row))
    # df_bids = df_bids[['date', 'straddle_name', 'expiry', 'dte', 'bid', 'bid_time', 'decay', 'decay_time', 'num_strikes', 'distinct_strikes', 'strike_decay']]
    # df_cummulative = df_cummulative[
    #     ['date', 'timestamp', 'straddle_name', 'expiry', 'dte', 'strike', 'straddle', 'bid', 'decay']]
    df_cummulative = df_cummulative.query('straddle_name not in ["NIFTY_NM", "NIFTY_NW", "NIFTY_CM", "BANKNIFTY_NW", "FINNIFTY_NW", "MIDCPNIFTY_NW"]')
    df_bids = df_bids.query('straddle_name not in ["NIFTY_NM", "NIFTY_NW", "NIFTY_CM", "BANKNIFTY_NW", "FINNIFTY_NW", "MIDCPNIFTY_NW"]')
    return df_bids, df_cummulative


def plot_graph(use_date, strd_name):
    global df_bids, df_cummulative
    # strd_name = str(input('Enter straddle_name(BANKNIFTY_CW, BANKNIFTY_NW etc) for which graph needs to be plotted:\t'))
    dfx = df_cummulative.query('date == @use_date and straddle_name == @strd_name')
    dfx['date_timestamp'] = pd.to_datetime(dfx['date'] + ' ' + dfx['timestamp'])
    df_grouped = dfx.groupby('strike')['decay'].sum().reset_index()
    ax = df_grouped.plot(kind = 'bar', x = 'strike', y = 'decay', legend = True, grid = True)
    ax.set_xlabel('Strike')
    ax.set_ylabel('Decay')
    # dfx.plot.bar(x = 'strike', y = 'decay')
    ax.set_title('Strike vs Decay')
    plt.xticks(rotation=45)
    plt.show()

# def main():
#     df_bids, df_cummulative = calc_bid_and_cummulative(use_date_range)

df_bids, df_cummulative = calc_bid_and_cummulative()
print(f'df_bids is \n{df_bids}')
print(f'df_cummulative is \n{df_cummulative}')

# function to plot the curve -
# query the required dates from df_cummulative and plot the time vs decay curve

strd_name = str(input('Enter straddle_name(BANKNIFTY_CW, BANKNIFTY_NW etc) for which graph needs to be plotted:\t'))
print(f'your choice for dates - {df_cummulative["date"].unique().tolist()}')
use_date = str(input('Enter date in format - YYYY-MM-DD\t'))
# use_date = pd.to_datetime(use_date)
plot_graph(use_date, strd_name)


