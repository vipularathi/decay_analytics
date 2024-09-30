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

df_cummulative = pd.DataFrame(columns=['Date'])
df_bids=pd.DataFrame(columns=['Date','Straddle_Name','bid','bid_time','bid_price','decay','decay_time','decay_price'])
start_date = str(input('Enter start date:\t'))
start_date = pd.to_datetime(start_date)
end_date = str(input('Enter end date:\t'))
end_date = pd.to_datetime(end_date)
if start_date > end_date:
    raise ValueError('Start date must be less than end date')
use_date_range = [use_date.date() for use_date in b_days if start_date<=use_date<=end_date]

def bid_summary(df):
    use_date = df.get('Date').unique()[0]
    straddle_name = df.get('straddle_name').unique()[0]
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
    bid_date_list = df_bids['date'].unique().tolist()
    strd_name_list = df_bids['straddle_name'].unique().tolist()
    for each_bid_date in bid_date_list:
        for each_strd_name in strd_name_list:
            num_strikes = len(df_cummulative.query("date == @each_bid_date and straddle_name == @each_strd_name"))
            df_bids.loc[(df_bids['date'] == each_bid_date) & (df_bids['straddle_name'] == each_strd_name), 'num_strikes'] = num_strikes
    return df_bids, df_cummulative


def plot_graph(strd_name):
    global df_bids, df_cummulative
    # strd_name = str(input('Enter straddle_name(BANKNIFTY_CW, BANKNIFTY_NW etc) for which graph needs to be plotted:\t'))
    dfx = df_cummulative.query('straddle_name == @strd_name')
    dfx['date_timestamp'] = pd.to_datetime(dfx['date'] + ' ' + dfx['timestamp'])
    # ----------------------------------------------------------------
    # ax = plt.gca()
    # dfx.plot(kind = 'line', x = 'date_timestamp', y = 'strike', color = 'red', ax = ax)
    # dfx.plot(kind = 'line', x = 'date_timestamp', y = 'decay', color = 'blue', ax = ax)
    # ----------------------------------------------------------------
    fig, ax  = plt.subplots()
    ax1 = ax.twinx()
    ax.plot(dfx.date_timestamp, dfx.strike, color = 'red')
    ax1.plot(dfx.date_timestamp, dfx.decay, color = 'blue')
    ax.set_xlabel('Timestamp', fontsize = 8)
    ax.set_ylabel('Strike', fontsize = 8)
    ax1.set_ylabel('Decay', fontsize = 8)
    # ----------------------------------------------------------------
    plt.title('Strike vs datetime')
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d %H:%M%S'))
    plt.tight_layout()
    plt.xticks(rotation=90, fontsize=8)
    # cursor = Cursor(ax, useblit=True, color='green', linewidth=1)
    #
    # def format_tooltip(event):
    #     if event.inaxes == ax:
    #         # Get nearest x-value (timestamp)
    #         x_value = mdates.num2date(event.xdata).strftime('%Y-%m-%d %H:%M:%S')
    #         # Get nearest y-values (strike and decay)
    #         strike_value = dfx.loc[(dfx['date_timestamp'] - pd.Timestamp(event.xdata)).abs().idxmin(), 'strike']
    #         decay_value = dfx.loc[(dfx['date_timestamp'] - pd.Timestamp(event.xdata)).abs().idxmin(), 'decay']
    #         print(f"Timestamp: {x_value}, Strike: {strike_value}, Decay: {decay_value}")
    #
    # fig.canvas.mpl_connect('motion_notify_event', format_tooltip)

    cursor = Cursor(ax, useblit=True, color='green', linewidth=1)

    # Create annotation for tooltip
    annot = ax.annotate("", xy=(0, 0), xytext=(20, 20), textcoords="offset points",
                        bbox=dict(boxstyle="round", fc="w"),
                        arrowprops=dict(arrowstyle="->"))
    annot.set_visible(False)

    # Tooltip formatter
    def format_tooltip(event):
        if event.inaxes == ax:
            x_value = mdates.num2date(event.xdata).strftime('%Y-%m-%d %H:%M:%S')

            # Find the nearest data points for strike and decay
            nearest_index = (dfx['date_timestamp'] - pd.Timestamp(mdates.num2date(event.xdata))).abs().idxmin()
            strike_value = dfx.loc[nearest_index, 'strike']
            decay_value = dfx.loc[nearest_index, 'decay']

            # Update annotation text
            annot.xy = (event.xdata, event.ydata)
            text = f"Timestamp: {x_value}\nStrike: {strike_value}\nDecay: {decay_value}"
            annot.set_text(text)
            annot.set_visible(True)
            fig.canvas.draw_idle()

    # Connect the format_tooltip function to the mouse motion event
    fig.canvas.mpl_connect('motion_notify_event', format_tooltip)

    plt.show()

# def main():
#     df_bids, df_cummulative = calc_bid_and_cummulative(use_date_range)

df_bids, df_cummulative = calc_bid_and_cummulative()
pd.set_option('display.max_columns', None)
# pd.set_option("display.max_rows", None)
strd_name = str(input('Enter straddle_name(BANKNIFTY_CW, BANKNIFTY_NW etc) for which graph needs to be plotted:\t'))
plot_graph(strd_name)

# function to plot the curve -
# query the required dates from df_cummulative and plot the time vs decay curve

print(f'df_bids is \n{df_bids}')
print(f'df_cummulative is \n{df_cummulative}')
