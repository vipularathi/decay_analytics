import io
import zipfile

import pandas as pd
import sqlalchemy as sql
from sqlalchemy import create_engine, inspect
from urllib.parse import quote_plus
import psycopg2
import re
import io
import os
import requests
from common import data_dir, yesterday

remote_host = "172.16.47.56"
remote_dbname = "TBTMData"
remote_user = "postgres"
remote_password = "Rathi123"

inst_list = ["JINDALSTEL", "LICHSGFIN", "ABB", "UNITDSPR", "RAMCOCEM", "LTTS", "BANKBARODA", "COROMANDEL", "GUJGASLTD", "OBEROIRLTY", "TITAN", "NMDC", "PEL", "SBILIFE", "AARTIIND", "BAJAJFINSV", "TATASTEEL", "DALBHARAT", "PFC", "HAL", "GLENMARK", "COALINDIA", "MUTHOOTFIN", "TCS", "DRREDDY", "LT", "IPCALAB", "SUNPHARMA", "BALKRISIND", "ICICIPRULI", "HCLTECH", "LTIM", "DEEPAKNTR", "CHOLAFIN", "GAIL", "VOLTAS", "ULTRACEMCO", "CHAMBLFERT", "APOLLOHOSP", "INDUSTOWER", "RBLBANK", "HINDUNILVR", "MCX", "VEDL", "BHARTIARTL", "GNFC", "ITC", "CROMPTON", "BANDHANBNK", "POWERGRID", "CANFINHOME", "HAVELLS", "HINDPETRO", "M&M", "ADANIENT", "POLYCAB", "SAIL", "BHARATFORG", "DIVISLAB", "IEX", "MPHASIS", "SHRIRAMFIN", "SIEMENS", "BRITANNIA", "TATAMOTORS", "MOTHERSON", "SBICARD", "ABCAPITAL", "BAJFINANCE", "MRF", "DIXON", "IDFCFIRSTB", "INDIAMART", "TATACHEM", "LTF", "CUB", "IOC", "BOSCHLTD", "ZYDUSLIFE", "SYNGENE", "PNB", "ATUL", "AUBANK", "RELIANCE", "PIIND", "LALPATHLAB", "FINNIFTY", "ABFRL", "MIDCPNIFTY", "TATACONSUM", "IGL", "NATIONALUM", "MARUTI", "SUNTV", "ALKEM", "IDFC", "HDFCLIFE", "GRANULES", "NESTLEIND", "MANAPPURAM", "ADANIPORTS", "ACC", "ASIANPAINT", "COFORGE", "ESCORTS", "BPCL", "WIPRO", "NIFTY", "ASHOKLEY", "BHEL", "COLPAL", "NAVINFLUOR", "TATAPOWER", "EXIDEIND", "BALRAMCHIN", "CIPLA", "ASTRAL", "LUPIN", "PIDILITIND", "AXISBANK", "SRF", "INDHOTEL", "ABBOTINDIA", "TECHM", "GRASIM", "ICICIBANK", "JSWSTEEL", "AUROPHARMA", "BATAINDIA", "INDIGO", "HDFCBANK", "APOLLOTYRE", "TRENT", "FEDERALBNK", "GODREJPROP", "HINDCOPPER", "TVSMOTOR", "AMBUJACEM", "CONCOR", "BSOFT", "TORNTPHARM", "EICHERMOT", "DABUR", "HINDALCO", "MARICO", "SHREECEM", "NIFTYNXT50", "INFY", "BANKNIFTY", "IDEA", "BERGEPAINT", "DLF", "BEL", "IRCTC", "NTPC", "LAURUSLABS", "METROPOLIS", "CANBK", "OFSS", "MGL", "KOTAKBANK", "HEROMOTOCO", "M&MFIN", "ICICIGI", "NAUKRI", "PAGEIND", "TATACOMM", "PVRINOX", "ONGC", "JUBLFOOD", "SBIN", "BIOCON", "RECLTD", "JKCEMENT", "PETRONET", "UPL", "GMRINFRA", "CUMMINSIND", "BAJAJ-AUTO", "MFSL", "INDUSINDBK", "GODREJCP", "HDFCAMC", "UBL", "PERSISTENT", "INDIACEM", "ZEEL", "MCDOWELL-N"]

def chk_remote_db():
    try:
        remote_engine = create_engine(f'postgresql+psycopg2://{remote_user}:{remote_password}@{remote_host}:5432/{remote_dbname}')

        query = '''
            select table_name from information_schema.tables
            where table_schema = 'public'
        '''

        df = pd.read_sql(query, remote_engine)
        df.to_csv(f'table_list.csv', index=False)
        print(df)

        df1 = df.copy()
        # df1['table_name'] = df1['table_name'].apply(lambda x: x if x.endswith('_fo_1M') else None)
        df1['table_name'] = df1['table_name'].apply(lambda x: x if isinstance(x, str) and x.endswith('_fo_1M') else None)
        # df1 = df1[(df1['table_name'] != None)]
        # df1 = df1[df1['table_name'].endswith('_fo_1M')]
        # df1.to_csv(f'fo_table_list.csv', index=False )
        # df1 = df1.query('table_name == None')
        df1 = df1[df1['table_name'].notna()]
        df1.to_csv(f'df1.csv', index=False)
        df_list = df1['table_name'].tolist()
        # print(f'{df1}\n{type(df1.tolist())}')
        # pattern =
        # print(df_list)
        # print(len(df_list))
        # instrument_name_list = [name.split('_fo_1M')[0] for name in df_list]
        # print(len(instrument_name_list))
        # print(instrument_name_list)
        # for each in inst_list:
        #     if each in instrument_name_list:
        #         continue
        #         # print(f'{each} - is present')
        #     else:
        #         print(f'{each} - is not present')
        # ----------------------------------------------------------------
        # for each in df_list:
        #     test = each.split('_fo_1M')[0]
        #     if test in inst_list:
        #         print(f'{each} - is not present\t{test} {type(test)}')
        #     else:
        #         print(f'{each} - is not present')
        # ----------------------------------------------------------------
        return df_list
    except psycopg2.Error as e:
        print(f'Error is {e}')

def download_bhavcopy(download_date, use :str = 'fo'):
    link_dict = {'fo': 'https://nsearchives.nseindia.com/content/fo/', 'cm':'https://nsearchives.nseindia.com/content/cm/'}
    filename_dict = {'fo':'BhavCopy_NSE_FO_0_0_0_', 'cm':'BhavCopy_NSE_CM_0_0_0_'}
    use_link = link_dict[use]
    use_filename = filename_dict[use]
    for_date = download_date.strftime('%Y%m%d')

    bhav_url = f'{use_link}{use_filename}{for_date}_F_0000.csv.zip' # Sample - https://nsearchives.nseindia.com/content/fo/BhavCopy_NSE_FO_0_0_0_2024SEP06_F_0000.csv.zip
    folder_name = f'{use_filename}{for_date}_F_0000'
    bhav_file_name = f'{use_filename}{for_date}_F_0000.csv'
    file_path = os.path.join(data_dir, folder_name)
    headers = {
        'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.55 Safari/537.36',
        'accept-encoding': 'gzip, deflate, br',
        'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8'
    }
    response_data = requests.get(bhav_url, headers=headers)
    zip_data = io.BytesIO(response_data.content)
    with zipfile.ZipFile(zip_data, 'r') as zip_file:
        zip_file.extractall(file_path)

    df = pd.read_csv(os.path.join(file_path, bhav_file_name), index_col=False)
    return df

# def bhavcopy_ops():
#     bhav_df = download_bhavcopy()

# testing
# dfg = download_bhavcopy(download_date=yesterday)
# print(dfg)
chk_remote_db()

