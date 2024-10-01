import os
from datetime import datetime
import pandas as pd

root_dir = os.path.dirname(os.path.abspath(__file__))
data_dir = os.path.join(root_dir, f'data/')
# pickle_dir = os.path.join(data_dir, 'straddle_pickle_files/')
logs_dir = os.path.join(root_dir,'logs/')
# table_dir = os.path.join(data_dir, 'tables/')
data_path = r"W:\Options & Futures Data\Intraday straddle and IV"
dir_list = [data_dir, logs_dir]
status = [os.makedirs(_dir, exist_ok=True) for _dir in dir_list if not os.path.exists(_dir)]

holidays_24 = ['2024-01-22', '2024-01-26', '2024-03-08', '2024-03-25', '2024-03-29', '2024-04-11', '2024-04-17', '2024-05-01', '2024-06-17', '2024-07-17', '2024-08-15', '2024-10-02', '2024-11-01', '2024-11-15', '2024-12-25']
holidays = pd.to_datetime(holidays_24)
excluded_dates = []
if excluded_dates is not None:
    excluded_dates = pd.to_datetime(excluded_dates)
    holidays = holidays[~holidays.isin(excluded_dates)]

b_days = pd.bdate_range(start=pd.to_datetime('2024-05-23'), end=pd.to_datetime(datetime.now().date().strftime('%Y-%m-%d')), holidays=holidays, freq='C', weekmask='1111100').tolist()
# print(b_days[0],'\n', type(b_days[0]),'\n', type(b_days))
# print(b_days)
# today, yesterday = b_days[-1], b_days[-2]
# yesterday = pd.Timestamp(year=2024, month=8, day=7)
# print(os.listdir())
# for _fol in os.listdir():
#     if os.

# fol_range = [f'fol_{i}' for i in range(10)]
# status = [os.makedirs(_fol, exist_ok=True) for _fol in fol_range if not os.path.exists(_fol)]

# df = pd.read_csv(os.path.join('fol_0', ))
