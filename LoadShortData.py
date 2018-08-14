import pandas as pd
from datetime import datetime
import datetime as dt
import time
import numpy as np
import os
import sys
import logging
import zipfile
import psycopg2
from sqlalchemy import create_engine
import gc
import objgraph
import pandas_market_calendars as mcal
from Underlying import DataManager
from functools import partial
from itertools import product
import math
from pandas import DataFrame
import os.path
from Underlying import existingTickers
import time
import calendar

import psutil

from multiprocessing import Pool
import multiprocessing
from multiprocessing import Process

DATA_PATH = 'C:\\Users\\Yaos\\Desktop\\Trading\\ShortSqueezeData\\'

MONTHS = ['',
'Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun',
'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'
]



def loadShortData(filename):


# READ OPTION CSV FILE
    usecols = ['Name','Symbol', 'Total Short Interest', 'Days to Cover', 'Short % of Float', '% Insider Ownership', '% Institutional Ownership']
    usecols += ['Short: Prior Mo','% Change Mo/Mo','Avg. Daily Vol.','Shares: Float', 'Shares: Outstanding',' Price ',' Market Cap ','Sector','Industry','Record Date']



    dtypes = {'Name': pd.np.str_,
              'Symbol': pd.np.str_,
              'Total Short Interest': pd.np.int64,
              'Days to Cover': pd.np.float16,
              'Short % of Float': pd.np.float16,
              'Short: Prior Mo': pd.np.float16,
              '% Change Mo/Mo': pd.np.float16,
              'Avg. Daily Vol.': pd.np.int64,
              'Shares: Float': pd.np.int64,
              'Shares: Outstanding': pd.np.int64,
              '% Insider Ownership': pd.np.float16,
              '% Institutional Ownership': pd.np.float16,
              ' Price ': pd.np.float16,
              ' Market Cap ': pd.np.int64,
              'Sector': pd.np.str_,
              'Industry': pd.np.str_,
              'Record Date': pd.np.str_,
                }



    df = pd.read_csv(DATA_PATH + '{0}'.format(str(filename)), encoding='ISO-8859-1', usecols=usecols, na_values=['-Infinity'])
    # df = pd.read_excel(io=DATA_PATH + '{0}'.format(str(filename)), usecols=usecols, dtype=dtypes)

    df = df.dropna(subset=['Symbol'])
    # print(df.info())

    number_cols = ['Total Short Interest', 'Days to Cover', 'Short % of Float', '% Insider Ownership', '% Institutional Ownership']
    number_cols += ['Short: Prior Mo', '% Change Mo/Mo', 'Avg. Daily Vol.', 'Shares: Float', 'Shares: Outstanding', ' Price ', ' Market Cap ']
    for i in number_cols:
        df[i] = df[i].apply(lambda x: x.replace(",","").replace("$","").replace(" ","") if isinstance(x, str) else x)
        df[i] = pd.to_numeric(df[i], errors='raise')
        # df[i] = float(df[i].replace(",","").replace("$","").replace(" ",""))

    print("{0} | Loading Short Data | Read {1} Recs".format(filename, len(df)))
    df_rename = {"Name": "name",
                 "Symbol": "symbol",
                 "Total Short Interest": "short_int",
                 "Days to Cover": "short_day_cover",
                 "Short % of Float": "short_float",
                 "Short: Prior Mo": "short_int_prior",
                 "% Change Mo/Mo": "short_change_pct",
                 "Avg. Daily Vol.": "avg_daily_vol",
                 "Shares: Float": "shares_float",
                 "Shares: Outstanding": "shares_outstanding",
                 "% Insider Ownership": "insider_own_pct",
                 "% Institutional Ownership": "institution_own_pct",
                 " Price ": "price",
                 " Market Cap ": "market_cap",
                 "Sector": "sector",
                 "Industry": "industry",
                 "Record Date": "record_date"
                 }
    df.rename(columns=df_rename, inplace=True)

    df['date'] = df['record_date'].apply(lambda x: recordDate(x))
    df.drop(columns='record_date', inplace=True)

    connection_info = create_engine('postgresql://postgres:inkstain@localhost:5432/wzyy_options')
    df.to_sql('short_data', connection_info, if_exists='append', index=False)
    connection_info.dispose()


def recordDate(string):
    # print(string.split("-"))
    year = int(string.split("-")[0])
    month_string = string.split("-")[1]
    month = MONTHS.index(month_string[:3])
    mid_ind = month_string[-1:]

    if mid_ind == 'A':
        day = round((calendar.monthrange(year, month)[0] + calendar.monthrange(year, month)[1])/2)
    else:
        day = calendar.monthrange(year, month)[1]

    # print(year,month,day)
    data_date = dt.date(year, month, day)
    return(data_date)
    # print(string, " : ", data_date)

if __name__ == '__main__':
    files = os.listdir(DATA_PATH)
    xlsfiles = [fi for fi in files if (fi.endswith(".csv"))]
    i = 0

    for file in xlsfiles:
        try:
            i += 1
            print("{0} | Loading Short Data | {1} / {2} ".format(file,i,len(xlsfiles)))
            loadShortData(file)
        except Exception as e:
            print("{0} | Error Loading | ",format(file), e)