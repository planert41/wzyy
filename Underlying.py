# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""
# ALPHA VANTAGE API KEY PR2GF4AKJDDZ10XJ

import pandas as pd

pd.core.common.is_list_like = pd.api.types.is_list_like
from pandas import HDFStore, read_hdf, DataFrame
import datetime as dt
from datetime import datetime, date, timedelta, time
import numpy as np
import psycopg2
from time import time as tm
import logging
from sqlalchemy import create_engine
# from alpha_vantage.timeseries import TimeSeries
# from alpha_vantage.techindicators import TechIndicators
from functools import reduce
import pandas_datareader.data as web
from pandas.util.testing import assert_frame_equal
from alphaVantageAPI.alphavantage import AlphaVantage



# ts = TimeSeries(key='PR2GF4AKJDDZ10XJ', output_format='pandas')
# price_data, meta_data = ts.get_daily(symbol='MSFT', outputsize='full')

# tc = TechIndicators(key='PR2GF4AKJDDZ10XJ', output_format='pandas')
# EMA8_data, meta_data = tc.get_ema(symbol='MSFT', interval='daily', time_period = 8, series_type = 'close')
# SMA200_data, meta_data = tc.get_sma(symbol='MSFT', interval='daily', time_period = 200, series_type = 'close')
# SMA100_data, meta_data = tc.get_sma(symbol='MSFT', interval='daily', time_period = 100, series_type = 'close')
# print(EMA8_data.head())

# dfs = [price_data, EMA8_data, SMA200_data, SMA100_data]
# df_final = reduce(lambda left,right: pd.merge(left,right,on='date'), dfs)
#
## We can describe it
#
##print(data.head())
# df_final.to_csv("MSFT_Flags.csv")
# print("CSV Exported")

class DataManager():

    def calculateMA(self, df):
        df.sort_index(inplace=True)
        df['sma_200'] = round(df['close'].rolling(window=200).mean(), 2)
        df['sma_100'] = round(df['close'].rolling(window=100).mean(), 2)
        df['ema_8'] = round(df['close'].ewm(span=8, adjust=False).mean(), 2)
        print("{0}| {1} Prices| {2} SMA-200| {3} 100-SMA:{3}| {4} EMA-8".format(df['symbol'][0], len(df), df['sma_200'].count(), df['sma_100'].count(), df['ema_8'].count()))
        return df

    def fetchUnderlyingMS(self, ticker, date_length='compact'):
        #        f = web.DataReader(ticker,'robinhood')
        # end = date
        # start = date - timedelta(days=10)
        # # newstart = date = timedelta(days = (365 * 5))
        # newstart = datetime(end.year - 5, 1, 1)
        pd.options.mode.chained_assignment = None

        connection = create_engine('postgresql://postgres:inkstain@localhost:5432/wzyy_options')

    # QUERY EXISTING UNDERLYING DATA
        request = "SELECT * FROM underlying_data where symbol = '{0}' ORDER BY date DESC;".format(ticker)
        df = pd.read_sql_query(request, con=connection)

        df.set_index(pd.DatetimeIndex(df['date']), inplace=True)
        # df.set_index((df['date']), inplace=True)
        df.drop('date', axis=1, inplace=True)

    # WEB QUERY STOCK DATA UP TO 5 YEARS
    #     ms = web.DataReader(ticker, 'morningstar', newstart, end).xs(ticker)
    #     ms = web.DataReader(ticker, 'iex', newstart, end)
    #     ms.to_csv("{0}_underlying_raw_test.csv".format(ticker))

        av = AlphaVantage(
            api_key='PR2GF4AKJDDZ10XJ',
            output_size=date_length,
            datatype='json',
            export=False,
            export_path='~/av_data',
            output='csv',
            clean=False,
            proxy={}
        )
        print("Fetching Prices for {0} - {1}".format(ticker, date_length))
        ms = av.data(symbol=ticker, function='D')
        ms.reset_index(inplace=True)
        ms_rename = {"datetime":"date", "1. open":"open","2. high":"high","3. low":"low","4. close":"close","5. volume":"volume"}
        ms.rename(columns=ms_rename, inplace=True)
        ms['open'] = (round(pd.to_numeric(ms['open'], errors='coerce'), 2))
        ms['high'] = (round(pd.to_numeric(ms['high'], errors='coerce'), 2))
        ms['low'] = (round(pd.to_numeric(ms['low'], errors='coerce'), 2))
        ms['close'] = (round(pd.to_numeric(ms['close'], errors='coerce'), 2))
        ms['volume'] = pd.to_numeric(ms['volume'], errors='coerce')


        ms.set_index(pd.DatetimeIndex(ms['date']), inplace=True)
        # ms.set_index((ms['date']), inplace=True)
        ms.drop('date', axis=1, inplace=True)
        ms.sort_index(ascending=False, inplace=True)
        # ms['date'] = datetime.strptime(ms.index, "%Y-%m-%d")
        # ms.index.names = ['date']

        ms['symbol'] = ticker
        # print("{0} MS Query {1} to {2}: {3}".format(ticker,start.strftime('%Y-%m-%d'), end.strftime('%Y-%m-%d'), len(ms)))

    # SETS DATES AS INDEX FOR BOTH DATASETS TO COMPARE
        ms_new = ms[~ms.index.isin(df.index)]
        ms_exist = ms[ms.index.isin(df.index)]
        df_exist = df[df.index.isin(ms.index)]

        print(ticker, "DB:", len(df), " Query:", len(ms), " NEW:", len(ms_new))

        #  Check existing price data to see if it matches
        if len(ms_exist) > 0:

            df_exist['total'] = df_exist[['close', 'high', 'low', 'volume']].sum(axis=1)
            ms_exist['total'] = ms_exist[['close', 'high', 'low', 'volume']].sum(axis=1)

            ms_exist = pd.merge(ms_exist, df_exist, on='date', how='left', suffixes=('_df', '_ms'))

            #            print(ms_exist.head())
            #            ms_exist = pd.merge(ms_exist,df_exist['date','df_total'],on='date')
            #            ms_exist.merge(df_exist['date','df_total'].to_frame(), left_on='date', right_index=True)
            exist_dif = ms_exist[abs(ms_exist['total_df'] - ms_exist['total_ms'])>10]

            if len(exist_dif) > 0:
                exist_dif.to_csv('Exist_Dif.csv')
                print("Underlying Error: New {0} Data Not Matching for {1} Days ".format(ticker, len(exist_dif)))
                print("DIF", exist_dif.head())
            # else:
            # print("{0} {1} to {2} Data Match: {3} ".format(ticker, ms_exist.index.min().strftime('%Y-%m-%d'), ms_exist.index.max().strftime('%Y-%m-%d'),len(ms_exist)))

        if len(ms_new) > 0:
            # Merge new price data to database and calculate MAs
            df = pd.concat([df, ms_new], sort=True)
            df.sort_index(inplace=True)
            df = self.calculateMA(df)
            df_upload = df[df.index.isin(ms_new.index)]

            try:
                df_upload.reset_index(inplace=True)
                df_upload['date'] = pd.to_datetime(df_upload['date'], format='%Y-%m-%d').dt.date
                # df_upload.to_csv("{0}_underlying_test.csv".format(ticker))
                df_upload.to_sql('underlying_data', connection, if_exists='append', index=False)
                print("{0}| Update {1} prices".format(ticker, len(ms_new)))
                # print(df_upload.reset_index().head())
            except Exception as e:
                print("Error Update New Price {0} {1} Records, Last {2}".format(ticker, len(ms_new), e))
                connection.dispose()
        # else:
        # print("NO NEW PRICE. {0} {1} Last: DB: {2} Query: {3}".format(ticker, date.strftime('%Y-%m-%d'), df.index.max().strftime('%Y-%m-%d'), ms.index.max().strftime('%Y-%m-%d')))
        connection.dispose()

# conn= create_engine('postgresql://postgres:inkstain@localhost:5432/wz_info')
#
# try:
#        request="SELECT * FROM tickers ORDER BY ticker DESC;"
#        df = pd.read_sql_query(request,con=conn)
#        my_list = df["ticker"].tolist()
#        print("List: {0}".format(my_list))
#
#        batch_price_data, meta_data = ts.get_batch_stock_quotes(symbols=my_list)
#        print(batch_price_data)
#        batch_price_data.to_csv("Batch_Price.csv")
#        conn.dispose()
# except Exception as e:
#        print("Error Pulling Tickers",e)
#        conn.dispose()
if __name__ == '__main__':

    dm = DataManager()
    test = dm.fetchUnderlyingMS("AAAP", date_length='full')
#