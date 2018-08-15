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
import time
import string


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

class existingTickers():
    def all(self, option="active"):
        connection_options = create_engine('postgresql://postgres:inkstain@localhost:5432/wzyy_options')

        if option == "active":
            request = "select * from ticker_log where end_date is null order by symbol asc"
        else:
            request = "select * from ticker_log order by symbol asc"

        tickers = pd.read_sql_query(request, con=connection_options)
        # print("Existing Tickers Query | {0} | {1} Recs".format(option, len(tickers)))
        tickers['start_date'] = pd.to_datetime(tickers['start_date']).dt.date
        tickers['end_date'] = pd.to_datetime(tickers['end_date']).dt.date

        # tickers = [x[0] for x in fetched]
        connection_options.dispose()
        return tickers

    def fetchAllPrices(self):
        tickers_data = self.all(self, option="active")
        tickers = tickers_data['symbol']
        connection_options = create_engine('postgresql://postgres:inkstain@localhost:5432/wzyy_options')
        start_char = string.ascii_lowercase.index('u')

        for ticker in tickers:

            ticker_char = string.ascii_lowercase.index(ticker.lower()[0])
            if start_char > ticker_char:
                continue
            try:
                underlying_exists = connection_options.execute("SELECT exists( select * FROM underlying_data where symbol = '{0}')".format(ticker)).fetchone()[0]
                if not underlying_exists:
                    print("{0} Exists = {1}| Fetch Prices".format(ticker, underlying_exists))
                    DataManager.fetchUnderlyingMS(DataManager(), ticker, date_length='full')
                    time.sleep(5)
                else:
                    print("{0} Exists".format(ticker))
            except Exception as e:
                print("Fetch All Prices ERROR | ",ticker, e)
                connection_options.dispose()
        connection_options.dispose()

    def update(self):
        tickers_info = self.all(self, option='all')
        tickers = tickers_info['symbol']
        connection_options = create_engine('postgresql://postgres:inkstain@localhost:5432/wzyy_options')
        i = 0
        for ticker in tickers:
            request = "SELECT * FROM underlying_data where symbol = '{0}' ORDER BY date ASC;".format(ticker)
            df = pd.read_sql_query(request, con=connection_options)

            if len(df) == 0:
                print("Skipping ",ticker)
                continue

            df.set_index(pd.DatetimeIndex(df['date']), inplace=True)
            df.drop('date', axis=1, inplace=True)
            df.sort_index(ascending=True, inplace=True)

            df['high_52w'] = df['close'].rolling(window=250).max()
            df['low_52w'] = df['close'].rolling(window=250).min()

            df['high_100d'] = df['close'].rolling(window=100).max()
            df['low_100d'] = df['close'].rolling(window=100).min()

            df['high_30d'] = df['close'].rolling(window=30).max()
            df['low_30d'] = df['close'].rolling(window=30).min()

            df['sma_50'] = round(df['close'].rolling(window=50).mean(), 2)


            df.reset_index(inplace=True)
            df['date'] = pd.to_datetime(df['date'], format='%Y-%m-%d').dt.date

            connection_options.execute("delete from underlying_data where symbol = '{0}'".format(ticker))
            df.to_sql('underlying_data', connection_options, if_exists='append', index=False)
            i+=1
            print("Updated {0} | {1} Prices | {2} high/lows | {3}/{4}".format(ticker, len(df), df['high_52w'].count(), i, len(tickers)))

        connection_options.dispose()


file_data_path = 'C:\\Users\\Yaos\\Desktop\\Trading\\OptionsData\\'

class DataManager():

    def loadETF(self):
        df_ETF = pd.read_csv(file_data_path + 'ETF_data.csv', encoding='ISO-8859-1', na_values=['#NAME?', 'NaN', 'Infinity', '-Infinity'])
        connection = create_engine('postgresql://postgres:inkstain@localhost:5432/wzyy_options')

        try:
            df_ETF.to_sql('etf_data', connection, if_exists='append', index=False)
            print("Uploaded etf_data| {0} Recs".format(len(df_ETF)))

        except Exception as e:
            print("ERROR etf_data")
            raise

        finally:
            connection.dispose()


    def calculateMA(self, df):
        df.sort_index(inplace=True)
        df['sma_200'] = round(df['close'].rolling(window=200).mean(), 2)
        df['sma_100'] = round(df['close'].rolling(window=100).mean(), 2)
        df['sma_50'] = round(df['close'].rolling(window=50).mean(), 2)
        df['ema_8'] = round(df['close'].ewm(span=8, adjust=False).mean(), 2)

        df['high_52w'] = df['close'].rolling(window=250).max()
        df['low_52w'] = df['close'].rolling(window=250).min()

        df['high_100d'] = df['close'].rolling(window=100).max()
        df['low_100d'] = df['close'].rolling(window=100).min()

        df['high_30d'] = df['close'].rolling(window=30).max()
        df['low_30d'] = df['close'].rolling(window=30).min()



        print("{0}| {1} Prices| {2} SMA-200| {3} 100-SMA:{3}| {4} EMA-8".format(df['symbol'][0], len(df), df['sma_200'].count(), df['sma_100'].count(), df['ema_8'].count()))
        return df

    def fetchUnderlyingMS(self, ticker, date_length='compact'):
        #        f = web.DataReader(ticker,'robinhood')
        # end = date
        # start = date - timedelta(days=10)
        # # newstart = date = timedelta(days = (365 * 5))
        # newstart = datetime(end.year - 5, 1, 1)

        start_date = datetime(2005, 1, 1).date()

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
            # PR2GF4AKJDDZ10XJ
            # TRKZ63X4EOAAL204
            # VCYRAHVIILTDWPFQ
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
        av_start = time.time()
        try:
            ms = av.data(symbol=ticker, function='D')

        except Exception as e:
            print("AlphaVantage Fetch Price: ERROR| ",ticker)
            # ADD sleep timer to not over query right away when error
            connection.dispose()
            return
        av_end = time.time()
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

        print(ticker, "DB:", len(df), " Query:", len(ms), " NEW:", len(ms_new), " Query Time: ",av_end-av_start)

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
                df_upload = df_upload[df_upload['date'].dt.date >= start_date]
                df_upload['date'] = pd.to_datetime(df_upload['date'], format='%Y-%m-%d').dt.date
                # df_upload.to_csv("{0}_underlying_test.csv".format(ticker))
                process_start = time.time()
                df_upload.to_sql('underlying_data', connection, if_exists='append', index=False)
                process_end = time.time()
                print("{0}| Update {1} prices {2} to {3}. Process Time: {4}".format(ticker, len(ms_new),df_upload['date'].min(),df_upload['date'].max(),process_end-process_start))

                # count_start = time.time()
                # result = connection.execute("select count(DISTINCT symbol) from underlying_data")
                # ticker_count = result.fetchone()[0]
                # count_end = time.time()

                # print("Total Tickers: ",ticker_count, count_end - count_start)

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

    # existingTickers.update(existingTickers)


    # try:
        # connection.execute("DROP INDEX IF EXISTS underlying_symbol_index;")
    # finally:
        connection = create_engine('postgresql://postgres:inkstain@localhost:5432/wzyy_options')
        dm = DataManager()
        test = dm.fetchUnderlyingMS("AAAP", date_length='full')
        # existingTickers.fetchAllPrices(existingTickers)
        # existingTickers.update(existingTickers)
        # dm.loadETF()

        # connection.execute("CREATE INDEX underlying_symbol_index ON option_data (symbol);")
        connection.dispose()

    # test = dm.fetchUnderlyingMS("AAAP")
