# -*- coding: utf-8 -*-
"""
Created on Sat Jun 23 10:53:35 2018

@author: Yaos
"""
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

import psutil

from multiprocessing import Pool
import multiprocessing
from multiprocessing import Process

from Option_Stats import OptionStats

file_dir = os.path.dirname(__file__)
sys.path.append(file_dir)
process = psutil.Process(os.getpid())

DATA_PATH = 'C:\\Users\\Yaos\\Desktop\\Trading\\OptionsData\\Data\\'
today = str(dt.date.today()).replace('-', '')

logger = logging.getLogger(__name__)
if not len(logger.handlers):
    formatter = logging.Formatter('%(asctime)s %(filename)s: %(funcName)s: %(message)s')
    logger.setLevel(logging.DEBUG)
    fh = logging.FileHandler('Load_Option_' + today + '.log', mode='a')
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(formatter)
    logger.addHandler(fh)


#    @
class DataLoader():
    def checkFileProcessed(self, filename):
        connection_info = create_engine('postgresql://postgres:inkstain@localhost:5432/wzyy_options')
        try:
            fc = connection_info.execute("select * from process_log where source_file = '{0}'".format(filename)).fetchone()
            connection_info.dispose()
        except Exception as e:
            print("Query Log Query ERROR: ", filename, e)
            connection_info.dispose()
            return True

        if not pd.isna(fc):
            print("{0} was processed on {1}: {2} records {3} tickers".format(filename, fc['date'], fc['record_count'], fc['ticker_count']))
            return True
        else:
            return False

    def format_file(self, filename, df):
        # print("FORMAT FILE: ", df)
        format_start = time.time()

        try:
            init_df = len(df)

            if len(df) == 0:
                print("No Data in file. Returning loop")

            # FORMAT FILE
            df['date'] = pd.to_datetime(df['date'], format="%Y-%m-%d").dt.date
            dataDate = df['date'].iloc[0]

            df['option_expiration'] = pd.to_datetime(df['option_expiration'], format="%Y-%m-%d").dt.date
            df['call_put'] = df['call_put'].apply(lambda x: "C" if x == "call" else "P")
            # df['option_symbol'] = df.apply(lambda x: x['symbol'].ljust(6) + x['option_expiration'].strftime('%y%m%d') + x['call_put'] + str(int(x['strike'] * 1000)).zfill(8), axis=1)
            df['option_symbol'] =df['symbol'].apply(lambda x: x.ljust(6)) + df['option_expiration'].apply(lambda x: x.strftime('%y%m%d')) + df['call_put'] + df['strike'].apply(lambda x: str(int(x * 1000)).zfill(8))


            # DROP DUPLICATE OPTION_SYMBOLS
            df = df.drop_duplicates(subset=['option_symbol'], keep='first')
            print("{0} | Pre: {1} Post: {2}| {3} Dups".format(dataDate, init_df, len(df), len(df) - init_df))

            # CHECK DATA FOR OTHER DATES
            df_extra = df[df['date'] != dataDate]
            if len(df_extra) > 0:
                print("Other Date Records in {0}: {1}, Expecting {2}".format(filename, len(df_extra), dataDate))
                logger.info("Other Date Records in {0}: {1}, Expecting {2}".format(filename, len(df_extra), dataDate))
                print(df_extra[['date', 'symbol']].groupby(['date'])['symbol'].count().reset_index(name='count'))
                logger.info(df_extra[['date', 'symbol']].groupby(['date'])['symbol'].count().reset_index(name='count'))

                df_extra.to_csv("{0}_DateDups.csv".format(filename))


            format_end = time.time()
            print("{0} | Formatting | {1}".format(filename, format_end - format_start))
            logger.info("{0} | Formatting | {1}".format(filename, format_end - format_start))

        except Exception as e:
            format_end = time.time()
            print("{0} | ERROR Formatting | {1} | {2}".format(filename, e, format_end - format_start))
            logger.info("{0} | ERROR Formatting | {1} | {2}".format(filename, e, format_end - format_start))
            raise

        finally:
            return df


    def readFullCSV(self, filename, tickers = []):

        print("{0} | Reading File | Filter Tickers: {1}".format(filename, len(tickers)))
        read_start = time.time()

        try:

            usecols = ['Symbol', 'ExpirationDate', 'AskPrice', 'BidPrice', 'LastPrice', 'PutCall', 'StrikePrice', 'Volume', 'ImpliedVolatility', 'Delta', 'Gamma', 'Vega', 'OpenInterest', 'UnderlyingPrice', 'DataDate']

            dtypes = {'Symbol': pd.np.str_,
                      'ExpirationDate': pd.np.str_,
                      'AskPrice': pd.np.float16,
                      'BidPrice': pd.np.float16,
                      'LastPrice': pd.np.float16,
                      'PutCall': pd.np.str_,
                      'StrikePrice': pd.np.float16,
                      'Volume': pd.np.int32,
                      'OpenInterest': pd.np.int32,
                      'UnderlyingPrice': pd.np.float16,
                      'ImpliedVolatility': pd.np.float16,
                      'Delta': pd.np.float16,
                      'Gamma': pd.np.float16,
                      'Vega': pd.np.float16,
                      'DataDate': pd.np.str_,
                      }
            df = pd.read_csv(DATA_PATH + '{0}'.format(str(filename)), encoding='ISO-8859-1', usecols=usecols, dtype=dtypes, na_values=['#NAME?','NaN','Infinity','-Infinity'])

            # RENAME COLUMNS
            df_rename = {"Symbol": "symbol", "ExpirationDate": "option_expiration", "DataDate": "date", "AskPrice": "ask", "BidPrice": "bid", "LastPrice": "last",
                         "PutCall": "call_put", "StrikePrice": "strike", "Volume": "volume", "OpenInterest": "open_interest", "UnderlyingPrice": "stock_price",
                         "ImpliedVolatility": "iv", "Delta": "delta", "Gamma": "gamma", "Vega": "vega"}
            df.rename(columns=df_rename, inplace=True)

        # FILTER DATA FOR TICKERS
            if len(tickers) > 0:
                df = df[df["symbol"].isin(tickers)]
            # print("Filtered {0} for {1} Tickers: {2} Records MEM: {3} MB".format(filename, len(tickers), len(df), (process.memory_info().rss / 1048576)))
            logger.info("Filtered {0} for {1} Tickers: {2} Records MEM: {3} MB".format(filename, len(tickers), len(df), (process.memory_info().rss / 1048576)))

            read_end = time.time()
            print("{0} | {1} Recs | Filter Tickers: {2} | {3}".format(filename, len(df),len(tickers), read_end - read_start))
            logger.info("{0} | {1} Recs | Filter Tickers: {2} | {3}".format(filename, len(df),len(tickers), read_end - read_start))


        except Exception as e:
            read_end = time.time()
            print("{0} | READ ERROR | Filter Tickers: {1} | {2} | {3}".format(filename, len(tickers), e, read_end - read_start))
            logger.info("{0} | READ ERROR | Filter Tickers: {1} | {2} | {3}".format(filename, len(tickers), e, read_end - read_start))
            raise

        finally:
            return  df

    def readOICSV(self, filename, tickers=[]):
        read_start = time.time()
        print("{0} | Reading File | Filter Tickers: {1}".format(filename, len(tickers)))

        try:
            usecols = ['Symbol', 'ExpirationDate', 'PutCall', 'StrikePrice', 'Volume', 'OpenInterest','DataDate']

            dtypes = {'Symbol': pd.np.str_,
                      'ExpirationDate': pd.np.str_,
                      'PutCall': pd.np.str_,
                      'StrikePrice': pd.np.float16,
                      'Volume': pd.np.int32,
                      'OpenInterest': pd.np.int32,
                      'DataDate': pd.np.str_
                    }
            df = pd.read_csv(DATA_PATH + '{0}'.format(str(filename)), encoding='ISO-8859-1', usecols=usecols, dtype=dtypes, na_values=['#NAME?','NaN','Infinity','-Infinity'])

            df_next_rename = {"Symbol": "symbol", "ExpirationDate": "option_expiration", "DataDate": "date",
                              "PutCall": "call_put", "StrikePrice": "strike", "Volume": "volume", "OpenInterest": "open_interest"}
            df.rename(columns=df_next_rename, inplace=True)

            # FILTER DATA FOR TICKERS
            if len(tickers) > 0:
                df = df[df["symbol"].isin(tickers)]
            # print("Filtered {0} for {1} Tickers: {2} Records MEM: {3} MB".format(filename, len(tickers), len(df), (process.memory_info().rss / 1048576)))
            logger.info("Filtered {0} for {1} Tickers: {2} Records MEM: {3} MB".format(filename, len(tickers), len(df), (process.memory_info().rss / 1048576)))

            read_end = time.time()
            print("{0} | OI-Only| {1} Recs | Filter Tickers: {2} | {3}".format(filename, len(df), len(tickers), read_end - read_start))
            logger.info("{0} | OI-Only| {1} Recs | Filter Tickers: {2} | {3}".format(filename, len(df), len(tickers), read_end - read_start))


        except Exception as e:
            read_end = time.time()
            print("{0} | OI-Only | READ ERROR | Filter Tickers: {1} | {2} | {3}".format(filename, len(tickers), e, read_end - read_start))
            logger.info("{0} | OI-Only | READ ERROR | Filter Tickers: {1} | {2} | {3}".format(filename, len(tickers),e, read_end - read_start))
            raise

        finally:
            return df


    def checkExpiry(self, df):
    # READ IN EXPIRY
        Add = 0
        dataDate = df['date'].iloc[0]
        try:
            connection = create_engine('postgresql://postgres:inkstain@localhost:5432/wzyy_options')
            connection_data = create_engine('postgresql://postgres:inkstain@localhost:5432/option_data')

            request = "select * from option_expiry_table order by option_expiration asc"
            expiry = pd.read_sql_query(request, con=connection)
            # if len(expiry) > 0:
            #     expiry['option_expiration'] = expiry['option_expiration'].apply(lambda x: x.date())
            unique_expiries = df['option_expiration'].drop_duplicates()
            print("{0} | {1} Expiries | {2} | {3}".format(dataDate, len(unique_expiries), unique_expiries.min(), unique_expiries.max()))

        except Exception as e:
            print("{0} | Read Expiry Error | {1}".format(dataDate,e))
            print("{0} | Read Expiry Error | {1}".format(dataDate,e))
            connection.dispose()
            connection_data.dispose()

            # new_expiries = len(unique_expiries[unique_expiries not in expiry['option_expiration'].values])

        try:
            for exp_date in unique_expiries:
                if exp_date not in expiry['option_expiration'].values:

                        table_name = "data_" + exp_date.strftime('%m_%d_%Y')

                    # CREATE NEW EXPIRY OPTION DATA TABLE
                        connection_data.execute("select exists(select * from information_schema.tables where table_name= '{0}')".format(table_name))
                        exists = connection_data.execute("select exists(select * from information_schema.tables where table_name= '{0}')".format(table_name)).fetchone()[0]

                        if exists:
                            print("{0} Already Exists".format(table_name))
                        else:
                            connection_data.execute("create table {0} (like option_data_init including all)".format(table_name))
                            print("{0} | CREATE NEW EXPIRY TABLE | {1} | {2}".format(dataDate, exp_date, table_name))
                            logger.info("{0} | CREATE NEW EXPIRY TABLE | {1} | {2}".format(dataDate, exp_date, table_name))
                            Add += 1

                    # ADD EXPIRY TO EXPIRY TABLE
                        cur_date = dataDate
                        connection.execute("insert into option_expiry_table (option_expiration, table_name, added_data_date) values  ('{0}','{1}','{2}')".format(exp_date, table_name, dataDate))
                        print("{0} | ADD TO OPTION EXPIRY TABLE | {1} | {2}".format(dataDate,exp_date,table_name))
                        logger.info("{0} | ADD TO OPTION EXPIRY TABLE | {1} | {2}".format(dataDate,exp_date,table_name))


            print("{0} | Added {1} Expiration Dates".format(dataDate, Add))
            logger.info("{0} | Added {1} Expiration Dates".format(dataDate, Add))
            return len(unique_expiries),unique_expiries.min(), unique_expiries.max()

        except Exception as e:
            print("{0} | NEW EXPIRY TABLE ERROR | {1} | {2} | {3}".format(dataDate,exp_date,table_name,e))
            logger.info("{0} | NEW EXPIRY TABLE ERROR | {1} | {2} | {3}".format(dataDate,exp_date,table_name,e))
            return

        finally:
            connection.dispose()
            connection_data.dispose()



    def checkTickerTables(self, df):
        #  Check Tickers
        checkTicker_start = time.time()
        existing_tickers = existingTickers().all(option="all")
        existing_tickers_symbols = existing_tickers['symbol'].values
        existing_tickers.set_index('symbol', inplace=True)
        dataDate = df['date'].iloc[0]
        connection_info = create_engine('postgresql://postgres:inkstain@localhost:5432/wzyy_options')

        try:
            print("{0} | {1} Tickers In File| {2} In Database".format(dataDate.strftime('%m/%d/%Y'), len(df), len(existing_tickers_symbols)))
            logger.info("{0} | {1} Tickers In File| {2} In Database".format(dataDate.strftime('%m/%d/%Y'), len(df), len(existing_tickers_symbols)))

            Add = 0
            Remove = 0

        # LOOP THROUGH DATA, CHECK FOR NEW TICKERS
            for index, row in df.iterrows():
                ticker = row['symbol']
            # ADD NEW TICKERS
                if ticker not in existing_tickers_symbols:
                    print("NEW TICKER: {0}".format(ticker))
                    connection_info.execute("insert into ticker_log (symbol, start_date) values  ('{0}','{1}')".format(ticker, dataDate))

                    print("{0} | New Ticker| Add Ticker Log".format(ticker))
                    logger.info("{0} | New Ticker| Add Ticker Log".format(ticker))
                    Add += 1

            # UPDATE END_DATE TO NULL SINCE STILL EXIST PAST END_DATE
                if ticker in existing_tickers_symbols:
                    end_date = existing_tickers.loc[ticker,'end_date']
                    if (end_date is not None):
                        if (end_date < dataDate):
                            # print("{0} | Update End_date | NULL".format(ticker))
                            logger.info("{0} | Update End_date | NULL".format(ticker))
                            connection_info.execute("UPDATE ticker_log set end_date = NULL where symbol = '{1}'".format(dataDate, ticker))
            # print("{0} | Added {1} Tickers".format(dataDate, Add))

         # LOOP THROUGH EXISTING TICKERS, CHECK FOR TICKERS THAT DISAPPEAR
            for index, row in existing_tickers.iterrows():
                ticker = index

            # TICKER DOESN'T EXIST ANYMORE. ADD END DATE
                if ticker not in df['symbol']:
                    if row['end_date'] is None:
                        # print("{0} | Update End_date | {1}".format(ticker, dataDate))
                        logger.info("{0} | Update End_date | {1}".format(ticker, dataDate))
                        connection_info.execute("UPDATE ticker_log set end_date = '{0}' where symbol = '{1}'".format(dataDate, ticker))
                        Remove += 1

                    # TICKER DOESN'T EXIST EARLIER THAN CURRENT END_DATE
                    if row['end_date'] > dataDate:
                        # print("Update ticker_log {0}: {1} End Date".format(ticker, dataDate))
                        logger.info("Update ticker_log {0}: {1} End Date".format(ticker, dataDate))
                        connection_info.execute("UPDATE ticker_log set end_date = '{0}' where symbol = '{1}'".format(dataDate, ticker))

            # print("{0} | Removed {1} Tickers".format(dataDate, Remove))

            checkTicker_end = time.time()
            print("{0} | Checked Tickers | {1} In File | {2} Add | {3} End | {4}".format(dataDate, len(df), Add, Remove, checkTicker_end - checkTicker_start))
            logger.info("{0} | Checked Tickers | {1} In File | {2} Add | {3} End | {4}".format(dataDate, len(df), Add, Remove, checkTicker_end - checkTicker_start))

        except Exception as e:
            checkTicker_end = time.time()
            print("{0} | Check Ticker Error | {1} Tickers | {2} | {3}".format(dataDate, len(df),e, checkTicker_end - checkTicker_start))
            logger.info("{0} | Check Ticker Error | {1} Tickers | {2} | {3}".format(dataDate, len(df),e, checkTicker_end - checkTicker_start))
            raise

        finally:
            connection_info.dispose()

        # Fetch Prices for New Underlying
        #     DataManager.fetchUnderlyingMS(DataManager(), ticker, date_length='full')


    def mergeOI(self, df, df_merge_inp, fieldname):
        merge_start = time.time()
        dataDate = df['date'].iloc[0].strftime('%y-%m-%d')
        dataDate_merge = df_merge_inp['date'].iloc[0].strftime('%y-%m-%d')

        zero_OI_count = len(df) - df['open_interest'].astype(bool).sum(axis=0)
        merge_zero_OI_count = len(df_merge_inp) - df_merge_inp['open_interest'].astype(bool).sum(axis=0)

        print("{0} | {1} Rec | {2} No OI | Merging | {3} | {4} Rec | {5} No OI".format(dataDate, len(df), zero_OI_count, dataDate_merge, len(df_merge_inp), merge_zero_OI_count))
        logger.info("{0} | {1} Rec | {2} No OI | Merging | {3} | {4} Rec | {5} No OI".format(dataDate, len(df), zero_OI_count, dataDate_merge, len(df_merge_inp), merge_zero_OI_count))

        try:
            df_merge = df_merge_inp[df_merge_inp['open_interest'] > 0]
            df_merge = df_merge_inp[['option_symbol', 'open_interest']]

            df_merge.columns = ["option_symbol", fieldname]

            df.set_index("option_symbol",inplace=True)
            df_merge.set_index("option_symbol",inplace=True)

            df = pd.merge(df, df_merge, how='left', on=['option_symbol'])
            df[fieldname].fillna(0, inplace=True)
            df.reset_index(inplace=True)

            zero_newOI_count = len(df) - df[fieldname].astype(bool).sum(axis=0)
            merge_end = time.time()
            print("{0} - {1} | Merge Success | {2} Recs {3} Zero OI | {4} Zero Merge OI | {5}".format(dataDate, fieldname, len(df), zero_OI_count, zero_newOI_count, merge_end - merge_start))
            logger.info("{0} - {1} | Merge Success | {2} Recs {3} Zero OI | {4} Zero Merge OI | {5}".format(dataDate, fieldname, len(df), zero_OI_count, zero_newOI_count, merge_end - merge_start))

        except Exception as e:
            merge_end = time.time()
            print("{0} | Merge Error | {1} | {2} | {3}".format(dataDate, dataDate_merge,e, merge_end - merge_start))
            logger.info("{0} | Merge Error | {1} | {2} | {3}".format(dataDate, dataDate_merge,e, merge_end - merge_start))
            raise

        finally:
            return df

    def checkOI(self, df, OI_field, checkOI_field):
        # CHECK FOR DATA HOLES
        checkOI_start = time.time()
        dataDate = df['date'].iloc[0].strftime('%y-%m-%d')
        hole_rec_count = 0
        hole_ticker_count = 0

        try:

            # Volume check is to make sure it wasn't used to fully close out OI and that 0 OI was legit
            # Checks if OI or NEW_OI = 0. Filters for volume < 50% OI to screen out actual closings or openings

            # df['data_holes'] = df.apply(lambda x: 1 if (x['volume'] < (0.5 * max(x[OI_field], x[checkOI_field]))) & ((x[OI_field] == 0) | (x[checkOI_field] == 0)) else 0, axis=1)
            # dataHoles = df[df['data_holes'] == 1]

            cond1 = df[OI_field] == 0
            cond2 = df[checkOI_field] == 0
            cond3 = df["volume"] < 0.5 * df[[OI_field, checkOI_field]].max(axis=1)
            cond4 = df["option_expiration"] > df["date"]

            dataHoles = df[cond3 & (cond1 | cond2) & cond4]
            # print("DATA HOLES: ", len(dataHoles))
            OI_hole = 0
            check_OI_hole = 0

            if len(dataHoles) > 0:
                hole_rec_count = len(dataHoles)
                hole_ticker_count = dataHoles['symbol'].nunique()
                # EXPORT DATA HOLES FILE
                # datahole_file = 'Data_Holes.csv'
                # if os.path.isfile(datahole_file):
                #     dataHoles.to_csv(datahole_file, mode='a', header=False)
                # else:
                #     dataHoles.to_csv(datahole_file, header=True)
                #     print("Create {0}".format(datahole_file))
                #     logger.info("Create {0}".format(datahole_file))

                print("Update Prev OI - Data Holes: {0} {1} | {2} Recs | {3} Tickers".format(ticker, dataDate, hole_rec_count, hole_ticker_count))

                # UPDATE DATA HOLES IN CURRENT DATA
                for row in dataHoles.itertuples():

                    # Update Hole in Current Day Data
                    df_idx = df.index[df['option_symbol'] == row.option_symbol].tolist()
                    for index in df_idx:
                        max_OI = max(row.open_interest, row.open_interest_new)

                        # UPDATE NEW OPEN INTEREST IF 0 TO CURRENT OI (ASSUME NO CHANGE)
                        if row.open_interest_new == 0:
                            orig_oi = df.at[index, checkOI_field]
                            df.at[index, checkOI_field] = max_OI
                            new_oi = df.at[index, checkOI_field]
                            logger.info("{0}| {1} | {2} UPDATE| {3} To {4}".format(row.option_symbol, row.date, checkOI_field, orig_oi, new_oi))
                            check_OI_hole += 1

                        # UPDATES CURRENT OPEN INTEREST IF 0.
                        if row.open_interest == 0:
                            orig_oi = df.at[index, OI_field]
                            df.at[index, OI_field] = max_OI
                            new_oi = df.at[index, OI_field]
                            logger.info("{0}| {1} | {2} UPDATE| {3} To {4}".format(row.option_symbol, row.date, OI_field, orig_oi, new_oi))
                            OI_hole += 1

                        # DIFFERENCE SMALL ENOUGH TO NOT WARRANT THE ADDITIONAL PERFORMANCE DRAG

            hole_rec_count = len(dataHoles)
            hole_ticker_count = dataHoles['symbol'].nunique()
            zero_OI_count = len(df) - df[OI_field].astype(bool).sum(axis=0)
            zero_check_OI_count = len(df) - df[checkOI_field].astype(bool).sum(axis=0)
            checkOI_end = time.time()
            print("{0} | Total DataHoles | {1} Rec | {2} Tickers".format(dataDate, hole_rec_count, hole_ticker_count))
            logger.info("{0} | Total DataHoles | {1} Rec | {2} Tickers".format(dataDate, hole_rec_count, hole_ticker_count))

            print("{0} | {1} Rec | OI Fill {2} | New OI Fill {3} | {4} No OI | {5} No New OI | {6}".format(dataDate, len(df), OI_hole, check_OI_hole, zero_OI_count, zero_check_OI_count, checkOI_end - checkOI_start))
            logger.info("{0} | {1} Rec | OI Fill {2} | New OI Fill {3} | {4} No OI | {5} No New OI | {6}".format(dataDate, len(df), OI_hole, check_OI_hole, zero_OI_count, zero_check_OI_count, checkOI_end - checkOI_start))

        except Exception as e:
            checkOI_end = time.time()
            print("{0} | Data Hole Error | {1} | {2}".format(dataDate.strftime('%Y-%m-%d'), checkOI_end - checkOI_start, e))
            logger.info("{0} | Data Hole Error | {1} | {2]".format(dataDate.strftime('%Y-%m-%d'), checkOI_end - checkOI_start, e))
            raise

        finally:
            # df.drop(columns='data_holes', inplace=True)
            return df, hole_rec_count, hole_ticker_count

    def checkOINew(self, df, OI_field, checkOI_field):
        # CHECK FOR DATA HOLES
        checkOI_start = time.time()
        dataDate = df['date'].iloc[0].strftime('%y-%m-%d')
        hole_rec_count = 0
        hole_ticker_count = 0

        try:

            # Volume check is to make sure it wasn't used to fully close out OI and that 0 OI was legit
            # Checks if OI or NEW_OI = 0. Filters for volume < 50% OI to screen out actual closings or openings

            # df['data_holes'] = df.apply(lambda x: 1 if (x['volume'] < (0.5 * max(x[OI_field], x[checkOI_field]))) & ((x[OI_field] == 0) | (x[checkOI_field] == 0)) else 0, axis=1)
            # dataHoles = df[df['data_holes'] == 1]

            cond1 = df[OI_field] == 0
            cond2 = df[checkOI_field] == 0
            cond3 = df["volume"] < 0.5 * df[[OI_field, checkOI_field]].max(axis=1)
            cond4 = df["option_expiration"] > df["date"]

            df["dataHole"] = (cond3 & (cond1 | cond2) & cond4)
            df['max_oi'] = df[[OI_field, checkOI_field]].max(axis=1)

            pre_OI_sum = df[OI_field].sum()
            pre_OI_check_sum = df[checkOI_field].sum()

            dataHoles = df[df["dataHole"]==True]
            OI_hole = len(dataHoles[dataHoles[OI_field]==0])
            check_OI_hole = len(dataHoles[dataHoles[checkOI_field]==0])
            hole_rec_count = len(dataHoles)
            hole_ticker_count = dataHoles['symbol'].nunique()

            df[OI_field] = df.apply(lambda x: int(x['max_oi']) if ((x["dataHole"]==True) & (x[OI_field] == 0)) else int(x[OI_field]), axis=1)
            df[checkOI_field] = df.apply(lambda x: int(x['max_oi']) if ((x["dataHole"]==True) & (x[checkOI_field] == 0)) else int(x[checkOI_field]), axis=1)

            post_OI_sum = df[OI_field].sum()
            post_OI_check_sum = df[checkOI_field].sum()

            zero_OI_count = len(df) - df[OI_field].astype(bool).sum(axis=0)
            zero_check_OI_count = len(df) - df[checkOI_field].astype(bool).sum(axis=0)
            checkOI_end = time.time()

            print("{0} | Total DataHoles | {1} Rec | {2} Tickers | OI {3} to {4}: {5} | New OI {6} to {7}: {8} ".format(dataDate, hole_rec_count, hole_ticker_count,pre_OI_sum, post_OI_sum,post_OI_sum-pre_OI_sum, pre_OI_check_sum,
                                                                                                                        post_OI_check_sum, post_OI_check_sum - pre_OI_check_sum))
            logger.info("{0} | Total DataHoles | {1} Rec | {2} Tickers | OI {3} to {4}: {5} | New OI {6} to {7}: {8} ".format(dataDate, hole_rec_count, hole_ticker_count,pre_OI_sum, post_OI_sum,post_OI_sum-pre_OI_sum, pre_OI_check_sum,
                                                                                                                        post_OI_check_sum, post_OI_check_sum - pre_OI_check_sum))

            print("{0} | {1} Rec | OI Fill {2} | New OI Fill {3} | {4} No OI | {5} No New OI | {6}".format(dataDate, len(df), OI_hole, check_OI_hole, zero_OI_count, zero_check_OI_count, checkOI_end - checkOI_start))
            logger.info("{0} | {1} Rec | OI Fill {2} | New OI Fill {3} | {4} No OI | {5} No New OI | {6}".format(dataDate, len(df), OI_hole, check_OI_hole, zero_OI_count, zero_check_OI_count, checkOI_end - checkOI_start))

        except Exception as e:
            checkOI_end = time.time()
            print("{0} | Data Hole Error | {1} | {2}".format(dataDate.strftime('%Y-%m-%d'), checkOI_end - checkOI_start, e))
            logger.info("{0} | Data Hole Error | {1} | {2]".format(dataDate.strftime('%Y-%m-%d'), checkOI_end - checkOI_start, e))
            raise

        finally:
            df.drop(columns=['dataHole','max_oi'], inplace=True)
            return df, hole_rec_count, hole_ticker_count


    def uploadCopy(self, df):
        upload_start = time.time()
        dataDate = df['date'].iloc[0].strftime('%y-%m-%d')
        conn = psycopg2.connect("dbname = 'option_data' user='postgres' host = 'localhost' password = 'inkstain'")
        cur = conn.cursor()
        try:

        # REARRANGE COLUMNS FOR UPLOAD COPY
            cols = ["date","symbol","stock_price",'option_symbol','option_expiration','strike','call_put','bid','ask','last','volume','open_interest']
            cols += ['open_interest_new','open_interest_change','open_interest_5day','open_interest_5day_change','iv','delta','gamma','vega']
            df = df[cols]
            # df['date'] = df['date'].apply(lambda x: x.strftime('%Y-%m-%d'))
            # df['option_expiration'] = df['option_expiration'].apply(lambda x: x.strftime('%Y-%m-%d'))

            df.to_csv('temp_upload.csv', index=False, header=False)
            f = open('temp_upload.csv', 'r')

            cur.copy_from(f, 'option_data', sep=',', null="")
            f.close()
            os.remove('temp_upload.csv')

            upload_end = time.time()
            print("{0} | Upload {1} Recs | {2}".format(dataDate, len(df),upload_end - upload_start))
            logger.info("{0} | Upload {1} Recs | {2}".format(dataDate, len(df), upload_end - upload_start))

        except Exception as e:
            upload_end = time.time()
            print("{0} | Upload ERROR | {1} | {2}".format(dataDate, e, upload_end - upload_start))
            logger.info("{0} | Upload ERROR | {1} | {2}".format(dataDate, e, upload_end - upload_start))
            raise

        finally:
            cur.close()
            conn.commit()
            conn.close()


    def uploadTest(self, df):
        upload_start = time.time()
        dataDate = df['date'].iloc[0].strftime('%y-%m-%d')
        conn = psycopg2.connect("dbname = 'option_data' user='postgres' host = 'localhost' password = 'inkstain'")
        cur = conn.cursor()
        try:

        # REARRANGE COLUMNS FOR UPLOAD COPY
            cols = ["date","symbol","stock_price",'option_symbol','option_expiration','strike','call_put','bid','ask','last','volume','open_interest']
            cols += ['open_interest_new','open_interest_change','open_interest_5day','open_interest_5day_change','iv','delta','gamma','vega']
            df = df[cols]
            # df['date'] = df['date'].apply(lambda x: x.strftime('%Y-%m-%d'))
            # df['option_expiration'] = df['option_expiration'].apply(lambda x: x.strftime('%Y-%m-%d'))

            df.to_csv('temp_upload.csv', index=False, header=False)
            f = open('temp_upload.csv', 'r')

            cur.copy_from(f, 'option_data', sep=',', null="")
            f.close()
            os.remove('temp_upload.csv')

            upload_end = time.time()
            print("{0} | Upload {1} Recs | {2}".format(dataDate, len(df),upload_end - upload_start))
            logger.info("{0} | Upload {1} Recs | {2}".format(dataDate, len(df), upload_end - upload_start))

        except Exception as e:
            upload_end = time.time()
            print("{0} | Upload ERROR | {1} | {2}".format(dataDate, e, upload_end - upload_start))
            logger.info("{0} | Upload ERROR | {1} | {2}".format(dataDate, e, upload_end - upload_start))
            raise

        finally:
            cur.close()
            conn.commit()
            conn.close()

    def uploadOptionStatTest(self, df):
        upload_start = time.time()
        dataDate = df['date'].iloc[0]
        connection_stats = create_engine('postgresql://postgres:inkstain@localhost:5432/wzyy_options')

        try:
            stats = pd.DataFrame()
            for name, group in df.groupby('symbol'):
                stats = stats.append(OptionStats(name, dataDate, group).result, sort=True)

            # print(stats.head())
            stats.to_sql('option_stat', connection_stats, if_exists='append', index=False)
            upload_end = time.time()
            print("{0} | Uploaded option_stat| {1} Recs | {2}".format(dataDate, len(stats), upload_end - upload_start))
            logger.info("{0} | Uploaded option_stat| {1} Recs | {2}".format(dataDate, len(stats), upload_end - upload_start))

        except Exception as e:
            upload_end = time.time()
            print("{0} | ERROR option_stat| {1} | {2}".format(dataDate, e, upload_end - upload_start))
            logger.info("{0} | ERROR option_stat| {1} | {2}".format(dataDate, e, upload_end - upload_start))
            raise

        finally:
            connection_stats.dispose()



    def uploadTickerExpiryTest(self, df):
        upload_start = time.time()
        if df['option_expiration'].nunique() > 1:
            print("More than one expiration")
            return

        dataDate = df['date'].iloc[0].strftime('%y-%m-%d')
        tableName = "data_" + df['option_expiration'].iloc[0].strftime('%m_%d_%Y')
        conn = psycopg2.connect("dbname = 'option_data' user='postgres' host = 'localhost' password = 'inkstain'")
        cur = conn.cursor()

        # connection = create_engine('postgresql://postgres:inkstain@localhost:5432/option_data')

        try:

            cols = ["date","symbol","stock_price",'option_symbol','option_expiration','strike','call_put','bid','ask','last','volume','open_interest']
            cols += ['open_interest_new','open_interest_change','open_interest_5day','open_interest_5day_change','iv','delta','gamma','vega']
            df = df[cols]
            # df['date'] = df['date'].apply(lambda x: x.strftime('%Y-%m-%d'))
            # df['option_expiration'] = df['option_expiration'].apply(lambda x: x.strftime('%Y-%m-%d'))
            temp_file_name = 'temp_upload_{0}.csv'.format(tableName)

            df.to_csv(temp_file_name, index=False, header=False)
            f = open(temp_file_name, 'r')

            cur.copy_from(f, tableName, sep=',', null="")
            f.close()
            os.remove(temp_file_name)

            # df.to_sql(tableName, connection, if_exists='append', index=False)

            upload_end = time.time()
            print("{0} | {1} | Upload {2} Recs | {3}".format(dataDate, tableName, len(df),upload_end - upload_start))
            logger.info("{0} | {1} | Upload {2} Recs | {3}".format(dataDate, tableName, len(df),upload_end - upload_start))

        except Exception as e:
            upload_end = time.time()
            print("{0} | Upload ERROR | {1} | {2}".format(dataDate, e, upload_end - upload_start))
            logger.info("{0} | Upload ERROR | {1} | {2}".format(dataDate, e, upload_end - upload_start))
            raise

        finally:
            cur.close()
            conn.commit()
            conn.close()


#################################################################################################################################


    def loadOptionsHistoricaDoubleLoadTest(self, filename, filename_next = "", filename_5d = "", tickers = []):
        process = psutil.Process(os.getpid())
        filename = filename.replace(".zip", "")
        connection_data = create_engine('postgresql://postgres:inkstain@localhost:5432/option_data')
        connection_info = create_engine('postgresql://postgres:inkstain@localhost:5432/wzyy_options')

        file_start = time.time()

        # Check if current date file has already been processed. Skips processing if already processed.
        if self.checkFileProcessed(filename):
            print('{0} FILE WAS PROCESSED - SKIP'.format(filename))
            logger.info('{0} FILE WAS PROCESSED - SKIP'.format(filename))
            return

        logger.info("Before Extract File: {0} MEM:{1} MB".format(filename, (process.memory_info().rss / 1048576)))

        # CREATE TICKER LOG
        process_log_cols = ['source_date', 'source_file', 'date', 'record_count', 'ticker_count', 'expiry_count', 'min_expiry','max_expiry','prev_oi_update', 'prev_oi_update_file', 'prev_oi_data_hole_rec_count', 'prev_oi_data_hole_ticker_count',
                            'prev_5day_oi_update', 'prev_5day_oi_update_file', 'upload_date', 'stat_date', 'flag_date', 'process_time']
        process_log = pd.DataFrame(columns=process_log_cols, index=[])
        process_log.loc[len(process_log)] = np.repeat(np.nan, len(process_log_cols))
        process_log['date'] = dt.datetime.now()

    # READ CURRENT FILE
        load_start = time.time()
        df = self.readFullCSV(filename,tickers)
        df = self.format_file(filename, df)

    # CHECK DATA FOR NEW TICKERS
        unique_tickers = df[['symbol', 'date']].drop_duplicates('symbol')
        unique_tickers['symbol'] = unique_tickers['symbol'].apply(lambda x: x.replace('/', '_'))
        self.checkTickerTables(unique_tickers)

        dataDate = df['date'].iloc[0]
        process_log['source_date'] = dataDate
        process_log['source_file'] = filename
        process_log['ticker_count'] = len(unique_tickers)


        load_end = time.time()
        print("{0} | FINISH | Loading {1} | {2}".format(filename,filename,load_end-load_start))

    # CHECK DATA FOR NEW EXPIRIES
        process_log['expiry_count'],process_log['min_expiry'],process_log['max_expiry'] = self.checkExpiry(df)

    # READ NEXT DAY FILE
        if filename_next == "":
            print("{0} | No Next Day File".format(filename))
        else:
            nextday_start = time.time()
            df_next = self.readOICSV(filename_next,tickers)
            df_next = self.format_file(filename_next, df_next)

            df = self.mergeOI(df, df_next, "open_interest_new")
            df, hole_rec_count, hole_ticker_count = self.checkOINew(df, "open_interest", "open_interest_new")

            # df['open_interest_change'] = df.apply(lambda x: (x['open_interest_new'] - x['open_interest']) if ~math.isnan(x['open_interest_new']) else 0, axis=1)
            # df['open_interest_change'].fillna(0, inplace=True)

            df['open_interest_new'].fillna(0, inplace=True)
            df['open_interest_change'] = df['open_interest_new'] - df['open_interest']

            df['open_interest_change'] = df['open_interest_change'].apply(lambda x: int(x))
            df['open_interest_new'] = df['open_interest_new'].apply(lambda x: int(x))
            nextday_end = time.time()
            print("{0} | FINISH | Updating New OI | {1}".format(filename, nextday_end - nextday_start))

            process_log['prev_oi_data_hole_rec_count'] = hole_rec_count
            process_log['prev_oi_data_hole_ticker_count'] = hole_ticker_count
            process_log['prev_oi_update'] = dt.datetime.now()
            process_log['prev_oi_update_file'] = filename_next


    # READ PREV 5 OI FILE
        if filename_5d == "":
            print("{0} | No Prev 5 Day Day File".format(filename_5d))
        else:
            day5_start = time.time()
            df_5d = self.readOICSV(filename_5d,tickers)
            df_5d = self.format_file(filename_5d, df_5d)

            df = self.mergeOI(df, df_5d, "open_interest_5day")

            # df['open_interest_5day_change'] = df.apply(lambda x: ((x['open_interest'] - x['open_interest_5day'])) if ~math.isnan(x['open_interest_5day']) else 0, axis=1)
            # df['open_interest_5day_change'].fillna(0, inplace=True)

            df['open_interest_5day_change'] = df['open_interest'] - df['open_interest_5day'].apply(lambda x: 0 if math.isnan(x) else x)
            df['open_interest_5day'].fillna(0, inplace=True)

            df['open_interest_5day_change'] = df['open_interest_5day_change'].apply(lambda x: int(x))
            df['open_interest_5day'] = df['open_interest_5day'].apply(lambda x: int(x))

            process_log['prev_5day_oi_update'] = dt.datetime.now()
            process_log['prev_5day_oi_update_file'] = filename_5d
            day5_end = time.time()
            print("{0} | FINISH | Updating 5 Day OI | {1}".format(filename, day5_end - day5_start))


    # UPLOAD EOD DATA
    #     upload_start = time.time()
    #     self.uploadCopy(df)
    #     upload_end = time.time()
    #     print("{0} | FINISH | Upload | {1}".format(filename, upload_end - upload_start))

    # UPLOAD EOD DATA BY EXPIRY

        upload_start = time.time()
        print("{0} | Upload Starts | {1}".format(filename, process_log['expiry_count']))
        num_processes = multiprocessing.cpu_count() * 2 - 4
        chunks = [chunk[1] for chunk in df.groupby('option_expiration')]
        pool = multiprocessing.Pool(processes=num_processes)
        pool.map(self.uploadTickerExpiryTest, chunks, chunksize=1)
        pool.close()
        pool.join()

        process_log['upload_date'] = dt.datetime.now()
        process_log['record_count'] = len(df)
        process_log['ticker_count'] = len(unique_tickers)

    # UPLOAD STATS DATA
        upload_start = time.time()
        self.uploadOptionStatTest(df)
        upload_end = time.time()
        print("{0} | FINISH | Stats | {1}".format(filename, upload_end - upload_start))

        process_log['stat_date'] = dt.datetime.now()

    # UPLOAD PROCESS LOG
        file_end = time.time()
        process_log['process_time'] = file_end - file_start

        upload_start = time.time()
        try:
            process_log.to_sql('process_log', connection_info, if_exists='append', index=False)
            upload_end = time.time()
            print("{0} | FINISH | Process Log | {1}".format(filename, upload_end - upload_start))
            print("{0} | Upload process_log | {1} Data| {2} recs| {3} tickers | {4} | {5}".format(filename, df['date'].iloc[0], len(df), len(unique_tickers), pd.datetime.now().strftime('%m/%d/%Y'), file_end - file_start))
            logger.info("{0} | Upload process_log | {1} Data| {2} recs| {3} tickers | {4} | {5}".format(filename, df['date'].iloc[0], len(df), len(unique_tickers), pd.datetime.now().strftime('%m/%d/%Y'), file_end - file_start))
        except Exception as e:
            print("{0} | Upload Process Log Error | {1}".format(filename, e))
            logger.info("{0} | Upload Process Log Error | {1}".format(filename, e))
        finally:
            connection_data.dispose()
            connection_info.dispose()


    ##################################################################################################################################


    def terminateConnections(self):
        commands = ("""SELECT pg_terminate_backend(pg_stat_activity.pid)
                    FROM pg_stat_activity
                    WHERE datname = 'option_data'
                    AND pid <> pg_backend_pid();
                """)

        commands_wz = ("""SELECT pg_terminate_backend(pg_stat_activity.pid)
                    FROM pg_stat_activity
                    WHERE datname = 'wzyy_options'
                    AND pid <> pg_backend_pid();
                """)
        try:
            conn = psycopg2.connect("dbname = 'option_data' user='postgres' host = 'localhost' password = 'inkstain'")
            cur = conn.cursor()
            cur.execute(commands)
            cur.close()
            conn.commit()
            conn.close()

            conn = psycopg2.connect("dbname = 'wzyy_options' user='postgres' host = 'localhost' password = 'inkstain'")
            cur = conn.cursor()
            cur.execute(commands)
            cur.close()
            conn.commit()
            conn.close()

            print("Terminate Connections")
        except (Exception, psycopg2.DatabaseERROR) as ERROR:
            print(ERROR)


    def addOptionDataConstraint(self):
        index_start = time.time()
        connection_info = create_engine('postgresql://postgres:inkstain@localhost:5432/wzyy_options')

        try:
            connection_info.execute("alter table option_data add unique (date, option_symbol)")

        except Exception as e:
            print("Index Add ERROR: ", e)
        finally:
            connection_info.dispose()
            index_end = time.time()
            print("Remove Option Data Constraint |", index_end - index_start)

    def removeOptionDataConstraint(self):
        index_start = time.time()
        connection_info = create_engine('postgresql://postgres:inkstain@localhost:5432/wzyy_options')

        try:
            connection_info.execute("alter table option_data DROP constraint IF EXISTS option_data_date_option_symbol_key")

        except Exception as e:
            print("Index Add ERROR: ", e)
        finally:
            connection_info.dispose()
            index_end = time.time()
            print("Remove Option Data Constraint |", index_end - index_start)


    def removeOptionDataIndex(self):
        index_start = time.time()
        connection_info = create_engine('postgresql://postgres:inkstain@localhost:5432/wzyy_options')

        try:
            connection_info.execute("DROP INDEX IF EXISTS option_data_symbol_index;")
            connection_info.execute("DROP INDEX IF EXISTS option_data_option_symbol_index;")

        except Exception as e:
            print("Index Add ERROR: ", e)
        finally:
            connection_info.dispose()
            index_end = time.time()
            print("Remove Option Data Index |", index_end - index_start)

    def addOptionDataIndex(self):
        index_start = time.time()
        connection_info = create_engine('postgresql://postgres:inkstain@localhost:5432/wzyy_options')

        try:
            connection_info.execute("SET maintenance_work_mem TO '1 GB'")
            connection_info.execute("CREATE INDEX IF NOT EXISTS option_data_option_symbol_index ON option_data(option_symbol, date);")
            connection_info.execute("CREATE INDEX IF NOT EXISTS option_data_symbol_index ON option_data(symbol, date);")

        except Exception as e:
            print("Index Add ERROR: ", e)
        finally:
            connection_info.dispose()
            index_end = time.time()
            print("Add Option Data Index |", index_end - index_start)

    def removeIndex(self):
        index_start = time.time()
        connection_info = create_engine('postgresql://postgres:inkstain@localhost:5432/wzyy_options')

        try:
            connection_info.execute("DROP INDEX IF EXISTS option_data_index;")
            connection_info.execute("DROP INDEX IF EXISTS option_stat_index;")
            connection_info.execute("DROP INDEX IF EXISTS underlying_data_index;")
            connection_info.execute("DROP INDEX IF EXISTS option_flag_index;")
            connection_info.execute("DROP INDEX IF EXISTS process_log_ticker_index;")

        except Exception as e:
            print("Index Add ERROR: ", e)
        finally:
            connection_info.dispose()
            index_end = time.time()
            print("Remove Index |", index_end - index_start)

    def addIndex(self):
        print("Adding Index To Option Data")
        index_start = time.time()
        connection_info = create_engine('postgresql://postgres:inkstain@localhost:5432/wzyy_options')

        try:
            connection_info.execute("CREATE INDEX IF NOT EXISTS option_data_index ON option_data(date, symbol, option_symbol);")
            connection_info.execute("CREATE INDEX IF NOT EXISTS option_stat_index ON option_stat (date, symbol);")
            connection_info.execute("CREATE INDEX IF NOT EXISTS underlying_data_index ON underlying_data(date, symbol);")
            connection_info.execute("CREATE INDEX IF NOT EXISTS option_flag_index ON option_flag(date, symbol, option_symbol);")
            connection_info.execute("CREATE INDEX IF NOT EXISTS process_log_ticker_index ON process_log_ticker(source_date, symbol);")

        except Exception as e:
            print("Index Add ERROR: ", e)
        finally:
            connection_info.dispose()
            index_end = time.time()
            print("Re-index Option Data |", index_end - index_start)


    ##################################################################################################################################


if __name__ == '__main__':

    # existingTickers.fetchAllPrices(existingTickers)


    # filenames = ["20180716_OData.csv", "20180717_OData.csv", "20180718_OData.csv", "20180719_OData.csv", "20180720_OData.csv"]
    # filenames += ["20180723_OData.csv", "20180724_OData.csv", "20180725_OData.csv", "20180726_OData.csv", "20180727_OData.csv"]
    # # filenames = ["20180725_OData.csv"]

    filenames = ["20180102_OData.csv", "20180103_OData.csv", "20180104_OData.csv"]
    filenames = ['20180515_OData.csv']

    ticker = ["GME",'TPX','TROX','AAPL','JAG','BBBY','QCOM','FDC','BLL','XRT','DPLO','USG','CPB','WWE','FOSL','WIN','ACXM']

    files = os.listdir(DATA_PATH)
    csvfiles = [fi for fi in files if (fi.endswith(".csv"))]


#######  INPUTS   ###########################################################################################################

    ticker = []
    process_files = filenames

#####################################################################################################################
    process = psutil.Process(os.getpid())

    now = dt.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
    print("Start NEW LOAD PROCESS | MEM: {0} MB | {1}".format(process.memory_info().rss / 1048576, now))
    logger.info("Start NEW LOAD PROCESS | MEM: {0} MB | {1}".format(process.memory_info().rss / 1048576, now))

    try:
        dataError_File = 'Data_Errors.csv'
        dataHole_File = 'Data_Holes.csv'
        if os.path.isfile(dataError_File):
            os.remove(dataError_File)
            print("Deleted ", dataError_File)
        if os.path.isfile(dataHole_File):
            os.remove(dataHole_File)
            print("Deleted ", dataHole_File)

    except Exception as e:
        print(e)

    file_data_path = 'C:\\Users\\Yaos\\Desktop\\Trading\\OptionsData\\'
    timestart = time.time()

    df_files = pd.read_csv(file_data_path + 'File_Index.csv', encoding='ISO-8859-1', na_values=['#NAME?','NaN','Infinity','-Infinity'])
    df_files['filename_next'] = df_files['filename'].shift(-1)
    df_files['filename_5day'] = df_files['filename'].shift(5)

    df_files['year'] = df_files['filename'].astype(str).str[:4]
    df_files = df_files.loc[df_files['year'] == '2018']

    # df_files = df_files[df_files['filename']=='20180613_OData.csv']


    dl = DataLoader()
    # dl.addOptionDataConstraint()
    # dl.addOptionDataIndex()
    # dl.removeOptionDataConstraint()
    # dl.removeOptionDataIndex()
    # try:
    #     DataLoader.readOICSV(DataLoader, '20180517_OData.csv')
    # except Exception as e:
    #     print(e)
    i = 0



    try:
        for row in df_files.itertuples():
            i += 1
            print("             ")
            try:
                load_start = time.time()
                print("Loading | {0} | NewOI {1} | 5-DayOI {2} | Tickers {3} | {4}/{5}".format(row.filename, row.filename_next, row.filename_5day, len(ticker), i, len(df_files)))
                DataLoader().loadOptionsHistoricaDoubleLoadTest(row.filename, row.filename_next, row.filename_5day, ticker)
                load_end = time.time()
                now = dt.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
                print("{0} | {1} | {2} | FINISH | Complete Process |{3} | {4}".format(row.filename, row.filename_next, row.filename_5day, now, load_end - load_start))
            except Exception as e:
                load_end = time.time()
                now = dt.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
                print("Error Processing {0} | {1} | {2} | {3} |{4} | {5}".format(row.filename, row.filename_next, row.filename_5day, e, now, load_end - load_start))
                logger.info("Error Processing {0} | {1} | {2} | {3} |{4} | {5}".format(row.filename, row.filename_next, row.filename_5day, e, now, load_end - load_start))
                break
        # dl.addOptionDataConstraint()
        # dl.addOptionDataIndex()
        dl.terminateConnections()
        timeend = time.time()
        print("FINISH Files: SUCCESS | {0} - {1} | {2} Files | {3}".format(process_files[0], process_files[-1], len(process_files),timeend-timestart))
        logger.info("FINISH Files: SUCCESS | {0} - {1} | {2} Files | {3}".format(process_files[0], process_files[-1], len(process_files),timeend-timestart))

    except Exception as e:
        dl.terminateConnections()
        timeend = time.time()
        print("ERROR Processing File | {0} - {1} | {2} Files | {3}".format(process_files[0], process_files[-1], len(process_files),timeend-timestart))
        logger.info("ERROR Processing File | {0} - {1} | {2} Files | {3}".format(process_files[0], process_files[-1], len(process_files),timeend-timestart))
