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
        try:
            format_start = time.time()
            init_df = len(df)

            if len(df) == 0:
                print("No Data in file. Returning loop")

        # RENAME COLUMNS
            df_rename = {"Symbol": "symbol", "ExpirationDate": "option_expiration", "DataDate": "date", "AskPrice": "ask", "BidPrice": "bid", "LastPrice": "last",
                         "PutCall": "call_put", "StrikePrice": "strike", "Volume": "volume", "OpenInterest": "open_interest", "UnderlyingPrice": "stock_price",
                         "ImpliedVolatility": "iv", "Delta": "delta", "Gamma": "gamma", "Vega": "vega"}
            df.rename(columns=df_rename, inplace=True)

        # FORMAT FILE
            df['date'] = pd.to_datetime(df['date'], format="%Y-%m-%d").dt.date
            dataDate = df['date'].max()

            df['option_expiration'] = pd.to_datetime(df['option_expiration'], format="%Y-%m-%d").dt.date
            df['call_put'] = df['call_put'].apply(lambda x: "C" if x == "call" else "P")
            df['option_symbol'] = df.apply(lambda x: x['symbol'].ljust(6) + x['option_expiration'].strftime('%y%m%d') + x['call_put'] + str(int(x['strike'] * 1000)).zfill(8), axis=1)

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

        # CHECK DATA FOR NEW TICKERS
            unique_tickers = df[['symbol', 'date']].drop_duplicates('symbol')
            unique_tickers['symbol'] = unique_tickers['symbol'].apply(lambda x: x.replace('/', '_'))
            # print(unique_tickers.head())
            self.checkTickerTables(unique_tickers)

            format_end = time.time()
            print("{0} | Finish Formatting | {1}".format(filename, format_end - format_start))

        except Exception as e:
            format_end = time.time()
            print("{0} | ERROR Formatting | {1}".format(filename, format_end - format_start))

        finally:
            return df

    def checkTickerTables(self, df):
        #  Check Tickers
        checkTicker_start = time.time()
        existing_tickers = existingTickers().all(option="all")
        existing_tickers_symbols = existing_tickers['symbol'].values
        dataDate = df['date'].max()
        checkTicker_end = time.time()

        print("{0} | tickers | {1} file| {2} database".format(dataDate.strftime('%m/%d/%Y'), len(df), len(existing_tickers_symbols), checkTicker_end - checkTicker_start))
        logger.info("{0} | tickers | {1} file| {2} database".format(dataDate.strftime('%m/%d/%Y'), len(df), len(existing_tickers_symbols), checkTicker_end - checkTicker_start))

        connection_info = create_engine('postgresql://postgres:inkstain@localhost:5432/wzyy_options')
        start = connection_info.execute("select count(*) from ticker_log where end_date is null").fetchone()[0]


    # LOOP THROUGH EXISTING TICKERS, CHECK FOR TICKERS THAT DISAPPEAR
    #     for index, row in existing_tickers.iterrows():
    #         ticker = row['symbol']
    #         if ticker not in df['symbol'] and row['end_date'] is None:
    #
    #             print("Update ticker_log {0}: {1} End Date".format(ticker, dataDate))
    #             logger.info("Update ticker_log {0}: {1} End Date".format(ticker, dataDate))
    #             connection_info.execute("UPDATE ticker_log set end_date = '{0}' where symbol = '{1}'".format(dataDate, ticker))

    # LOOP THROUGH DATA, CHECK FOR NEW TICKERS
        for index, row in df.iterrows():
            ticker = row['symbol']
            # Add New Tickers to Ticker Log
            if ticker not in existing_tickers_symbols:
                print("NEW TICKER: {0}".format(ticker))
                connection_info.execute("insert into ticker_log (symbol, start_date) values  ('{0}','{1}')".format(ticker, dataDate))

                print("Tickers Info | Added {0}".format(ticker))
                logger.info("Tickers Info | Added {0}".format(ticker))

        final = connection_info.execute("select count(*) from ticker_log where end_date is null").fetchone()[0]
        connection_info.dispose()
        checkTicker_end = time.time()
        print("{0} | Checked Tickers | Tickers: {1} | {2} Add | {3}".format(dataDate.strftime('%y%m%d'), final, final - start, checkTicker_end-checkTicker_start))
        logger.info("{0} | Checked Tickers | Tickers: {1} | {2} Add | {3}".format(dataDate.strftime('%y%m%d'), final, final - start, checkTicker_end-checkTicker_start))


            # Fetch Prices for New Underlying
            #     DataManager.fetchUnderlyingMS(DataManager(), ticker, date_length='full')


    def updateHistoricalFile(self, filename, tickers):

        process = psutil.Process(os.getpid())
        filename = filename.replace(".zip", "")
        connection_info = create_engine('postgresql://postgres:inkstain@localhost:5432/wzyy_options')
        file_start = time.time()

        logger.info("Before Extract File: {0} MEM:{1} MB".format(filename, (process.memory_info().rss / 1048576)))

        # READ OPTION CSV FILE
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

        df = pd.read_csv(DATA_PATH + '{0}'.format(str(filename)), encoding='ISO-8859-1', usecols=usecols, dtype=dtypes, na_values=['Infinity', '-Infinity'])
        init_df = len(df)

        if len(df) == 0:
            print("No Data in file. Returning loop")

        df_rename = {"Symbol": "symbol", "ExpirationDate": "option_expiration", "DataDate": "date", "AskPrice": "ask", "BidPrice": "bid", "LastPrice": "last",
                     "PutCall": "call_put", "StrikePrice": "strike", "Volume": "volume", "OpenInterest": "open_interest", "UnderlyingPrice": "stock_price",
                     "ImpliedVolatility": "iv", "Delta": "delta", "Gamma": "gamma", "Vega": "vega"}
        df.rename(columns=df_rename, inplace=True)
        # print(df.head())

        # FILTER DATA FOR TICKERS
        if len(tickers) > 0:
            df = df[df["symbol"].isin(tickers)]
        # print("Filtered {0} for {1} Tickers: {2} Records MEM: {3} MB".format(filename, len(tickers), len(df), (process.memory_info().rss / 1048576)))
        logger.info("Filtered {0} for {1} Tickers: {2} Records MEM: {3} MB".format(filename, len(tickers), len(df), (process.memory_info().rss / 1048576)))

        # FORMAT FILE
        df['date'] = pd.to_datetime(df['date'], format="%Y-%m-%d").dt.date
        dataDate = df['date'].max()

        df['option_expiration'] = pd.to_datetime(df['option_expiration'], format="%Y-%m-%d").dt.date
        df['call_put'] = df['call_put'].apply(lambda x: "C" if x == "call" else "P")
        df['option_symbol'] = df.apply(lambda x: x['symbol'].ljust(6) + x['option_expiration'].strftime('%y%m%d') + x['call_put'] + str(int(x['strike'] * 1000)).zfill(8), axis=1)
        # print("Delta: {0} Gamma: {1} Vega {2}".format(df['delta'].sum(), df['gamma'].sum(),df['vega'].sum() ))

        # DROP DUPLICATE OPTION_SYMBOLS
        preDup = len(df)
        df = df.drop_duplicates(subset=['option_symbol'], keep='first')
        print("{0} | Pre: {1} Post: {2}| {3} Dups".format(filename, preDup, len(df), len(df) - preDup))

        read_end = time.time()
        print("{0} | {1} Read| {2} Process | {3}".format(filename, init_df, len(df), read_end - file_start))
        logger.info("{0}| {1} Read| {2} Process | {3}".format(filename, init_df, len(df), read_end - file_start))

        # CHECK DATA FOR OTHER DATES
        df_extra = df[df['date'] != dataDate]
        if len(df_extra) > 0:
            print("Other Date Records in {0}: {1}, Expecting {2}".format(filename, len(df_extra), dataDate))
            logger.info("Other Date Records in {0}: {1}, Expecting {2}".format(filename, len(df_extra), dataDate))
            print(df_extra[['date', 'symbol']].groupby(['date'])['symbol'].count().reset_index(name='count'))
            logger.info(df_extra[['date', 'symbol']].groupby(['date'])['symbol'].count().reset_index(name='count'))

            df_extra.to_csv("{0}_DateDups.csv".format(filename))
            # df = df[df['date'] == dataDate]

        # CHECK DATA FOR NEW TICKERS
        unique_tickers = df[['symbol', 'date']].drop_duplicates('symbol')
        unique_tickers['symbol'] = unique_tickers['symbol'].apply(lambda x: x.replace('/', '_'))
        # print(unique_tickers.head())
        self.checkTickerTables(unique_tickers)

        # DROP INDEX IN DATA BEFORE LOADING
        #     try:
        #         connection_info.execute("DROP INDEX IF EXISTS option_data_symbol_index;")
        #     except Exception as e:
        #         print("Index Drop ERROR: ", e)

        # FIND PREVIOUS DATES
        try:
            prev_date = 0
            prev_date_file = ""
            prev_date_5 = 0
            prev_date_5_file = ""

            request = "select * from process_log where source_date <= '{0}' ORDER BY source_date DESC".format(dataDate)
            process_log = pd.read_sql(request, connection_info)

            # FIND DAY - 1
            if len(process_log) == 0:
                # print("No Data for Prev Day {0}".format(dataDate))
                logger.info("No Data for Prev Day {0}".format(dataDate))
            else:
                prev_date = (dataDate + dt.timedelta(-1))
                prev_date_found = prev_date in process_log['source_date'].values

                while ~prev_date_found & (prev_date > min(process_log['source_date'])):
                    prev_date = (prev_date + dt.timedelta(-1))
                    prev_date_found = (prev_date in process_log['source_date'].values)
                    # print(prev_date, prev_date_found)
                prev_date_file = process_log[process_log['source_date'] == prev_date]['source_file'].iloc[0]
                # print("PREV DAY-1 : {0} {1}".format(prev_date, prev_date_file))

        except Exception as e:
            print("Find Previous Dates Error {0} {1}".format(dataDate, e))


        # START PROCESSING
        print("{0} | Processing | Day-1: {1} {2}".format(filename, prev_date, prev_date_file))

            # PREV DAY OI
        if prev_date != 0:
            try:
                df = self.updatePrevOITest(prev_date, df)
                process_log['prev_oi_update'] = dt.datetime.now()
                process_log['prev_oi_update_file'] = prev_date_file
                # print("{0} | Updated Prev Day OI| {1}".format(filename, dt.datetime.now()))

            except Exception as e:
                print("Update Prev Day OI: ERROR - {0} {1} - {2}: {3}".format(ticker, dataDate, prev_date, e))
                logger.info("Update Prev Day OI: ERROR - {0} {1} - {2}: {3}".format(ticker, dataDate, prev_date, e))


    def loadOptionsHistoricaDoubleLoadTest(self, filename, filename_next, filename_5OI, tickers):

        process = psutil.Process(os.getpid())
        filename = filename.replace(".zip", "")
        connection_info = create_engine('postgresql://postgres:inkstain@localhost:5432/wzyy_options')
        file_start = time.time()

        # Check if current date file has already been processed. Skips processing if already processed.
        if self.checkFileProcessed(filename):
            print('{0} FILE WAS PROCESSED - SKIP'.format(filename))
            logger.info('{0} FILE WAS PROCESSED - SKIP'.format(filename))
            return

        logger.info("Before Extract File: {0} MEM:{1} MB".format(filename, (process.memory_info().rss / 1048576)))

    # READ OPTION CSV FILE
        try:
            usecols = ['Symbol', 'ExpirationDate', 'AskPrice', 'BidPrice', 'LastPrice', 'PutCall', 'StrikePrice', 'Volume', 'ImpliedVolatility', 'Delta', 'Gamma', 'Vega', 'OpenInterest','UnderlyingPrice','DataDate']

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

            print("{0} | Reading File".format(filename))
            df = pd.read_csv(DATA_PATH + '{0}'.format(str(filename)), encoding='ISO-8859-1', usecols=usecols, dtype=dtypes, na_values=['Infinity','-Infinity'])
            init_df = len(df)

            if len(df) == 0:
                print("No Data in file. Returning loop")

            df_rename = {"Symbol":"symbol", "ExpirationDate":"option_expiration","DataDate":"date","AskPrice":"ask","BidPrice":"bid","LastPrice":"last",
                         "PutCall":"call_put","StrikePrice":"strike", "Volume":"volume", "OpenInterest":"open_interest","UnderlyingPrice":"stock_price",
                         "ImpliedVolatility":"iv", "Delta":"delta","Gamma":"gamma","Vega":"vega"}
            df.rename(columns=df_rename, inplace=True)
            # print(df.head())

        # FILTER DATA FOR TICKERS
            if len(tickers) > 0:
                df = df[df["symbol"].isin(tickers)]
            # print("Filtered {0} for {1} Tickers: {2} Records MEM: {3} MB".format(filename, len(tickers), len(df), (process.memory_info().rss / 1048576)))
            logger.info("Filtered {0} for {1} Tickers: {2} Records MEM: {3} MB".format(filename, len(tickers), len(df), (process.memory_info().rss / 1048576)))

        # FORMAT FILE
            df['date'] = pd.to_datetime(df['date'], format="%Y-%m-%d").dt.date
            dataDate = df['date'].max()

            df['option_expiration'] = pd.to_datetime(df['option_expiration'], format="%Y-%m-%d").dt.date
            df['call_put'] = df['call_put'].apply(lambda x: "C" if x == "call" else "P")
            df['option_symbol'] = df.apply(lambda x:  x['symbol'].ljust(6) + x['option_expiration'].strftime('%y%m%d') + x['call_put'] + str(int(x['strike']*1000)).zfill(8), axis=1)
            # print("Delta: {0} Gamma: {1} Vega {2}".format(df['delta'].sum(), df['gamma'].sum(),df['vega'].sum() ))

        # DROP DUPLICATE OPTION_SYMBOLS
            preDup = len(df)
            df = df.drop_duplicates(subset=['option_symbol'], keep='first')
            if preDup - len(df) > 0 :
                print("{0} | Pre: {1} Post: {2}| {3} Dups".format(filename, preDup, len(df), len(df)-preDup))
                logger.info("{0} | Pre: {1} Post: {2}| {3} Dups".format(filename, preDup, len(df), len(df)-preDup))

            read_end = time.time()
            print("{0} | {1} Read| {2} Process | {3}".format(filename, init_df, len(df), read_end-file_start))
            logger.info("{0}| {1} Read| {2} Process | {3}".format(filename, init_df, len(df), read_end-file_start))


        # CHECK DATA FOR OTHER DATES
            df_extra = df[df['date'] != dataDate]
            if len(df_extra) > 0:
                print("Other Date Records in {0}: {1}, Expecting {2}".format(filename, len(df_extra), dataDate))
                logger.info("Other Date Records in {0}: {1}, Expecting {2}".format(filename, len(df_extra), dataDate))
                print(df_extra[['date', 'symbol']].groupby(['date'])['symbol'].count().reset_index(name='count'))
                logger.info(df_extra[['date', 'symbol']].groupby(['date'])['symbol'].count().reset_index(name='count'))

                df_extra.to_csv("{0}_DateDups.csv".format(filename))
                # df = df[df['date'] == dataDate]

        # CHECK DATA FOR NEW TICKERS
            unique_tickers = df[['symbol', 'date']].drop_duplicates('symbol')
            unique_tickers['symbol'] = unique_tickers['symbol'].apply(lambda x: x.replace('/', '_'))
            # print(unique_tickers.head())
            self.checkTickerTables(unique_tickers)

        except Exception as e:
            print("ERROR Loading {0} | {1}".format(filename,e))
            logger.info("ERROR Loading {0} | {1}".format(filename,e))


# READ NEXT FILE FOR NEW OI
        next_read_start = time.time()

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

            print("{0} | Reading File".format(filename_next))
            df_next = pd.read_csv(DATA_PATH + '{0}'.format(filename_next), encoding='ISO-8859-1', usecols=usecols, dtype=dtypes, na_values=['Infinity','-Infinity'])

            df_next_rename = {"Symbol": "symbol", "ExpirationDate": "option_expiration", "DataDate": "date",
                              "PutCall": "call_put", "StrikePrice": "strike", "Volume": "volume", "OpenInterest": "open_interest"}
            df_next.rename(columns=df_next_rename, inplace=True)

        # FORMAT FILE
            df_next['date'] = pd.to_datetime(df_next['date'], format="%Y-%m-%d").dt.date
            next_dataDate = df_next['date'].max()

            if next_dataDate < dataDate :
                print("Next Data Date ERROR| CUR: {0} {1} | NEXT: {2} {3}".format(filename, dataDate, filename_next, next_dataDate))
                logger.info("Next Data Date ERROR| CUR: {0} {1} | NEXT: {2} {3}".format(filename, dataDate, filename_next, next_dataDate))
                return

            df_next['option_expiration'] = pd.to_datetime(df_next['option_expiration'], format="%Y-%m-%d").dt.date
            df_next['call_put'] = df_next['call_put'].apply(lambda x: "C" if x == "call" else "P")
            df_next['option_symbol'] = df_next.apply(lambda x: x['symbol'].ljust(6) + x['option_expiration'].strftime('%y%m%d') + x['call_put'] + str(int(x['strike'] * 1000)).zfill(8), axis=1)

        # DROP DUPLICATE OPTION_SYMBOLS
            preDup = len(df_next)
            df_next = df_next.drop_duplicates(subset=['option_symbol'], keep='first')

            if preDup -len(df_next) > 0:
                print("{0} | NEXT FILE | Pre: {1} Post: {2}| {3} Dups".format(filename_next, preDup, len(df_next), len(df_next) - preDup))
                logger.info("{0} | NEXT FILE | Pre: {1} Post: {2}| {3} Dups".format(filename_next, preDup, len(df_next), len(df_next) - preDup))

            next_read_end = time.time()
            print("{0} | NEXT FILE | {1} Read| {2} Process | {3}".format(filename, init_df, len(df_next), next_read_end - next_read_start))
            logger.info("{0}| NEXT FILE | {1} Read| {2} Process | {3}".format(filename_next, init_df, len(df_next), next_read_end - next_read_start))

        except Exception as e:
            print("ERROR Loading {0} | {1}".format(filename_next,e))
            logger.info("ERROR Loading {0} | {1}".format(filename_next,e))


    # MERGING DATA SETS FOR NEW OI
        try:
            merge_start = time.time()
            df_next = df_next[['option_symbol', 'open_interest']]
            df_next.columns = ["option_symbol", "open_interest_new"]
            OI_zero_count = len(df) - df['open_interest'].astype(bool).sum(axis=0)
            df_next_OI_zero_count = len(df_next) - df_next['open_interest_new'].astype(bool).sum(axis=0)

            print("Merging DataFiles | {0} {1} | {2} No OI | {3} {4} | {5} No New OI".format(dataDate.strftime('%y%m%d'), len(df), OI_zero_count, next_dataDate.strftime('%y%m%d'), len(df_next),df_next_OI_zero_count))
            logger.info("Merging DataFiles | {0} {1} | {2} No OI | {3} {4} | {5} No New OI".format(dataDate.strftime('%y%m%d'), len(df), OI_zero_count, next_dataDate.strftime('%y%m%d'), len(df_next),df_next_OI_zero_count))

            df = pd.merge(df, df_next, how='left', on=['option_symbol'])
            NEWOI_zero_count = len(df) - df['open_interest_new'].astype(bool).sum(axis=0)
            merge_end = time.time()

            print("Merged | {0} | {1} Recs | {2} No OI | {3} No New OI | {4}".format(dataDate.strftime('%y%m%d'), len(df), OI_zero_count,NEWOI_zero_count, merge_end - merge_start))
            logger.info("Merged | {0} | {1} Recs | {2} No OI | {3} No New OI | {4}".format(dataDate.strftime('%y%m%d'), len(df), OI_zero_count,NEWOI_zero_count, merge_end - merge_start))

        except Exception as e:
            print("ERROR Merging DataSets | {0} | {1}".format(filename, filename_next))
            logger.info("ERROR Merging DataSets | {0} | {1}".format(filename, filename_next))

    # CHECK FOR DATA HOLES
        try:
            # Volume check is to make sure it wasn't used to fully close out OI and that 0 OI was legit
            # Checks if OI or NEW_OI = 0. Filters for volume < 50% OI to screen out actual closings or openings
            df['data_holes'] = df.apply(lambda x: 1 if (x['volume'] < (0.5 * max(x['open_interest'], x['open_interest_new']))) & ((x['open_interest'] == 0) | (x['open_interest_new'] == 0)) else 0, axis=1)
            dataHoles = df[df['data_holes'] == 1]
            print("DATA HOLES: ", len(dataHoles))
            hole_start = time.time()

            if len(dataHoles) > 0:
                # print("Update Prev OI - Data Holes: {0} {1} | {2} Recs".format(ticker, dataDate, len(dataHoles)))

                # EXPORT DATA HOLES FILE
                datahole_file = 'Data_Holes.csv'
                if os.path.isfile(datahole_file):
                    dataHoles.to_csv(datahole_file, mode='a', header=False)
                else:
                    dataHoles.to_csv(datahole_file, header=True)
                    print("Create {0}".format(datahole_file))
                    logger.info("Create {0}".format(datahole_file))

                # UPDATE DATA HOLES IN CURRENT DATA
                for row in dataHoles.itertuples():

                    # Update Hole in Current Day Data
                    df_idx = df.index[df['option_symbol'] == row.option_symbol].tolist()
                    for index in df_idx:
                        max_OI = max(row.open_interest, row.open_interest_new)

                    # UPDATE NEW OPEN INTEREST IF 0 TO CURRENT OI (ASSUME NO CHANGE)
                        if row.open_interest_new == 0:
                            orig_oi = df.at[index, 'open_interest_new']
                            df.at[index, 'open_interest_new'] = max_OI
                            new_oi = df.at[index, 'open_interest_new']
                            logger.info("{0}| {1} | NEW_OI HOLE UPDATE| {2} To {3}".format(row.option_symbol, row.date, orig_oi, new_oi))

                    # UPDATES CURRENT OPEN INTEREST IF 0.
                        if row.open_interest == 0:
                            orig_oi = df.at[index, 'open_interest']
                            df.at[index, 'open_interest'] = max_OI
                            new_oi = df.at[index, 'open_interest']
                            logger.info("{0}| {1} | OI HOLE UPDATE| {2} To {3}".format(row.option_symbol, row.date, orig_oi, new_oi))
                            # DIFFERENCE SMALL ENOUGH TO NOT WARRANT THE ADDITIONAL PERFORMANCE DRAG

            post_OI_zero_count = len(df) - df['open_interest'].astype(bool).sum(axis=0)
            post_NEWOI_zero_count = len(df) - df['open_interest_new'].astype(bool).sum(axis=0)
            oi_fill = OI_zero_count - post_OI_zero_count
            newoi_fill = NEWOI_zero_count - post_NEWOI_zero_count
            hole_end = time.time()
            df.drop(columns='data_holes', inplace=True)


            print("{0} | {1} Rec| {2} No OI; Filled {3} | {4} No NEW OI; Filled {5} | {6} Data Holes | {7}".format(dataDate.strftime('%Y-%m-%d'), len(df), post_OI_zero_count, oi_fill, post_NEWOI_zero_count, newoi_fill, len(dataHoles),
                                                                                                                   hole_end - hole_start))
            logger.info("{0} | {1} Rec| {2} No OI; Filled {3} | {4} No NEW OI; Filled {5} | {6} Data Holes | {7}".format(dataDate.strftime('%Y-%m-%d'), len(df), post_OI_zero_count, oi_fill, post_NEWOI_zero_count, newoi_fill, len(dataHoles),
                                                                                                                   hole_end - hole_start))
        except Exception as e:
            hole_end = time.time()
            print("{0} | Data Hole Error | {1} | {2}".format(dataDate.strftime('%Y-%m-%d'), hole_end-hole_start, e))
            logger.info("{0} | Data Hole Error | {1} | {2]".format(dataDate.strftime('%Y-%m-%d'), hole_end-hole_start, e))

    # CALCULATE OI CHANGE

        df['open_interest_change'] = df.apply(lambda x: (x['open_interest_new'] - x['open_interest']) if ~math.isnan(x['open_interest_new']) else 0, axis=1)
        df['open_interest_change'].fillna(0, inplace=True)


    # FIND PREVIOUS DATES
        try:
            prev_date = 0
            prev_date_file = ""
            prev_date_5 = 0
            prev_date_5_file = ""

            request = "select * from process_log where source_date <= '{0}' ORDER BY source_date DESC".format(dataDate)
            process_log = pd.read_sql(request, connection_info)

        # FIND DAY - 1
            if len(process_log)==0:
                # print("No Data for Prev Day {0}".format(dataDate))
                logger.info("No Data for Prev Day {0}".format(dataDate))
            else:
                prev_date = (dataDate + dt.timedelta(-1))
                prev_date_found = prev_date in process_log['source_date'].values

                while ~prev_date_found & (prev_date > min(process_log['source_date'])):
                    prev_date = (prev_date + dt.timedelta(-1))
                    prev_date_found = (prev_date in process_log['source_date'].values)
                    # print(prev_date, prev_date_found)
                prev_date_file = process_log[process_log['source_date']==prev_date]['source_file'].iloc[0]
                # print("PREV DAY-1 : {0} {1}".format(prev_date, prev_date_file))

        # FIND DAY - 5
            if len(process_log)<6:
                # print("No Data for Prev 5 Day {0}".format(dataDate))
                logger.info("No Data for Prev 5 Day {0}".format(dataDate))
            else:
                prev_date_5 = (dataDate + dt.timedelta(-7))
                prev_date_found = prev_date_5 in process_log['source_date'].values

                while ~prev_date_found & (prev_date_5 > min(process_log['source_date'])):
                    prev_date_5 = (prev_date_5 + dt.timedelta(-1))
                    prev_date_found = (prev_date_5 in process_log['source_date'].values)
                    # print(prev_date_5, prev_date_found)

                prev_date_5_file = process_log[process_log['source_date'] == prev_date_5]['source_file'].iloc[0]
                # print("PREV DAY-5 : {0} {1}".format(prev_date_5, prev_date_5_file))

        except Exception as e:
            print("Find Previous Dates Error {0} {1}".format(dataDate,e))
            logger.info("Find Previous Dates Error {0} {1}".format(dataDate, e))


    # CREATE TICKER LOG
        process_log_cols = ['source_date', 'source_file', 'date', 'record_count', 'ticker_count', 'prev_oi_update', 'prev_oi_update_file',
                            'prev_5day_oi_update', 'prev_5day_oi_update_file', 'upload_date', 'stat_date', 'flag_date', 'process_time']
        process_log = pd.DataFrame(columns=process_log_cols, index=[])
        process_log.loc[len(process_log)] = np.repeat(np.nan, len(process_log_cols))
        process_log['source_date'] = dataDate
        process_log['source_file'] = filename
        process_log['date'] = dt.datetime.now()
        process_log['record_count'] = len(df)
        process_log['ticker_count'] = len(unique_tickers)


        # START PROCESSING

        try:
            print("{0} | Processing | Day-1: {1} {2} | Day-5: {3} {4}".format(filename, prev_date, prev_date_file, prev_date_5, prev_date_5_file))
            # READ IN 5 DAY OI
            if prev_date_5 != 0:
                try:
                    df = self.updatePrev5OI_test(prev_date_5, df)
                    process_log['prev_5day_oi_update'] = dt.datetime.now()
                    process_log['prev_5day_oi_update_file'] = prev_date_5_file
                    # print("{0} | Updated 5 Day OI| {1}".format(filename, dt.datetime.now()))
                except Exception as e:
                    print("Update Prev 5 Day OI: ERROR - {0} {1} - {2}: {3}".format(ticker, dataDate, prev_date_5, e))
                    logger.info("Update Prev 5 Day OI: ERROR - {0} {1} - {2}: {3}".format(ticker, dataDate, prev_date_5, e))

            # PREV DAY OI
            # if prev_date != 0:
            #     try:
            #         df = self.updatePrevOITest(prev_date, df)
            #         process_log['prev_oi_update'] = dt.datetime.now()
            #         process_log['prev_oi_update_file'] = prev_date_file
            #         # print("{0} | Updated Prev Day OI| {1}".format(filename, dt.datetime.now()))
            #
            #     except Exception as e:
            #         print("Update Prev Day OI: ERROR - {0} {1} - {2}: {3}".format(ticker, dataDate, prev_date, e))
            #         logger.info("Update Prev Day OI: ERROR - {0} {1} - {2}: {3}".format(ticker, dataDate, prev_date, e))

            # UPLOAD DATA
            try:
                self.uploadDataTestBulk(dataDate, df)
                return
                # self.uploadDataTest(dataDate, df)
                # print("{0} | Updated Data| {1}".format(filename, dt.datetime.now()))
                process_log['upload_date'] = dt.datetime.now()

            except Exception as e:
                print("Upload Data: ERROR - {0} {1}: {2}".format(ticker, dataDate.strftime('%m/%d/%Y'), e))
                logger.info("Upload Data: ERROR - {0} {1}: {2}".format(ticker, dataDate.strftime('%m/%d/%Y'), e))

            # OPTION STAT SUMMARY
            try:
                self.uploadOptionStatTest(dataDate, df)
                # print("{0} | Updated Stat| {1}".format(filename, dt.datetime.now()))
                process_log['stat_date'] = dt.datetime.now()
            except Exception as e:
                print("Upload Option Stat: ERROR - {0} {1}: {2] ".format(ticker, dataDate.strftime('%m/%d/%Y'), e))
                logger.info("Upload Option Stat: ERROR - {0} {1}: {2] ".format(ticker, dataDate.strftime('%m/%d/%Y'), e))

            # UPLOAD PROCESS LOG
            file_end = time.time()
            process_log['process_time'] = file_end - file_start

            # UPDATE PROCESS LOG AFTER PROCESSING
            try:
                processlog_start = time.time()
                # print(process_log)
                process_log_exists = connection_info.execute("SELECT exists( select * FROM process_log where source_file = '{0}')".format(filename)).fetchone()[0]
                if process_log_exists:
                    print("Process_Log Exists. Overriding {0}".format(filename))
                    connection_info.execute("DELETE FROM process_log where source_file = '{0}'".format(filename))

                process_log.to_sql('process_log', connection_info, if_exists='append', index=False)
                processlog_end = time.time()
                print("{0} | Uploaded process_log | {1} Data| {2} recs| {3} tickers | {4} | {5}".format(filename, df['date'].max(), len(df), len(unique_tickers), pd.datetime.now().strftime('%m/%d/%Y'), processlog_end - processlog_start))
                logger.info("{0} | Uploaded process_log | {1} Data| {2} recs| {3} tickers | {4} | {5}".format(filename, df['date'].max(), len(df), len(unique_tickers), pd.datetime.now().strftime('%m/%d/%Y'), processlog_end - processlog_start))

            except Exception as e:
                print('UPDATE PROCESS LOG: ', e, filename)
                logger.info('UPDATE PROCESS LOG: ', e, filename)
                connection_info.dispose()

        except Exception as e:
            print("Error Processing {0} | {1}".format(filename, e))
            logger.info("Error Processing {0} | {1}".format(filename, e))

        print("{0} | FINISH loadOptionsHistorical | {1}".format(filename,file_end-file_start))
        print("           ")
        logger.info("{0} | FINISH loadOptionsHistorical | {1}".format(filename,file_end-file_start))

    #        print("Removed " + extractFileName," MEM: ",  (process.memory_info().rss/1048576),"MB")

##################################################################################################################################

    def loadOptionsHistoricaTest(self, filename, tickers):

        process = psutil.Process(os.getpid())
        filename = filename.replace(".zip", "")
        connection_info = create_engine('postgresql://postgres:inkstain@localhost:5432/wzyy_options')
        file_start = time.time()

        # Check if current date file has already been processed. Skips processing if already processed.
        if self.checkFileProcessed(filename):
            print('{0} FILE WAS PROCESSED - SKIP'.format(filename))
            logger.info('{0} FILE WAS PROCESSED - SKIP'.format(filename))
            return

        logger.info("Before Extract File: {0} MEM:{1} MB".format(filename, (process.memory_info().rss / 1048576)))

    # READ OPTION CSV FILE
        usecols = ['Symbol', 'ExpirationDate', 'AskPrice', 'BidPrice', 'LastPrice', 'PutCall', 'StrikePrice', 'Volume', 'ImpliedVolatility', 'Delta', 'Gamma', 'Vega', 'OpenInterest','UnderlyingPrice','DataDate']

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


        df = pd.read_csv(DATA_PATH + '{0}'.format(str(filename)), encoding='ISO-8859-1', usecols=usecols, dtype=dtypes, na_values=['Infinity','-Infinity'])
        init_df = len(df)

        if len(df) == 0:
            print("No Data in file. Returning loop")

        df_rename = {"Symbol":"symbol", "ExpirationDate":"option_expiration","DataDate":"date","AskPrice":"ask","BidPrice":"bid","LastPrice":"last",
                     "PutCall":"call_put","StrikePrice":"strike", "Volume":"volume", "OpenInterest":"open_interest","UnderlyingPrice":"stock_price",
                     "ImpliedVolatility":"iv", "Delta":"delta","Gamma":"gamma","Vega":"vega"}
        df.rename(columns=df_rename, inplace=True)
        # print(df.head())

    # FILTER DATA FOR TICKERS
        if len(tickers) > 0:
            df = df[df["symbol"].isin(tickers)]
        # print("Filtered {0} for {1} Tickers: {2} Records MEM: {3} MB".format(filename, len(tickers), len(df), (process.memory_info().rss / 1048576)))
        logger.info("Filtered {0} for {1} Tickers: {2} Records MEM: {3} MB".format(filename, len(tickers), len(df), (process.memory_info().rss / 1048576)))

    # FORMAT FILE
        df['date'] = pd.to_datetime(df['date'], format="%Y-%m-%d").dt.date
        dataDate = df['date'].max()

        df['option_expiration'] = pd.to_datetime(df['option_expiration'], format="%Y-%m-%d").dt.date
        df['call_put'] = df['call_put'].apply(lambda x: "C" if x == "call" else "P")
        df['option_symbol'] = df.apply(lambda x:  x['symbol'].ljust(6) + x['option_expiration'].strftime('%y%m%d') + x['call_put'] + str(int(x['strike']*1000)).zfill(8), axis=1)
        # print("Delta: {0} Gamma: {1} Vega {2}".format(df['delta'].sum(), df['gamma'].sum(),df['vega'].sum() ))

    # DROP DUPLICATE OPTION_SYMBOLS
        preDup = len(df)
        df = df.drop_duplicates(subset=['option_symbol'], keep='first')
        print("{0} | Pre: {1} Post: {2}| {3} Dups".format(filename, preDup, len(df), len(df)-preDup))

        read_end = time.time()
        print("{0} | {1} Read| {2} Process | {3}".format(filename, init_df, len(df), read_end-file_start))
        logger.info("{0}| {1} Read| {2} Process | {3}".format(filename, init_df, len(df), read_end-file_start))


    # CHECK DATA FOR OTHER DATES
        df_extra = df[df['date'] != dataDate]
        if len(df_extra) > 0:
            print("Other Date Records in {0}: {1}, Expecting {2}".format(filename, len(df_extra), dataDate))
            logger.info("Other Date Records in {0}: {1}, Expecting {2}".format(filename, len(df_extra), dataDate))
            print(df_extra[['date', 'symbol']].groupby(['date'])['symbol'].count().reset_index(name='count'))
            logger.info(df_extra[['date', 'symbol']].groupby(['date'])['symbol'].count().reset_index(name='count'))

            df_extra.to_csv("{0}_DateDups.csv".format(filename))
            # df = df[df['date'] == dataDate]


    # CHECK DATA FOR NEW TICKERS
        unique_tickers = df[['symbol', 'date']].drop_duplicates('symbol')
        unique_tickers['symbol'] = unique_tickers['symbol'].apply(lambda x: x.replace('/', '_'))
        # print(unique_tickers.head())
        self.checkTickerTables(unique_tickers)

    # DROP INDEX IN DATA BEFORE LOADING
    #     try:
    #         connection_info.execute("DROP INDEX IF EXISTS option_data_symbol_index;")
    #     except Exception as e:
    #         print("Index Drop ERROR: ", e)

    # FIND PREVIOUS DATES
        try:
            prev_date = 0
            prev_date_file = ""
            prev_date_5 = 0
            prev_date_5_file = ""

            request = "select * from process_log where source_date <= '{0}' ORDER BY source_date DESC".format(dataDate)
            process_log = pd.read_sql(request, connection_info)

        # FIND DAY - 1
            if len(process_log)==0:
                # print("No Data for Prev Day {0}".format(dataDate))
                logger.info("No Data for Prev Day {0}".format(dataDate))
            else:
                prev_date = (dataDate + dt.timedelta(-1))
                prev_date_found = prev_date in process_log['source_date'].values

                while ~prev_date_found & (prev_date > min(process_log['source_date'])):
                    prev_date = (prev_date + dt.timedelta(-1))
                    prev_date_found = (prev_date in process_log['source_date'].values)
                    # print(prev_date, prev_date_found)
                prev_date_file = process_log[process_log['source_date']==prev_date]['source_file'].iloc[0]
                # print("PREV DAY-1 : {0} {1}".format(prev_date, prev_date_file))

        # FIND DAY - 5
            if len(process_log)<6:
                # print("No Data for Prev 5 Day {0}".format(dataDate))
                logger.info("No Data for Prev 5 Day {0}".format(dataDate))
            else:
                prev_date_5 = (dataDate + dt.timedelta(-7))
                prev_date_found = prev_date_5 in process_log['source_date'].values

                while ~prev_date_found & (prev_date_5 > min(process_log['source_date'])):
                    prev_date_5 = (prev_date_5 + dt.timedelta(-1))
                    prev_date_found = (prev_date_5 in process_log['source_date'].values)
                    # print(prev_date_5, prev_date_found)

                prev_date_5_file = process_log[process_log['source_date'] == prev_date_5]['source_file'].iloc[0]
                # print("PREV DAY-5 : {0} {1}".format(prev_date_5, prev_date_5_file))

        except Exception as e:
            print("Find Previous Dates Error {0} {1}".format(dataDate,e))


    # CREATE TICKER LOG
        process_log_cols = ['source_date', 'source_file', 'date', 'record_count', 'ticker_count', 'prev_oi_update', 'prev_oi_update_file',
                            'prev_5day_oi_update', 'prev_5day_oi_update_file', 'upload_date', 'stat_date', 'flag_date', 'process_time']
        process_log = pd.DataFrame(columns=process_log_cols, index=[])
        process_log.loc[len(process_log)] = np.repeat(np.nan, len(process_log_cols))
        process_log['source_date'] = dataDate
        process_log['source_file'] = filename
        process_log['date'] = dt.datetime.now()
        process_log['record_count'] = len(df)
        process_log['ticker_count'] = len(unique_tickers)


        # START PROCESSING

        try:
            print("{0} | Processing | Day-1: {1} {2} | Day-5: {3} {4}".format(filename, prev_date, prev_date_file, prev_date_5, prev_date_5_file))
            # READ IN 5 DAY OI
            if prev_date_5 != 0:
                try:
                    df = self.updatePrev5OI_test(prev_date_5, df)
                    process_log['prev_5day_oi_update'] = dt.datetime.now()
                    process_log['prev_5day_oi_update_file'] = prev_date_5_file
                    # print("{0} | Updated 5 Day OI| {1}".format(filename, dt.datetime.now()))
                except Exception as e:
                    print("Update Prev 5 Day OI: ERROR - {0} {1} - {2}: {3}".format(ticker, dataDate, prev_date_5, e))
                    logger.info("Update Prev 5 Day OI: ERROR - {0} {1} - {2}: {3}".format(ticker, dataDate, prev_date_5, e))

            # PREV DAY OI
            if prev_date != 0:
                try:
                    df = self.updatePrevOITest(prev_date, df)
                    process_log['prev_oi_update'] = dt.datetime.now()
                    process_log['prev_oi_update_file'] = prev_date_file
                    # print("{0} | Updated Prev Day OI| {1}".format(filename, dt.datetime.now()))

                except Exception as e:
                    print("Update Prev Day OI: ERROR - {0} {1} - {2}: {3}".format(ticker, dataDate, prev_date, e))
                    logger.info("Update Prev Day OI: ERROR - {0} {1} - {2}: {3}".format(ticker, dataDate, prev_date, e))

            # UPLOAD DATA
            try:
                self.uploadDataTest(dataDate, df)
                # print("{0} | Updated Data| {1}".format(filename, dt.datetime.now()))
                process_log['upload_date'] = dt.datetime.now()

            except Exception as e:
                print("Upload Data: ERROR - {0} {1}: {2}".format(ticker, dataDate.strftime('%m/%d/%Y'), e))
                logger.info("Upload Data: ERROR - {0} {1}: {2}".format(ticker, dataDate.strftime('%m/%d/%Y'), e))

            # OPTION STAT SUMMARY
            try:
                self.uploadOptionStatTest(dataDate, df)
                # print("{0} | Updated Stat| {1}".format(filename, dt.datetime.now()))
                process_log['stat_date'] = dt.datetime.now()
            except Exception as e:
                print("Upload Option Stat: ERROR - {0} {1}: {2] ".format(ticker, dataDate.strftime('%m/%d/%Y'), e))
                logger.info("Upload Option Stat: ERROR - {0} {1}: {2] ".format(ticker, dataDate.strftime('%m/%d/%Y'), e))


            # UPLOAD PROCESS LOG
            file_end = time.time()
            process_log['process_time'] = file_end - file_start

            # UPDATE PROCESS LOG AFTER PROCESSING
            try:
                processlog_start = time.time()
                # print(process_log)
                process_log_exists = connection_info.execute("SELECT exists( select * FROM process_log where source_file = '{0}')".format(filename)).fetchone()[0]
                if process_log_exists:
                    print("Process_Log Exists. Overriding {0}".format(filename))
                    connection_info.execute("DELETE FROM process_log where source_file = '{0}'".format(filename))

                process_log.to_sql('process_log', connection_info, if_exists='append', index=False)
                processlog_end = time.time()
                print("{0} | Uploaded process_log | {1} Data| {2} recs| {3} tickers | {4} | {5}".format(filename, df['date'].max(), len(df), len(unique_tickers), pd.datetime.now().strftime('%m/%d/%Y'), processlog_end - processlog_start))
                logger.info("{0} | Uploaded process_log | {1} Data| {2} recs| {3} tickers | {4} | {5}".format(filename, df['date'].max(), len(df), len(unique_tickers), pd.datetime.now().strftime('%m/%d/%Y'), processlog_end - processlog_start))

            except Exception as e:
                print('UPDATE PROCESS LOG: ', e, filename)
                logger.info('UPDATE PROCESS LOG: ', e, filename)
                connection_info.dispose()

        except Exception as e:
            print("Error Processing {0}".format(filename))
            logger.info("Error Processing {0}".format(filename))

        print("{0} | FINISH loadOptionsHistorical | {1}".format(filename,file_end-file_start))
        print("           ")
        logger.info("{0} | FINISH loadOptionsHistorical | {1}".format(filename,file_end-file_start))

    #        print("Removed " + extractFileName," MEM: ",  (process.memory_info().rss/1048576),"MB")



##################################################################################################################

    def loadOptionsHistorical(self, filename, tickers):

        process = psutil.Process(os.getpid())
        filename = filename.replace(".zip", "")
        connection_info = create_engine('postgresql://postgres:inkstain@localhost:5432/wzyy_options')
        file_start = time.time()

        # Check if current date file has already been processed. Skips processing if already processed.
        if self.checkFileProcessed(filename):
            print('{0} FILE WAS PROCESSED - SKIP'.format(filename))
            logger.info('{0} FILE WAS PROCESSED - SKIP'.format(filename))
            return

        logger.info("Before Extract File: {0} MEM:{1} MB".format(filename, (process.memory_info().rss / 1048576)))

    # READ OPTION CSV FILE
        usecols = ['Symbol', 'ExpirationDate', 'AskPrice', 'BidPrice', 'LastPrice', 'PutCall', 'StrikePrice', 'Volume', 'ImpliedVolatility', 'Delta', 'Gamma', 'Vega', 'OpenInterest','UnderlyingPrice','DataDate']

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


        df = pd.read_csv(DATA_PATH + '{0}'.format(str(filename)), encoding='ISO-8859-1', usecols=usecols, dtype=dtypes, na_values=['Infinity','-Infinity'])
        init_df = len(df)

        if len(df) == 0:
            print("No Data in file. Returning loop")

        df_rename = {"Symbol":"symbol", "ExpirationDate":"option_expiration","DataDate":"date","AskPrice":"ask","BidPrice":"bid","LastPrice":"last",
                     "PutCall":"call_put","StrikePrice":"strike", "Volume":"volume", "OpenInterest":"open_interest","UnderlyingPrice":"stock_price",
                     "ImpliedVolatility":"iv", "Delta":"delta","Gamma":"gamma","Vega":"vega"}
        df.rename(columns=df_rename, inplace=True)
        # print(df.head())

    # FILTER DATA FOR TICKERS
        if len(tickers) > 0:
            df = df[df["symbol"].isin(tickers)]
        # print("Filtered {0} for {1} Tickers: {2} Records MEM: {3} MB".format(filename, len(tickers), len(df), (process.memory_info().rss / 1048576)))
        logger.info("Filtered {0} for {1} Tickers: {2} Records MEM: {3} MB".format(filename, len(tickers), len(df), (process.memory_info().rss / 1048576)))

    # FORMAT FILE
        df['date'] = pd.to_datetime(df['date'], format="%Y-%m-%d").dt.date
        df['option_expiration'] = pd.to_datetime(df['option_expiration'], format="%Y-%m-%d").dt.date
        df['call_put'] = df['call_put'].apply(lambda x: "C" if x == "call" else "P")
        df['option_symbol'] = df.apply(lambda x:  x['symbol'].ljust(6) + x['option_expiration'].strftime('%y%m%d') + x['call_put'] + str(int(x['strike']*1000)).zfill(8), axis=1)
        # print("Delta: {0} Gamma: {1} Vega {2}".format(df['delta'].sum(), df['gamma'].sum(),df['vega'].sum() ))
        dataDate = df['date'].max()

    # DROP DUPLICATE OPTION_SYMBOLS
        preDup = len(df)
        df = df.drop_duplicates(subset=['option_symbol'], keep='first')
        print("{0} | Pre: {1} Post: {2}| {3} Dups".format(filename, preDup, len(df), len(df)-preDup))

        read_end = time.time()
        print("{0} Loaded| {1} Read| {2} Process | {3}".format(filename, init_df, len(df), read_end-file_start))
        logger.info("{0} Loaded| {1} Read| {2} Process | {3}".format(filename, init_df, len(df), read_end-file_start))


    # CHECK DATA FOR OTHER DATES
        df_extra = df[df['date'] != dataDate]
        if len(df_extra) > 0:
            print("Other Date Records in {0}: {1} {2}".format(filename, len(df_extra)))
            logger.info("Other Date Records in {0}: {1} {2}".format(filename, len(df_extra)))
            print(df_extra[['date', 'symbol']].groupby(['date'])['symbol'].count().reset_index(name='count'))
            logger.info(df_extra[['date', 'symbol']].groupby(['date'])['symbol'].count().reset_index(name='count'))

            df_extra.to_csv("{0}_DateDups.csv".format(filename))
            # df = df[df['date'] == dataDate]


    # CHECK DATA FOR NEW TICKERS
        unique_tickers = df[['symbol', 'date']].drop_duplicates('symbol')
        unique_tickers['symbol'] = unique_tickers['symbol'].apply(lambda x: x.replace('/', '_'))
        # print(unique_tickers.head())
        self.checkTickerTables(unique_tickers)

    # DROP INDEX IN DATA BEFORE LOADING
    #     try:
    #         connection_info.execute("DROP INDEX IF EXISTS option_data_symbol_index;")
    #     except Exception as e:
    #         print("Index Drop ERROR: ", e)

    # FIND PREVIOUS DATES
        try:
            prev_date = 0
            prev_date_file = ""
            prev_date_5 = 0
            prev_date_5_file = ""

            request = "select * from process_log ORDER BY source_date DESC"
            process_log = pd.read_sql(request, connection_info)

        # FIND DAY - 1
            if len(process_log)==0:
                # print("No Data for Prev Day {0}".format(dataDate))
                logger.info("No Data for Prev Day {0}".format(dataDate))
            else:
                prev_date = (dataDate + dt.timedelta(-1))
                prev_date_found = prev_date in process_log['source_date'].values

                while ~prev_date_found & (prev_date > min(process_log['source_date'])):
                    prev_date = (prev_date + dt.timedelta(-1))
                    prev_date_found = (prev_date in process_log['source_date'].values)
                    # print(prev_date, prev_date_found)
                prev_date_file = process_log[process_log['source_date']==prev_date]['source_file'].iloc[0]
                # print("PREV DAY-1 : {0} {1}".format(prev_date, prev_date_file))

        # FIND DAY - 5
            if len(process_log)<6:
                # print("No Data for Prev 5 Day {0}".format(dataDate))
                logger.info("No Data for Prev 5 Day {0}".format(dataDate))
            else:
                prev_date_5 = (dataDate + dt.timedelta(-7))
                prev_date_found = prev_date_5 in process_log['source_date'].values

                while ~prev_date_found & (prev_date_5 > min(process_log['source_date'])):
                    prev_date_5 = (prev_date_5 + dt.timedelta(-1))
                    prev_date_found = (prev_date_5 in process_log['source_date'].values)
                    # print(prev_date_5, prev_date_found)

                prev_date_5_file = process_log[process_log['source_date'] == prev_date_5]['source_file'].iloc[0]
                # print("PREV DAY-5 : {0} {1}".format(prev_date_5, prev_date_5_file))

        except Exception as e:
            print("Find Previous Dates Error {0} {1}".format(dataDate,e))

    # PROCESS DATA BY TICKER
        try:
            num_processes = multiprocessing.cpu_count() * 2 - 4
            chunks = [chunk[1] for chunk in df.groupby('symbol')]
            pool = multiprocessing.Pool(processes=num_processes)
            print('Starting Ticker Process| CPU: {0} | Chunks: {1}'.format(multiprocessing.cpu_count(), len(chunks)), "| MEM: ", (process.memory_info().rss / 1048576), "MB")
            logger.info('Starting...')

            func = partial(self.processTickers, filename, prev_date, prev_date_file, prev_date_5, prev_date_5_file)
            result = pool.map(func, chunks, chunksize=1)
            pool.close()
            pool.join()

            # UPDATE INDEX IN DATA AFTER LOADING
            # try:
            #     index_start = time.time()
            #     connection_info.execute("CREATE INDEX IF NOT EXISTS option_data_symbol_index ON option_data (symbol, option_symbol);")
            #     index_end = time.time()
            #     print("Re-index Option Data |", index_end - index_start)
            #
            # except Exception as e:
            #     print("Index Add ERROR: ", e)

            # UPDATE PROCESS LOG AFTER PROCESSING
            try:
                processlog_start = time.time()
                process_columns = "(date, source_file, source_date, record_count, ticker_count, process_time)"
                file_end = time.time()

                connection_info.execute("insert into process_log {0} values  ('{1}','{2}','{3}','{4}','{5}','{6}')".format(process_columns, pd.datetime.now(), filename, df['date'].max(), len(df), len(unique_tickers), file_end - file_start))
                processlog_end = time.time()

                print("{0} process_log | {1} Run | {2} Data| {3} rows| {4} tickers | {5}".format(filename, pd.datetime.now().strftime('%m/%d/%Y'), df['date'].max(), len(df), len(unique_tickers), processlog_end - processlog_start))
                logger.info("{0} process_log | {1} Run | {2} Data| {3} rows| {4} tickers | {5}".format(filename, pd.datetime.now().strftime('%m/%d/%Y'), df['date'].max(), len(df), len(unique_tickers), processlog_end - processlog_start))

            except Exception as e:
                print('UPDATE PROCESS LOG: ', e, filename)
                logger.info('UPDATE PROCESS LOG: ', e, filename)
                connection_info.dispose()

        except Exception as e:
            if e == MemoryError:
                print("Memory ERROR")
                return
            else:
                print('ERROR Processing Ticker:', e)
                logger.info('ERROR Processing Ticker:', e)
                connection_info.dispose()
                return

        print("{0} | FINISH loadOptionsHistorical | {1}".format(filename,file_end-file_start))
        print("           ")
        logger.info("{0} | FINISH loadOptionsHistorical | {1}".format(filename,file_end-file_start))

    #        print("Removed " + extractFileName," MEM: ",  (process.memory_info().rss/1048576),"MB")


################################################################################################################################

    def processTickersTest(self, filename, prev_date, prev_date_file, prev_date_5, prev_date_5_file, df):

        dataDate = df['date'].max()
        dataDatePrint = dataDate.strftime('%m/%d/%Y')
        ticker_start = time.time()
        connection_options = create_engine('postgresql://postgres:inkstain@localhost:5432/wzyy_options')

        # CREATE TICKER LOG
        process_log_cols = ['source_date', 'source_file','date','record_count', 'ticker_count','prev_oi_update', 'prev_oi_update_file',
                           'prev_5day_oi_update', 'prev_5day_oi_update_file',  'upload_date', 'stat_date', 'flag_date','process_time']
        ticker_log = pd.DataFrame(columns=process_log_cols, index=[])
        ticker_log.loc[len(ticker_log)] = np.repeat(np.nan, len(process_log_cols))
        ticker_log['symbol'] = ticker
        ticker_log['source_date'] = dataDate
        ticker_log['source_file'] = filename

        # DATA CHECKS
        datacheck = df[(df['date'] != dataDate)]

        if len(datacheck) > 0:
            print("Removing {0} entries that do not conform {1} {2}".format(len(datacheck), ticker, dataDatePrint))
            logger.info("Removing {0} entries that do not conform {1} {2}".format(len(datacheck), ticker, dataDatePrint))
            df = df[(df['date'] == dataDate)]

        # print("Processing {0} {1}: {2}".format(ticker, dataDatePrint, len(df)))
        logger.info("Processing {0} {1}: {2}".format(ticker, dataDatePrint, len(df)))

        # READ IN 5 DAY OI
        if prev_date_5 != 0:
            try:
                df = self.updatePrev5OI_test(prev_date_5, df)
            except Exception as e:
                print("Update Prev 5 Day OI: ERROR - {0} {1} - {2}: {3}".format(ticker, dataDate, prev_date_5, e))
                logger.info("Update Prev 5 Day OI: ERROR - {0} {1} - {2}: {3}".format(ticker, dataDate, prev_date_5, e))

        # PREV DAY OI
        if prev_date != 0:
            try:
                # df = self.updatePrevOI(prev_date, df)
                df = self.updatePrevOITest(prev_date, df)
            except Exception as e:
                print("Update Prev Day OI: ERROR - {0} {1} - {2}: {3}".format(ticker, dataDate, prev_date, e))
                logger.info("Update Prev Day OI: ERROR - {0} {1} - {2}: {3}".format(ticker, dataDate, prev_date, e))

        # UPLOAD DATA
        try:
            self.uploadData(dataDate, df)

        except Exception as e:
            print("Upload Data: ERROR - {0} {1}: {2}".format(ticker, dataDate.strftime('%m/%d/%Y'), e))
            logger.info("Upload Data: ERROR - {0} {1}: {2}".format(ticker, dataDate.strftime('%m/%d/%Y'), e))

        # OPTION STAT SUMMARY
        try:
            self.uploadOptionStat(dataDate, df)
        except Exception as e:
            print("Upload Option Stat: ERROR - {0} {1}: {2] ", ticker, dataDate.strftime('%m/%d/%Y'), e)
            logger.info("Upload Option Stat: ERROR - {0} {1}: {2] ", ticker, dataDate.strftime('%m/%d/%Y'), e)

        # UNDERLYING
        #     try:
        #         underlying_exists = connection_options.execute("SELECT exists( select * FROM underlying_data where symbol = '{0}' and date = '{1}')".format(ticker, dataDate.strftime('%Y-%m-%d'))).fetchone()[0]
        #         if not underlying_exists:
        #             print("Fetch Prices for {0} {1} Exists = {2}".format(ticker, dataDate, underlying_exists))
        #             DataManager.fetchUnderlyingMS(DataManager(), ticker, date_length='compact')
        #
        #     except Exception as e:
        #         print("Underlying ERROR: {0} (1)".format(ticker,dataDate))
        #         logger.info("Underlying ERROR: {0} (1)".format(ticker,dataDate))

        # UPDATE PROCESS TICKER LOG AFTER PROCESSING
        try:
            tickerlog_start = time.time()
            ticker_end = time.time()

            # print(ticker_log)
            tickerlog_end = time.time()

            print("{0} ticker_log | Updated {1} | {2}".format(ticker, pd.datetime.now().strftime('%m/%d/%Y'), tickerlog_end - tickerlog_start))
            logger.info("{0} ticker_log | Updated {1}".format(ticker, pd.datetime.now().strftime('%m/%d/%Y'), tickerlog_end - tickerlog_start))

        except Exception as e:
            print('UPDATE PROCESS LOG: ', e, filename)
            logger.info('UPDATE PROCESS LOG: ', e, filename)
            connection_options.dispose()

        # WRAP UP
        process = psutil.Process(os.getpid())
        print("{0} Process | {1} | {2} Rec | {3} {4} | SUCCESS | {5}".format(ticker, dataDatePrint, len(df), (process.memory_info().rss / 1048576), "MB", ticker_end - ticker_start))
        logger.info("{0} Process | {1} | {2} Rec | {3} {4} | SUCCESS | {5}".format(ticker, dataDatePrint, len(df), (process.memory_info().rss / 1048576), "MB", ticker_end - ticker_start))
        connection_options.dispose()



    def processTickers(self, filename, prev_date, prev_date_file, prev_date_5, prev_date_5_file, df):
        if 'company_name' in df.columns:
            df.drop(columns='company_name', inplace=True)

        ticker = df['symbol'].iloc[0]
        dataDate = df['date'].max()
        dataDatePrint = dataDate.strftime('%m/%d/%Y')
        ticker_start = time.time()

        print("{0} Processing | Day-1: {1} {2} | Day-5: {3} {4}".format(ticker,prev_date, prev_date_file, prev_date_5, prev_date_5_file))

        connection_options = create_engine('postgresql://postgres:inkstain@localhost:5432/wzyy_options')

        # CREATE TICKER LOG
        ticker_log_cols = ['symbol', 'source_date', 'source_file', 'upload_date', 'record_count', 'prev_oi_update', 'prev_oi_update_file',
                           'prev_5day_oi_update', 'prev_5day_oi_update_file', 'stat_date', 'flag_date','process_time']
        ticker_log = pd.DataFrame(columns=ticker_log_cols, index=[])
        ticker_log.loc[len(ticker_log)] = np.repeat(np.nan, len(ticker_log_cols))
        ticker_log['symbol'] = ticker
        ticker_log['source_date'] = dataDate
        ticker_log['source_file'] = filename

    # DATA CHECKS
        datacheck = df[((df['symbol'] != ticker) | (df['date'] != dataDate))]

        if len(datacheck) > 0:
            print("Removing {0} entries that do not conform {1} {2}".format(len(datacheck), ticker, dataDatePrint))
            logger.info("Removing {0} entries that do not conform {1} {2}".format(len(datacheck), ticker, dataDatePrint))
            df = df[(df['symbol'] == ticker | df['date'] == dataDate)]

        # print("Processing {0} {1}: {2}".format(ticker, dataDatePrint, len(df)))
        logger.info("Processing {0} {1}: {2}".format(ticker, dataDatePrint, len(df)))

    # READ IN 5 DAY OI
        if prev_date_5 != 0:
            try:
                df = self.updatePrev5OI(prev_date_5, df)
                ticker_log['prev_5day_oi_update'] = dt.datetime.now()
                ticker_log['prev_5day_oi_update_file'] = prev_date_5_file
            except Exception as e:
                print("Update Prev 5 Day OI: ERROR - {0} {1} - {2}: {3}".format(ticker, dataDate,prev_date_5,e))
                logger.info("Update Prev 5 Day OI: ERROR - {0} {1} - {2}: {3}".format(ticker, dataDate,prev_date_5,e))

    # PREV DAY OI
        if prev_date != 0:
            try:
                # df = self.updatePrevOI(prev_date, df)
                df = self.updatePrevOITest(prev_date,df)
                ticker_log['prev_oi_update'] = dt.datetime.now()
                ticker_log['prev_oi_update_file'] = prev_date_file
            except Exception as e:
                print("Update Prev Day OI: ERROR - {0} {1} - {2}: {3}".format(ticker, dataDate,prev_date,e))
                logger.info("Update Prev Day OI: ERROR - {0} {1} - {2}: {3}".format(ticker, dataDate,prev_date,e))

    # UPLOAD DATA
        try:
            self.uploadData(dataDate, df)
            ticker_log['upload_date'] = dt.datetime.now()
            ticker_log['record_count'] = len(df)

        except Exception as e:
            print("Upload Data: ERROR - {0} {1}: {2}".format(ticker, dataDate.strftime('%m/%d/%Y'), e))
            logger.info("Upload Data: ERROR - {0} {1}: {2}".format(ticker, dataDate.strftime('%m/%d/%Y'), e))

    # OPTION STAT SUMMARY
        try:
            self.uploadOptionStat(dataDate, df)
            ticker_log['stat_date'] = dt.datetime.now()
        except Exception as e:
            print("Upload Option Stat: ERROR - {0} {1}: {2] ", ticker, dataDate.strftime('%m/%d/%Y'), e)
            logger.info("Upload Option Stat: ERROR - {0} {1}: {2] ", ticker, dataDate.strftime('%m/%d/%Y'), e)

    # UNDERLYING
    #     try:
    #         underlying_exists = connection_options.execute("SELECT exists( select * FROM underlying_data where symbol = '{0}' and date = '{1}')".format(ticker, dataDate.strftime('%Y-%m-%d'))).fetchone()[0]
    #         if not underlying_exists:
    #             print("Fetch Prices for {0} {1} Exists = {2}".format(ticker, dataDate, underlying_exists))
    #             DataManager.fetchUnderlyingMS(DataManager(), ticker, date_length='compact')
    #
    #     except Exception as e:
    #         print("Underlying ERROR: {0} (1)".format(ticker,dataDate))
    #         logger.info("Underlying ERROR: {0} (1)".format(ticker,dataDate))


    # UPDATE PROCESS TICKER LOG AFTER PROCESSING
        try:
            tickerlog_start = time.time()
            ticker_end = time.time()

            ticker_log['process_time'] = ticker_end - ticker_start

            ticker_log.to_sql('process_log_ticker', connection_options, if_exists='append', index=False)
            # print(ticker_log)
            tickerlog_end = time.time()

            print("{0} ticker_log | Updated {1} | {2}".format(ticker, pd.datetime.now().strftime('%m/%d/%Y'),tickerlog_end-tickerlog_start))
            logger.info("{0} ticker_log | Updated {1}".format(ticker, pd.datetime.now().strftime('%m/%d/%Y'),tickerlog_end-tickerlog_start))

        except Exception as e:
            print('UPDATE PROCESS LOG: ', e, filename)
            logger.info('UPDATE PROCESS LOG: ', e, filename)
            connection_options.dispose()

    # WRAP UP
        process = psutil.Process(os.getpid())
        print("{0} Process | {1} | {2} Rec | {3} {4} | SUCCESS | {5}".format(ticker, dataDatePrint, len(df),(process.memory_info().rss/1048576),"MB", ticker_end - ticker_start))
        logger.info("{0} Process | {1} | {2} Rec | {3} {4} | SUCCESS | {5}".format(ticker, dataDatePrint, len(df),(process.memory_info().rss/1048576),"MB",ticker_end - ticker_start))
        connection_options.dispose()

################################################################################################

    def updatePrevOITest(self, prev_date, df):
        # UPDATES POSTGRES DATABASE INSTEAD OF DELETING AND INSERTING
        # PREVIOUS OI UPDATE
        ticker = ""
        dataDate = df['date'].max()
        prev_date = prev_date
        prev_start = time.time()
        connection_options = create_engine('postgresql://postgres:inkstain@localhost:5432/wzyy_options')

        try:
            request = "SELECT date, option_symbol, volume, open_interest FROM option_data WHERE date = '{0}'".format(prev_date)
            df_prev = pd.read_sql(request, connection_options)
            check_df_prev = len(df_prev)

            # MERGE CURRENT NEW INTO PREVIOUS DAY AS NEW OI
            if len(df_prev) > 0:
                df_update_oi = df[['option_symbol', 'open_interest']]
                df_update_oi.columns = ["option_symbol", "open_interest_new"]
                df_prev = pd.merge(df_prev, df_update_oi, how='left', on=['option_symbol'])

                try:
                # CHECK FOR DATA HOLES

                # Volume check is to make sure it wasn't used to fully close out OI and that 0 OI was legit
                    df_prev['data_holes'] = df_prev.apply(lambda x: 1 if ((x['volume'] < (0.5*x['open_interest'])) & (x['open_interest'] > 0) & (x['open_interest_new'] == 0)) else 0, axis=1)
                    dataHoles = df_prev[df_prev['data_holes'] == 1]
                    print("DATA HOLES: ", len(dataHoles))

                    if len(dataHoles) > 0:
                        # print("Update Prev OI - Data Holes: {0} {1} | {2} Recs".format(ticker, dataDate, len(dataHoles)))

                        # EXPORT DATA HOLES FILE
                        datahole_file = 'Data_Holes.csv'
                        if os.path.isfile(datahole_file):
                            dataHoles.to_csv(datahole_file, mode='a', header=False)
                        else:
                            dataHoles.to_csv(datahole_file, header=True)
                            print("Create {0}".format(datahole_file))
                            logger.info("Create {0}".format(datahole_file))

                        # UPDATE DATA HOLES IN CURRENT DATA
                        for row in dataHoles.itertuples():

                            # Update Hole in Current Day Data
                            df_idx = df.index[df['option_symbol'] == row.option_symbol].tolist()
                            for index in df_idx:
                                # df.loc[index]['open_interest'] = row.open_interest
                                orig_oi = df.at[index, 'open_interest']
                                df.at[index, 'open_interest'] = row.open_interest
                                new_oi = df.at[index, 'open_interest']
                                # print("{0}| {1} | Orig OI {2} New OI {3}".format(row.option_symbol, row.date, orig_oi, new_oi))
                                logger.info("{0}| {1} | Orig OI {2} New OI {3}".format(row.option_symbol, row.date, orig_oi, new_oi))

                            # Update Hole in Previous Day OI
                            df_prev_idx = df_prev.index[df_prev['option_symbol'] == row.option_symbol].tolist()
                            # print(df_prev_idx)
                            for indexp in df_prev_idx:
                                # df_prev.loc[index]['open_interest_new'] = row.open_interest
                                orig_oi = df_prev.at[indexp, 'open_interest_new']
                                df_prev.at[indexp, 'open_interest_new'] = row.open_interest
                                new_oi = df_prev.at[indexp, 'open_interest_new']
                                # print("{0} PrevOI| {1} | Orig OI {2} New OI {3}".format(row.option_symbol, row.date, orig_oi, new_oi))
                                logger.info("{0} PrevOI| {1} | Orig OI {2} New OI {3}".format(row.option_symbol, row.date, orig_oi, new_oi))

                    df_prev['open_interest_change'] = df_prev.apply(lambda x: (x['open_interest_new']-x['open_interest']) if ~math.isnan(x['open_interest_new']) else 0, axis=1)
                    df_prev['open_interest_change'].fillna(0, inplace=True)

                    prev_OI_zero_count = len(df_prev) - df_prev['open_interest'].astype(bool).sum(axis=0)
                    new_OI_zero_count = len(df_prev) - df_prev['open_interest_new'].astype(bool).sum(axis=0)

                    print("{0} | {1} Rec| {2} OI_0 - {3} NewOI_0 | {4} Data Holes".format(dataDate.strftime('%Y-%m-%d'),len(df_prev),prev_OI_zero_count,new_OI_zero_count,len(dataHoles)))
                    logger.info("{0} | {1} Rec| {2} OI_0 - {3} NewOI_0 | {4} Data Holes".format(dataDate.strftime('%Y-%m-%d'),len(df_prev),prev_OI_zero_count,new_OI_zero_count,len(dataHoles)))

                except Exception as e:
                    print("{0} | Data Hole Error | {1}".format(dataDate.strftime('%Y-%m-%d'),e))
                    logger.info("{0} | Data Hole Error | {1}".format(dataDate.strftime('%Y-%m-%d'),e))


            # CHECK IF OI INCREASE > VOLUME
            #     try:
            #         # OK IF OI DECREASES MORE THAN VOLUME - Data not the cleanest, so putting a 50 vol buffer
            #         df_prev['OI_Check'] = df_prev.apply(lambda x: 1 if (abs(x['open_interest_change']) > (x['volume'] + 50)) else 0, axis=1)
            #         if df_prev['OI_Check'].sum() > 0:
            #             df_error = df_prev[df_prev['OI_Check'] > 0].copy()
            #             df_error['dif'] = df_error.apply(lambda x: abs((abs(x['open_interest_change']) - x['volume'])), axis=1)
            #
            #             print("{0} | OI Volume Errors: {1} | Vol: {2} ; OI Change:{3} | Max Error = {4} | {5} Tickers".format(dataDate.strftime('%Y-%m-%d'), len(df_error), df_error['volume'].sum(), df_error['open_interest_change'].sum(),df_error['dif'].max(), df_error['symbol'].nunique()))
            #             logger.info("{0} | OI Volume Errors: {1} | Vol: {2} ; OI Change:{3} | Max Error = {4} | {5} Tickers".format(dataDate.strftime('%Y-%m-%d'), len(df_error), df_error['volume'].sum(), df_error['open_interest_change'].sum(),
            #                                                                                                               df_error['dif'].max(), df_error['symbol'].nunique()))
            #
            #             # EXPORT DATA ERROR FILE
            #             dataerror_file = 'Data_Errors.csv'
            #             if os.path.isfile(dataerror_file):
            #                 df_error.to_csv(dataerror_file, mode='a', header=False)
            #             else:
            #                 df_error.to_csv(dataerror_file, header=True)
            #                 print("Create {0}".format(dataerror_file))
            #                 logger.info("Create {0}".format(dataerror_file))
            #             # df_prev[df_prev['OI_Check']>0].to_csv("Data_Errors\{0}_{1}_OICheckError.csv".format(ticker,prev_date))
            #
            #             # Fill in current data if vol and OI = 0
            #     except Exception as e:
            #         print("{0} | OI Volume Errors: PROCESS ERROR | {1}".format(dataDate.strftime('%Y-%m-%d'), e))
            #         logger.info("{0} | OI Volume Errors: PROCESS ERROR | {1}".format(dataDate.strftime('%Y-%m-%d'), e))

            else:
                print("Update PrevOI: NO PREV DATA {0} {1}".format(ticker, prev_date.strftime('%m/%d/%Y')))
                logger.info("Update PrevOI: NO PREV DATA {0} {1}".format(ticker, prev_date.strftime('%m/%d/%Y')))

        except Exception as e:
            print("UpdatePrevOI day-1 Process ERROR: {0} {1} {2}".format(ticker, dataDate.strftime('%m/%d/%Y'), e))
            logger.info("UpdatePrevOI day-1 Process ERROR: {0} {1} {2}".format(ticker, dataDate.strftime('%m/%d/%Y'), e))

    # # UPLOAD UPDATED PREV DAY DATA TO SQL
    #     try:
    #         df_prev.drop(columns=['data_holes'], inplace=True, errors='ignore')
    #         # df_prev.drop(columns=['OI_Check', 'data_holes'], inplace=True, errors='ignore')
    #         # df_prev.drop(columns=['data_holes'], inplace=True, errors='ignore')
    #         # DELETE AND REPLACE OPTION DATA FROM PREV DAY
    #         if check_df_prev != len(df_prev):
    #             print("Update Prev IO. Missing Rows? | {0} | {1} | {2} - {3} Rec".format(ticker, prev_date, len(df_prev), check_df_prev))
    #         else:
    #             connection_options.execute("delete from option_data where date = '{0}'".format(prev_date.strftime('%Y-%m-%d')))
    #
    #             print("{0} | Uploading PrevOI option_data | {1} Recs: {2} Tickers, {3} MB".format(dataDate, len(df_prev), df_prev['symbol'].nunique(), df_prev.memory_usage(index=True).sum() / 1048576))
    #             logger.info("{0} | Uploading PrevOI option_data | {1} Recs: {2} Tickers, {3} MB".format(dataDate, len(df_prev), df_prev['symbol'].nunique(), df_prev.memory_usage(index=True).sum() / 1048576))
    #
    #             df_prev.to_sql('option_data', connection_options, if_exists='append', index=False, chunksize= 50000)
    #
    #             prev_end = time.time()
    #             prev_OI_zero_count = len(df_prev) - df_prev['open_interest'].astype(bool).sum(axis=0)
    #             new_OI_zero_count = len(df_prev) - df_prev['open_interest_new'].astype(bool).sum(axis=0)
    #
    #             print("{0} | Updated Prev OI | {1} Rec| {2} OI_0 - {3} NewOI_0 | {4}".format(dataDate.strftime('%Y-%m-%d'),len(df_prev),prev_OI_zero_count,new_OI_zero_count,prev_end-prev_start))
    #             logger.info("{0} | Updated Prev OI | {1} Rec| {2} OI_0 - {3} NewOI_0 | {4}".format(dataDate.strftime('%Y-%m-%d'),len(df_prev),prev_OI_zero_count,new_OI_zero_count,prev_end-prev_start))
    #
    #
    #     except Exception as e:
    #         print("Upload Day-1 Data: ERROR: {0} {1} {2}".format(ticker, prev_date, e))
    #         logger.info("Upload Day-1 Data: ERROR: {0} {1} {2}".format(ticker, prev_date, e))
    #         connection_options.dispose()
    #
    #     finally:
    #         connection_options.dispose()
    #         return df
    #

    # UPLOAD UPDATED DAY-1 DATA TO SQL - UPLOADING TEMP DATA TABLE AND UPDATING DATA FROM POSTGRES
        try:
            if check_df_prev != len(df_prev):
                print("Update Prev IO. Missing Rows? | {0} | {1} | {2} - {3} Rec".format(ticker, prev_date, len(df_prev), check_df_prev))
            else:
                temp_tablename = "temp_" + dataDate.strftime('%m/%d/%Y')

            # CREATE TEMP TABLE
                create_command = """
                    CREATE TABLE {0} (
                        date date,
                        option_symbol varchar,
                        open_interest_new integer,
                        open_interest_change integer
                    )
                    """.format(temp_tablename)
                try:
                    connection_options.execute(create_command)
                except Exception as e:
                    print("Create Temp Table ERROR | {0} {1} | {2} | {3}".format(ticker, prev_date, temp_tablename, e))

                try:
                    prev_OI_zero_count = len(df_prev) - df_prev['open_interest'].astype(bool).sum(axis=0)
                    new_OI_zero_count = len(df_prev) - df_prev['open_interest_new'].astype(bool).sum(axis=0)
                    df_prev.drop(columns=['volume', 'open_interest', 'data_holes'], inplace=True, errors='ignore')
                    df_prev.to_sql(temp_tablename, connection_options, if_exists='replace', index=False)
                except Exception as e:
                    print("Upload Temp Table ERROR | {0} {1} | {2} | {3}".format(ticker, prev_date, temp_tablename, e))

                # UPDATE DATA WITH TEMP TABLE
                update_command = """
                    UPDATE option_data
                        SET 
                        open_interest_new = {0}.open_interest_new
                        open_interest_change = {0}.open_interest_change
                        FROM {0}
                        WHERE {0}.option_symbol = option_data.option_symbol
                        AND {0}.date = option_data.date
                """.format(temp_tablename)

                try:
                    connection_options.execute(update_command)
                except Exception as e:
                    print("Update Date ERROR | {0} {1} | {2} | {3}".format(ticker, prev_date, temp_tablename, e))


            # DELETE TEMP TABLE
                try:
                    connection_options.execute("drop table if exists {0}".format(temp_tablename))
                except Exception as e:
                    print("Delete Temp Table ERROR | {0} {1} | {2} | {3}".format(ticker, prev_date, temp_tablename, e))

                prev_end = time.time()

                print("{0} | Prev OI Uploaded| {1} | {2} 0 OIs | {3} 0 New OIs | Time: {4}".format(ticker, prev_date.strftime('%m/%d/%Y'),prev_OI_zero_count, new_OI_zero_count,prev_end-prev_start))
                logger.info("{0} | Prev OI Uploaded| {1} | {2} 0 OIs | {3} 0 New OIs | Time: {4}".format(ticker, prev_date.strftime('%m/%d/%Y'),prev_OI_zero_count, new_OI_zero_count,prev_end-prev_start))

        except Exception as e:
            print("Upload Day-1 Data: ERROR: {0} {1} {2}".format(ticker, prev_date, e))
            logger.info("Upload Day-1 Data: ERROR: {0} {1} {2}".format(ticker, prev_date, e))
            connection_options.dispose()

        finally:
            connection_options.dispose()
            return df

    def updatePrevOITestOld(self, prev_date, df):
        # UPDATES POSTGRES DATABASE INSTEAD OF DELETING AND INSERTING
        # PREVIOUS OI UPDATE
        ticker = ""
        dataDate = df['date'].max()
        prev_date = prev_date
        prev_start = time.time()
        connection_options = create_engine('postgresql://postgres:inkstain@localhost:5432/wzyy_options')

        try:
            request = "SELECT * FROM option_data WHERE date = '{0}'".format(prev_date)
            df_prev = pd.read_sql(request, connection_options)
            check_df_prev = len(df_prev)

            # MERGE CURRENT NEW INTO PREVIOUS DAY AS NEW OI
            if len(df_prev) > 0:
                df_prev.drop(columns=['open_interest_new', 'open_interest_change'], inplace=True)

                df_update_oi = df[['option_symbol', 'open_interest']]
                df_update_oi.columns = ["option_symbol", "open_interest_new"]
                df_prev = pd.merge(df_prev, df_update_oi, how='left', on=['option_symbol'])

                try:
                    # CHECK FOR DATA HOLES

                    #     Volume check is to make sure it wasn't used to fully close out OI and that 0 OI was legit
                    df_prev['data_holes'] = df_prev.apply(lambda x: 1 if ((x['volume'] < (0.5 * x['open_interest'])) & (x['open_interest'] > 0) & (x['open_interest_new'] == 0)) else 0, axis=1)
                    dataHoles = df_prev[df_prev['data_holes'] == 1]
                    print("DATA HOLES: ", len(dataHoles))

                    if len(dataHoles) > 0:
                        # print("Update Prev OI - Data Holes: {0} {1} | {2} Recs".format(ticker, dataDate, len(dataHoles)))

                        # EXPORT DATA HOLES FILE
                        datahole_file = 'Data_Holes.csv'
                        if os.path.isfile(datahole_file):
                            dataHoles.to_csv(datahole_file, mode='a', header=False)
                        else:
                            dataHoles.to_csv(datahole_file, header=True)
                            print("Create {0}".format(datahole_file))
                            logger.info("Create {0}".format(datahole_file))

                        # UPDATE DATA HOLES IN CURRENT DATA
                        for row in dataHoles.itertuples():

                            # Update Hole in Current Day Data
                            df_idx = df.index[df['option_symbol'] == row.option_symbol].tolist()
                            for index in df_idx:
                                # df.loc[index]['open_interest'] = row.open_interest
                                orig_oi = df.at[index, 'open_interest']
                                df.at[index, 'open_interest'] = row.open_interest
                                new_oi = df.at[index, 'open_interest']
                                # print("{0}| {1} | Orig OI {2} New OI {3}".format(row.option_symbol, row.date, orig_oi, new_oi))
                                logger.info("{0}| {1} | Orig OI {2} New OI {3}".format(row.option_symbol, row.date, orig_oi, new_oi))

                            # Update Hole in Previous Day OI
                            df_prev_idx = df_prev.index[df_prev['option_symbol'] == row.option_symbol].tolist()
                            # print(df_prev_idx)
                            for indexp in df_prev_idx:
                                # df_prev.loc[index]['open_interest_new'] = row.open_interest
                                orig_oi = df_prev.at[indexp, 'open_interest_new']
                                df_prev.at[indexp, 'open_interest_new'] = row.open_interest
                                new_oi = df_prev.at[indexp, 'open_interest_new']
                                # print("{0} PrevOI| {1} | Orig OI {2} New OI {3}".format(row.option_symbol, row.date, orig_oi, new_oi))
                                logger.info("{0} PrevOI| {1} | Orig OI {2} New OI {3}".format(row.option_symbol, row.date, orig_oi, new_oi))

                    df_prev['open_interest_change'] = df_prev.apply(lambda x: (x['open_interest_new'] - x['open_interest']) if ~math.isnan(x['open_interest_new']) else 0, axis=1)
                    df_prev['open_interest_change'].fillna(0, inplace=True)

                    prev_OI_zero_count = len(df_prev) - df_prev['open_interest'].astype(bool).sum(axis=0)
                    new_OI_zero_count = len(df_prev) - df_prev['open_interest_new'].astype(bool).sum(axis=0)

                    print("{0} | {1} Rec| {2} OI_0 - {3} NewOI_0 | {4} Data Holes".format(dataDate.strftime('%Y-%m-%d'), len(df_prev), prev_OI_zero_count, new_OI_zero_count, len(dataHoles)))
                    logger.info("{0} | {1} Rec| {2} OI_0 - {3} NewOI_0 | {4} Data Holes".format(dataDate.strftime('%Y-%m-%d'), len(df_prev), prev_OI_zero_count, new_OI_zero_count, len(dataHoles)))

                except Exception as e:
                    print("{0} | Data Hole Error | {1}".format(dataDate.strftime('%Y-%m-%d'), e))
                    logger.info("{0} | Data Hole Error | {1}".format(dataDate.strftime('%Y-%m-%d'), e))


            # CHECK IF OI INCREASE > VOLUME
            #     try:
            #         # OK IF OI DECREASES MORE THAN VOLUME - Data not the cleanest, so putting a 50 vol buffer
            #         df_prev['OI_Check'] = df_prev.apply(lambda x: 1 if (abs(x['open_interest_change']) > (x['volume'] + 50)) else 0, axis=1)
            #         if df_prev['OI_Check'].sum() > 0:
            #             df_error = df_prev[df_prev['OI_Check'] > 0].copy()
            #             df_error['dif'] = df_error.apply(lambda x: abs((abs(x['open_interest_change']) - x['volume'])), axis=1)
            #
            #             print("{0} | OI Volume Errors: {1} | Vol: {2} ; OI Change:{3} | Max Error = {4} | {5} Tickers".format(dataDate.strftime('%Y-%m-%d'), len(df_error), df_error['volume'].sum(), df_error['open_interest_change'].sum(),df_error['dif'].max(), df_error['symbol'].nunique()))
            #             logger.info("{0} | OI Volume Errors: {1} | Vol: {2} ; OI Change:{3} | Max Error = {4} | {5} Tickers".format(dataDate.strftime('%Y-%m-%d'), len(df_error), df_error['volume'].sum(), df_error['open_interest_change'].sum(),
            #                                                                                                               df_error['dif'].max(), df_error['symbol'].nunique()))
            #
            #             # EXPORT DATA ERROR FILE
            #             dataerror_file = 'Data_Errors.csv'
            #             if os.path.isfile(dataerror_file):
            #                 df_error.to_csv(dataerror_file, mode='a', header=False)
            #             else:
            #                 df_error.to_csv(dataerror_file, header=True)
            #                 print("Create {0}".format(dataerror_file))
            #                 logger.info("Create {0}".format(dataerror_file))
            #             # df_prev[df_prev['OI_Check']>0].to_csv("Data_Errors\{0}_{1}_OICheckError.csv".format(ticker,prev_date))
            #
            #             # Fill in current data if vol and OI = 0
            #     except Exception as e:
            #         print("{0} | OI Volume Errors: PROCESS ERROR | {1}".format(dataDate.strftime('%Y-%m-%d'), e))
            #         logger.info("{0} | OI Volume Errors: PROCESS ERROR | {1}".format(dataDate.strftime('%Y-%m-%d'), e))

            else:
                print("Update PrevOI: NO PREV DATA {0} {1}".format(ticker, prev_date.strftime('%m/%d/%Y')))
                logger.info("Update PrevOI: NO PREV DATA {0} {1}".format(ticker, prev_date.strftime('%m/%d/%Y')))

        except Exception as e:
            print("UpdatePrevOI day-1 Process ERROR: {0} {1} {2}".format(ticker, dataDate.strftime('%m/%d/%Y'), e))
            logger.info("UpdatePrevOI day-1 Process ERROR: {0} {1} {2}".format(ticker, dataDate.strftime('%m/%d/%Y'), e))

        # UPLOAD UPDATED PREV DAY DATA TO SQL
        try:
            df_prev.drop(columns=['data_holes'], inplace=True, errors='ignore')
            # df_prev.drop(columns=['OI_Check', 'data_holes'], inplace=True, errors='ignore')
            # df_prev.drop(columns=['data_holes'], inplace=True, errors='ignore')
            # DELETE AND REPLACE OPTION DATA FROM PREV DAY
            if check_df_prev != len(df_prev):
                print("Update Prev IO. Missing Rows? | {0} | {1} | {2} - {3} Rec".format(ticker, prev_date, len(df_prev), check_df_prev))
            else:
                connection_options.execute("delete from option_data where date = '{0}'".format(prev_date.strftime('%Y-%m-%d')))

                print("{0} | Uploading PrevOI option_data | {1} Recs: {2} Tickers, {3} MB".format(dataDate, len(df_prev), df_prev['symbol'].nunique(), df_prev.memory_usage(index=True).sum() / 1048576))
                logger.info("{0} | Uploading PrevOI option_data | {1} Recs: {2} Tickers, {3} MB".format(dataDate, len(df_prev), df_prev['symbol'].nunique(), df_prev.memory_usage(index=True).sum() / 1048576))

                df_prev.to_sql('option_data', connection_options, if_exists='append', index=False, chunksize=50000)

                prev_end = time.time()
                prev_OI_zero_count = len(df_prev) - df_prev['open_interest'].astype(bool).sum(axis=0)
                new_OI_zero_count = len(df_prev) - df_prev['open_interest_new'].astype(bool).sum(axis=0)

                print("{0} | Updated Prev OI | {1} Rec| {2} OI_0 - {3} NewOI_0 | {4}".format(dataDate.strftime('%Y-%m-%d'), len(df_prev), prev_OI_zero_count, new_OI_zero_count, prev_end - prev_start))
                logger.info("{0} | Updated Prev OI | {1} Rec| {2} OI_0 - {3} NewOI_0 | {4}".format(dataDate.strftime('%Y-%m-%d'), len(df_prev), prev_OI_zero_count, new_OI_zero_count, prev_end - prev_start))


        except Exception as e:
            print("Upload Day-1 Data: ERROR: {0} {1} {2}".format(ticker, prev_date, e))
            logger.info("Upload Day-1 Data: ERROR: {0} {1} {2}".format(ticker, prev_date, e))
            connection_options.dispose()

        finally:
            connection_options.dispose()
            return df

        # UPLOAD UPDATED DAY-1 DATA TO SQL
        # try:
        #     # DELETE AND REPLACE OPTION DATA FROM PREV DAY
        #     if check_df_prev != len(df_prev):
        #         print("Update Prev IO. Missing Rows? | {0} | {1} | {2} - {3} Rec".format(ticker, prev_date, len(df_prev), check_df_prev))
        #     else:
        #         temp_tablename = "temp_" + dataDate.strftime('%m/%d/%Y')
        #
        #         # CREATE TEMP TABLE
        #         create_command = """
        #              CREATE TABLE {0} (
        #                  date date,
        #                  option_symbol varchar,
        #                  open_interest_new integer,
        #                  open_interest_change integer
        #              )
        #              """.format(temp_tablename)
        #         try:
        #             connection_options.execute(create_command)
        #         except Exception as e:
        #             print("Create Temp Table ERROR | {0} {1} | {2} | {3}".format(ticker, prev_date, temp_tablename, e))
        #
        #         try:
        #             prev_OI_zero_count = len(df_prev) - df_prev['open_interest'].astype(bool).sum(axis=0)
        #             new_OI_zero_count = len(df_prev) - df_prev['open_interest_new'].astype(bool).sum(axis=0)
        #             df_prev.drop(columns=['volume', 'open_interest', 'data_holes'], inplace=True, errors='ignore')
        #             df_prev.to_sql(temp_tablename, connection_options, if_exists='replace', index=False)
        #         except Exception as e:
        #             print("Upload Temp Table ERROR | {0} {1} | {2} | {3}".format(ticker, prev_date, temp_tablename, e))
        #
        #         # UPDATE DATA WITH TEMP TABLE
        #         update_command = """
        #              UPDATE option_data
        #                  SET open_interest_new = {0}.open_interest_new
        #                  FROM {0}
        #                  WHERE {0}.option_symbol = option_data.option_symbol
        #                  AND {0}.date = option_data.date
        #          """.format(temp_tablename)
        #
        #         try:
        #             connection_options.execute(update_command)
        #         except Exception as e:
        #             print("Update Date ERROR | {0} {1} | {2} | {3}".format(ticker, prev_date, temp_tablename, e))
        #
        #         # DELETE TEMP TABLE
        #         try:
        #             connection_options.execute("drop table if exists {0}".format(temp_tablename))
        #         except Exception as e:
        #             print("Delete Temp Table ERROR | {0} {1} | {2} | {3}".format(ticker, prev_date, temp_tablename, e))
        #
        #         prev_end = time.time()
        #
        #         print("{0} | Prev OI Uploaded| {1} | {2} 0 OIs | {3} 0 New OIs | Time: {4}".format(ticker, prev_date.strftime('%m/%d/%Y'), prev_OI_zero_count, new_OI_zero_count, prev_end - prev_start))
        #         logger.info("{0} | Prev OI Uploaded| {1} | {2} 0 OIs | {3} 0 New OIs | Time: {4}".format(ticker, prev_date.strftime('%m/%d/%Y'), prev_OI_zero_count, new_OI_zero_count, prev_end - prev_start))
        #
        # except Exception as e:
        #     print("Upload Day-1 Data: ERROR: {0} {1} {2}".format(ticker, prev_date, e))
        #     logger.info("Upload Day-1 Data: ERROR: {0} {1} {2}".format(ticker, prev_date, e))
        #     connection_options.dispose()
        #
        # finally:
        #     connection_options.dispose()
        #     return df

    def updatePrevOI(self, prev_date, df):

        # PREVIOUS OI UPDATE
        ticker = df['symbol'].iloc[0]
        dataDate = df['date'].iloc[0]
        prev_date = prev_date
        prev_start = time.time()
        connection_options = create_engine('postgresql://postgres:inkstain@localhost:5432/wzyy_options')

        # READ IN DAY-1 DATA
        try:
            request = "SELECT * FROM option_data WHERE symbol = '{0}' and date = '{1}'".format(ticker, prev_date)
            df_prev = pd.read_sql(request, connection_options)
            check_df_prev= len(df_prev)

            # UPDATE NEW OI FOR DAY-1 DATA BASED ON OPTION SYMBOL
            if len(df_prev) > 0:
                df_prev.drop(columns=['open_interest_new', 'open_interest_change'], inplace=True)
                df_update_oi = df[['option_symbol', 'open_interest']].copy()
                df_update_oi.columns = ["option_symbol", "open_interest_new"]
                df_prev = pd.merge(df_prev, df_update_oi, how='left', on=['option_symbol'])

            # CHECK FOR DATA HOLES
                df_prev['data_holes'] = df_prev.apply(lambda x: 1 if ((x['volume'] < (0.5*x['open_interest'])) & (x['open_interest'] > 0) & (x['open_interest_new'] == 0)) else 0, axis=1)
                dataHoles = df_prev[df_prev['data_holes'] == 1]
                # print("DATA HOLES: ", len(dataHoles))
                if len(dataHoles) > 0:
                    print("Update Prev OI - Data Holes: {0} {1} | {2} Recs".format(ticker, dataDate, len(dataHoles)))
                    datahole_file = 'Data_Holes.csv'
                    if os.path.isfile(datahole_file):
                        dataHoles.to_csv(datahole_file, mode='a', header=False)
                    else:
                        dataHoles.to_csv(datahole_file, header=True)
                        print("Create ", datahole_file)

                    for row in dataHoles.itertuples():
                        # print(row.option_symbol, row.open_interest)
                        # print(df.head())
                        # Update Hole in Current Day Data
                        df_idx = df.index[df['option_symbol'] == row.option_symbol].tolist()
                        # print(df_idx)
                        for index in df_idx:
                            # df.loc[index]['open_interest'] = row.open_interest
                            orig_oi = df.at[index, 'open_interest']
                            df.at[index, 'open_interest'] = row.open_interest
                            new_oi = df.at[index, 'open_interest']
                            print("{0}| {1} | Orig OI {2} New OI {3}".format(row.option_symbol, row.date, orig_oi, new_oi))
                            logger.info("{0}| {1} | Orig OI {2} New OI {3}".format(row.option_symbol, row.date, orig_oi, new_oi))

                        # Update Hole in Previous Day OI
                        df_prev_idx = df_prev.index[df_prev['option_symbol'] == row.option_symbol].tolist()
                        # print(df_prev_idx)
                        for indexp in df_prev_idx:
                            # df_prev.loc[index]['open_interest_new'] = row.open_interest
                            orig_oi = df_prev.at[indexp, 'open_interest_new']
                            df_prev.at[indexp, 'open_interest_new'] = row.open_interest
                            new_oi = df_prev.at[indexp, 'open_interest_new']
                            print("{0} PrevOI| {1} | Orig OI {2} New OI {3}".format(row.option_symbol, row.date, orig_oi, new_oi))
                            logger.info("{0} PrevOI| {1} | Orig OI {2} New OI {3}".format(row.option_symbol, row.date, orig_oi, new_oi))

                df_prev['open_interest_change'] = df_prev.apply(lambda x: (x['open_interest_new']-x['open_interest']) if ~math.isnan(x['open_interest_new']) else 0, axis=1)
                df_prev['open_interest_change'].fillna(0, inplace=True)

                print("{0} | {1} Rec| {4}: {5} total_OI| {2}: {3} total_OI_prevday | {4}".format(ticker, len(df_prev), prev_date, df_prev['open_interest'].sum(), dataDate.strftime('%Y-%m-%d'), df_prev['open_interest_new'].sum()))
                logger.info("{0} | {1} Rec| {4}: {5} total_OI| {2}: {3} total_OI_prevday | {4} ".format(ticker, len(df_prev), prev_date, df_prev['open_interest'].sum(), dataDate.strftime('%Y-%m-%d'), df_prev['open_interest_new'].sum()))

            # CHECK IF OI INCREASE > VOLUME
                # OK IF OI DECREASES MORE THAN VOLUME - Data not the cleanest, so putting a 50 vol buffer
                df_prev['OI_Check'] = df_prev.apply(lambda x: 1 if ((x['open_interest_change']) > (x['volume'] + 50)) else 0, axis=1)
                if df_prev['OI_Check'].sum() > 0:
                    print("{0} New OI Update Errors {1}".format(ticker, df_prev['OI_Check'].sum()))
                    dataerror_file = 'Data_Errors.csv'
                    if os.path.isfile(dataerror_file):
                        df_prev[df_prev['OI_Check'] > 0].to_csv(dataerror_file, mode='a', header=False)
                    else:
                        df_prev[df_prev['OI_Check'] > 0].to_csv(dataerror_file, header=True)
                        print("Create ", dataerror_file)
                    # df_prev[df_prev['OI_Check']>0].to_csv("Data_Errors\{0}_{1}_OICheckError.csv".format(ticker,prev_date))

                    # Fill in current data if vol and OI = 0

            else:
                print("Update PrevOI: NO PREV DATA {0} {1}".format(ticker, prev_date.strftime('%m/%d/%Y')))
                logger.info("Update PrevOI: NO PREV DATA {0} {1}".format(ticker, prev_date.strftime('%m/%d/%Y')))

        except Exception as e:
            print("UpdatePrevOI day-1 Process ERROR: {0} {1} {2}".format(ticker, dataDate.strftime('%m/%d/%Y'), e))
            logger.info("UpdatePrevOI day-1 Process ERROR: {0} {1} {2}".format(ticker, dataDate.strftime('%m/%d/%Y'), e))

    # UPLOAD UPDATED DAY-1 DATA TO SQL
        try:
            df_prev.drop(columns=['OI_Check', 'data_holes'], inplace=True, errors='ignore')
            # DELETE AND REPLACE OPTION DATA FROM PREV DAY
            if check_df_prev != len(df_prev):
                print("Update Prev IO. Missing Rows? | {0} | {1} | {2} - {3} Rec".format(ticker, prev_date, len(df_prev), check_df_prev))
            else:
                connection_options.execute("delete from option_data where date = '{1}' AND symbol = '{0}'".format(ticker, prev_date.strftime('%Y-%m-%d')))
                self.removeOptionDataIndex()
                df_prev.to_sql('option_data', connection_options, if_exists='append', index=False)
                self.addOptionDataIndex()

                prev_end = time.time()
                print("{0} | Prev OI Uploaded| {1} - {2} total_OI | {3} - {4} total_OI| Time: {5}".format(ticker, prev_date.strftime('%m/%d/%Y'),df_prev['open_interest'].sum(),dataDate, df_prev['open_interest_new'].sum(),prev_end-prev_start))
                logger.info("{0} | Prev OI Uploaded| {1} - {2} total_OI | {3} - {4} total_OI| Time: {5}".format(ticker, prev_date.strftime('%m/%d/%Y'),df_prev['open_interest'].sum(),dataDate, df_prev['open_interest_new'].sum(),prev_end-prev_start))

        except Exception as e:
            print("Upload Day-1 Data: ERROR: {0} {1} {2}".format(ticker, prev_date, e))
            logger.info("Upload Day-1 Data: ERROR: {0} {1} {2}".format(ticker, prev_date, e))
            connection_options.dispose()

        finally:
            connection_options.dispose()
            return df

            # connection_options = create_engine('postgresql://postgres:inkstain@localhost:5432/wzyy_options')
            # data_count = connection_options.execute("select date, count(date), SUM (open_interest_new) as open_interest_new from {0} group by date".format(tableName)).fetchall()
            # # print("AFTER UPDATE: ", data_count)
            # connection_options.dispose()

    def updatePrev5OI_test(self, prev_date_5, df):

        connection_options = create_engine('postgresql://postgres:inkstain@localhost:5432/wzyy_options')
        dataDate = df['date'].max()
        prev_date_5 = prev_date_5
        ticker = ""

        # READ IN DAY-5 DATA
        try:
            request = "SELECT option_symbol, open_interest FROM option_data WHERE date = '{0}'".format(prev_date_5)
            df_update_oi_5 = pd.read_sql(request, connection_options)

            # UPDATE NEW OI FOR DAY-1 DATA BASED ON OPTION SYMBOL
            if len(df_update_oi_5) > 0:
                df_update_oi_5.columns = ["option_symbol", "open_interest_5day"]
                df = pd.merge(df, df_update_oi_5, how='left', on=['option_symbol'])
                df['open_interest_5day_change'] = df.apply(lambda x: (x['open_interest'] - x['open_interest_5day']) if ~math.isnan(x['open_interest_5day']) else 0, axis=1)
                df['open_interest_5day_change'].fillna(0, inplace=True)
                print("{0}| {1} Rec| {2} {3} total_OI| {4} {5} total_OI_5day".format(dataDate.strftime('%m/%d/%Y'), len(df), dataDate.strftime('%m/%d/%Y'), df['open_interest'].sum(), prev_date_5.strftime('%m/%d/%Y'), df['open_interest_5day'].sum()))
                logger.info("{0}| {1} Rec| {2} {3} total_OI| {4} {5} total_OI_5day".format( dataDate.strftime('%m/%d/%Y'), len(df), dataDate.strftime('%m/%d/%Y'), df['open_interest'].sum(), prev_date_5.strftime('%m/%d/%Y'), df['open_interest_5day'].sum()))

            else:
                print("Update OI_5day: NO OI_5day DATA {0} {1}".format(ticker, prev_date_5.strftime('%m/%d/%Y')))
                logger.info("Update OI_5day: NO OI_5day DATA {0} {1}".format(ticker, prev_date_5.strftime('%m/%d/%Y')))

        except Exception as e:
            print("UpdatePrevOI ERROR, day-5: {0} {1} {2}".format(ticker, prev_date_5.strftime('%m/%d/%Y'), e))
            logger.info("UpdatePrevOI ERROR, day-5: {0} {1} {2}".format(ticker, prev_date_5.strftime('%m/%d/%Y'), e))
            connection_options.dispose()
        finally:
            return df
            connection_options.dispose()





    def updatePrev5OI(self, prev_date_5, df):

        connection_options = create_engine('postgresql://postgres:inkstain@localhost:5432/wzyy_options')
        ticker = df['symbol'].iloc[0]
        dataDate = df['date'].iloc[0]
        prev_date_5 = prev_date_5

        # READ IN DAY-5 DATA
        try:
            request = "SELECT option_symbol, open_interest FROM option_data WHERE symbol = '{0}' and date = '{1}'".format(ticker, prev_date_5)
            df_update_oi_5 = pd.read_sql(request, connection_options)

            # UPDATE NEW OI FOR DAY-1 DATA BASED ON OPTION SYMBOL
            if len(df_update_oi_5) > 0:
                df_update_oi_5.columns = ["option_symbol", "open_interest_5day"]
                df = pd.merge(df, df_update_oi_5, how='left', on=['option_symbol'])
                df['open_interest_5day_change'] = df.apply(lambda x: (x['open_interest'] - x['open_interest_5day']) if ~math.isnan(x['open_interest_5day']) else 0, axis=1)
                df['open_interest_5day_change'].fillna(0, inplace=True)
                print("{0}| {1} Rec| {2} {3} total_OI| {4} {5} total_OI_5day".format(ticker, len(df), dataDate.strftime('%m/%d/%Y'), df['open_interest'].sum(), prev_date_5.strftime('%m/%d/%Y'), df['open_interest_5day'].sum()))
                logger.info("{0}| {1} Rec| {2} {3} total_OI| {4} {5} total_OI_5day".format(ticker, len(df), dataDate.strftime('%m/%d/%Y'), df['open_interest'].sum(), prev_date_5.strftime('%m/%d/%Y'), df['open_interest_5day'].sum()))

            else:
                print("Update PrevOI: NO PREV 5 DAY DATA {0} {1}".format(ticker, prev_date_5.strftime('%m/%d/%Y')))
                logger.info("Update PrevOI: NO PREV 5 DAY DATA {0} {1}".format(ticker, prev_date_5.strftime('%m/%d/%Y')))

        except Exception as e:
            print("UpdatePrevOI ERROR, day-5: {0} {1} {2}".format(ticker, prev_date_5.strftime('%m/%d/%Y'), e))
            logger.info("UpdatePrevOI ERROR, day-5: {0} {1} {2}".format(ticker, prev_date_5.strftime('%m/%d/%Y'), e))
            connection_options.dispose()
        finally:
            return df
            connection_options.dispose()

    def uploadData(self, dataDate, df):
        ticker = df['symbol'].iloc[0]
        connection_options = create_engine('postgresql://postgres:inkstain@localhost:5432/wzyy_options')
        exists = connection_options.execute("select count(*) from option_data where symbol = '{0}' and date = '{1}'".format(ticker, dataDate)).fetchone()[0]
        upload_start = time.time()

        if (exists > 0):
            print("Data already exist: Skip Data Upload. {0} {1}: {2} Records. ".format(ticker, dataDate, exists))
            logger.info("Data already exist: Skip Data Upload. {0} {1}: {2} Records. ".format(ticker, dataDate, exists))
            connection_options.dispose()
        else:
            try:
                # pre_count = connection_options.execute("select count(*) from option_data where symbol = '{0}'".format(ticker)).fetchone()[0]
                # Upload Data to
                # SQL NOTES - "https://stackoverflow.com/questions/48006551/speeding-up-pandas-dataframe-to-sql-with-fast-executemany-of-pyodbc"

                print("{0} | Uploading option_data | {1} Recs: {2} Tickers, {3} MB".format(dataDate, len(df), df['symbol'].nunique(), df.memory_usage(index=True).sum()/1048576))
                logger.info("{0} | Uploading option_data | {1} Recs: {2} Tickers, {3} MB".format(dataDate, len(df), df['symbol'].nunique(), df.memory_usage(index=True).sum()/1048576))

                df.to_sql("option_data", connection_options, if_exists='append', index=False)
                # post_count = connection_options.execute("select count(*) from option_data where symbol = '{0}'".format(ticker)).fetchone()[0]

                upload_end = time.time()
                print("{0} | Uploaded option_data | {1} | {2} Add | {3}".format(ticker, dataDate, len(df),upload_end-upload_start))
                logger.info("{0} | Uploaded option_data | {1} | {2} Add | {3}".format(ticker, dataDate, len(df),upload_end-upload_start))

            except Exception as e:
                print("ERROR: Data Upload {0} {1} {2} {3}".format(ticker, dataDate, len(df), e))
                logger.info("ERROR: Data Upload {0} {1} {2} {3}".format(ticker, dataDate, len(df), e))
            finally:
                connection_options.dispose()

    def uploadDataTest(self, dataDate, df):
        ticker = ""
        connection_options = create_engine('postgresql://postgres:inkstain@localhost:5432/wzyy_options')
        exists = connection_options.execute("select count(*) from option_data where date = '{0}'".format(dataDate)).fetchone()[0]
        upload_start = time.time()

        if (exists > 0):
            print("Data already exist: Skip Data Upload. {0} {1}: {2} Records. ".format(ticker, dataDate, exists))
            logger.info("Data already exist: Skip Data Upload. {0} {1}: {2} Records. ".format(ticker, dataDate, exists))
            connection_options.dispose()
        else:
            try:
                # pre_count = connection_options.execute("select count(*) from option_data where symbol = '{0}'".format(ticker)).fetchone()[0]
                # Upload Data to SQL
                # self.removeOptionDataIndex()
                print("{0} | Uploading option_data | {1} Recs: {2} Tickers, {3} MB".format(dataDate, len(df), df['symbol'].nunique(), df.memory_usage(index=True).sum() / 1048576))
                logger.info("{0} | Uploading option_data | {1} Recs: {2} Tickers, {3} MB".format(dataDate, len(df), df['symbol'].nunique(), df.memory_usage(index=True).sum() / 1048576))

                df.to_sql("option_data", connection_options, if_exists='append', index=False, chunksize= 50000)
                # post_count = connection_options.execute("select count(*) from option_data where symbol = '{0}'".format(ticker)).fetchone()[0]

                upload_end = time.time()
                print("{0} | Uploaded option_data| {1} Rec | {2}".format(dataDate, len(df),upload_end-upload_start))
                logger.info("{0} | Uploaded option_data| {1} Rec | {2}".format(dataDate, len(df),upload_end-upload_start))
                # self.addOptionDataIndex()

            except Exception as e:
                print("ERROR: Data Upload | {0} | {1} Recs | {2}".format(dataDate.strftime('%m/%d/%Y'), len(df), e))
                logger.info("ERROR: Data Upload | {0} | {1} Recs | {2}".format(dataDate.strftime('%m/%d/%Y'), len(df), e))
            finally:
                connection_options.dispose()

    def uploadDataTestBulk(self, dataDate, df):
        ticker = ""
        connection_options = create_engine('postgresql://postgres:inkstain@localhost:5432/wzyy_options')
        exists = connection_options.execute("select count(*) from option_data where date = '{0}'".format(dataDate)).fetchone()[0]
        upload_start = time.time()

        if (exists > 0):
            print("Data already exist: Skip Data Upload. {0} {1}: {2} Records. ".format(ticker, dataDate, exists))
            logger.info("Data already exist: Skip Data Upload. {0} {1}: {2} Records. ".format(ticker, dataDate, exists))
            connection_options.dispose()
        else:
            try:
                # pre_count = connection_options.execute("select count(*) from option_data where symbol = '{0}'".format(ticker)).fetchone()[0]
                # Upload Data to SQL
                # self.removeOptionDataIndex()
                print("{0} | Uploading option_data | {1} Recs: {2} Tickers, {3} MB".format(dataDate, len(df), df['symbol'].nunique(), df.memory_usage(index=True).sum() / 1048576))
                logger.info("{0} | Uploading option_data | {1} Recs: {2} Tickers, {3} MB".format(dataDate, len(df), df['symbol'].nunique(), df.memory_usage(index=True).sum() / 1048576))

            # First Export Data to CSV
                df.to_csv("temp_upload.csv")





                # df.to_sql("option_data", connection_options, if_exists='append', index=False, chunksize= 50000)
                # # post_count = connection_options.execute("select count(*) from option_data where symbol = '{0}'".format(ticker)).fetchone()[0]
                #
                # upload_end = time.time()
                # print("{0} | Uploaded option_data| {1} Rec | {2}".format(dataDate, len(df),upload_end-upload_start))
                # logger.info("{0} | Uploaded option_data| {1} Rec | {2}".format(dataDate, len(df),upload_end-upload_start))
                # self.addOptionDataIndex()

            except Exception as e:
                print("ERROR: Data Upload | {0} | {1} Recs | {2}".format(dataDate.strftime('%m/%d/%Y'), len(df), e))
                logger.info("ERROR: Data Upload | {0} | {1} Recs | {2}".format(dataDate.strftime('%m/%d/%Y'), len(df), e))
            finally:
                connection_options.dispose()


    def uploadOptionStat(self, dataDate, df):
        ticker = df['symbol'].iloc[0]
        stats = OptionStats(ticker, dataDate, df).result
        upload_start = time.time()

        connection_stats = create_engine('postgresql://postgres:inkstain@localhost:5432/wzyy_options')
        exists = connection_stats.execute("select exists(select * from option_stat where symbol = '{0}' and date = '{1}')".format(ticker, dataDate.strftime('%Y-%m-%d'))).fetchone()[0]

        if exists:
            print("Data already exist {0} {1} in option_stat".format(ticker, dataDate))

        try:
            stats.to_sql('option_stat', connection_stats, if_exists='append', index=False)

            upload_end = time.time()
            print("{0} option_stat| {1}| {2} Add | {3}".format(ticker, dataDate, len(stats),upload_end-upload_start))
            logger.info("{0} option_stat| {1}| {2} Add | {3}".format(ticker, dataDate, len(stats),upload_end-upload_start))

        except Exception as e:
            print(e)
            print("Stats ERROR: Adding {0} {1} to option_stat".format(ticker, dataDate))
            logger.info("Stats ERROR: Adding {0} {1} to option_stat".format(ticker, dataDate))
            connection_stats.dispose()
        finally:
            connection_stats.dispose()

    def uploadOptionStatTest(self, dataDate, df):
        ticker = ""

        # df_grouped = df.groupby('symbol')
        stats = pd.DataFrame()
        for name, group in df.groupby('symbol'):
            # print(group.head())
            # stat_ind =
            # print(stat_ind.head())
            stats = stats.append(OptionStats(name, dataDate, group).result, sort=True)

        # print(stats.head())
        upload_start = time.time()

        connection_stats = create_engine('postgresql://postgres:inkstain@localhost:5432/wzyy_options')
        exists = connection_stats.execute("select exists(select * from option_stat where date = '{0}')".format(dataDate.strftime('%Y-%m-%d'))).fetchone()[0]

        if exists:
            print("Data already exist {0} {1} in option_stat".format(ticker, dataDate))

        try:
            stats.to_sql('option_stat', connection_stats, if_exists='append', index=False)
            upload_end = time.time()
            print("{0} | Uploaded option_stat| {1} Recs | {2}".format(dataDate, len(stats),upload_end-upload_start))
            logger.info("{0} | Uploaded option_stat| {1} Recs | {2}".format(dataDate, len(stats),upload_end-upload_start))

        except Exception as e:
            print(e)
            print("Stats ERROR: Adding {0} {1} to option_stat".format(ticker, dataDate))
            logger.info("Stats ERROR: Adding {0} {1} to option_stat".format(ticker, dataDate))
            connection_stats.dispose()
        finally:
            connection_stats.dispose()

    def addPrevOIChange(self, df, prev_date, column_name):
        # 5 DAY AGO DAY
        ticker = df['symbol'].iloc[0]
        connection_options = create_engine('postgresql://postgres:inkstain@localhost:5432/wzyy_options')
        try:
            request = "SELECT * FROM option_data WHERE symbol = '{0}' and date = '{1}'".format(ticker, prev_date)
            df_prev = pd.read_sql(request, connection_options)

            if (len(df_prev) > 0):
            # UPDATE NEW OI BASED ON OPTION SYMBOL
                df_prev_update = df_prev[['option_symbol', 'open_interest']].copy()
                df_prev_update.columns = ["option_symbol", 'open_interest_other']
                df = pd.merge(df, df_prev_update, how='left', on=['option_symbol'])
                df['open_interest_other'].fillna(0, inplace=True)
                df[column_name] = df['open_interest'] - df['open_interest_other']

            else:
                df[column_name] = 0

        except Exception as e:
            print("ERROR Reading {0} {1} {2}".format(ticker, column_name, prev_date), e)
            df[column_name] = 0
        finally:
            return df
            connection_options.dispose()

    def terminateConnections(self):
        commands = ("""SELECT pg_terminate_backend(pg_stat_activity.pid)
                    FROM pg_stat_activity
                    WHERE datname = 'wzyy_options'
                    AND pid <> pg_backend_pid();
                """)
        try:
            conn = psycopg2.connect("dbname = 'wzyy_options' user='postgres' host = 'localhost' password = 'inkstain'")
            cur = conn.cursor()
            cur.execute(commands)
            cur.close()
            conn.commit()
            conn.close()
            print("Terminate Connections")
        except (Exception, psycopg2.DatabaseERROR) as ERROR:
            print(ERROR)

    #    "Function Starts "



###################################################################################################################################

    def removeOptionDataIndex(self):
        index_start = time.time()
        connection_info = create_engine('postgresql://postgres:inkstain@localhost:5432/wzyy_options')

        try:
            connection_info.execute("DROP INDEX IF EXISTS option_data_index;")

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
            connection_info.execute("CREATE INDEX IF NOT EXISTS option_data_index ON option_data(date, option_symbol, symbol);")

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
            connection_info.execute("CREATE INDEX IF NOT EXISTS option_data_index ON option_data(date, symbol, option_symbol, );")
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

    # DataLoader().updateHistoricalFile("20180108_OData.csv", ticker)

    try:
        dataError_File = 'Data_Errors.csv'
        dataHole_File = 'Data_Holes.csv'
        if os.path.isfile(dataError_File):
            os.remove(dataError_File)
            print("Deleted ", dataError_File)
        if os.path.isfile(dataHole_File):
            os.remove(dataHole_File)
            print("Deleted ", dataHole_File)
        DataLoader.removeOptionDataIndex(DataLoader)
    except Exception as e:
        print(e)

    file_data_path = 'C:\\Users\\Yaos\\Desktop\\Trading\\OptionsData\\'
    timestart = time.time()

    df_files = pd.read_csv(file_data_path + 'File_Index.csv', encoding='ISO-8859-1', na_values=['Infinity','-Infinity'])
    df_files['filename_next'] = df_files['filename'].shift(-1)
    df_files['filename_5day'] = df_files['filename'].shift(5)



    for row in df_files.itertuples():
        if row.filename[:8] == '20180515':
            print("             ")
            try:
                DataLoader().loadOptionsHistoricaDoubleLoadTest(row.filename, row.filename_next, ticker)
            except Exception as e:
                now = dt.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
                print("Error Processing {0} | {1} | {2} ".format(row.filename, row.filename_next, now))
                logger.info("Error Processing {0} | {1} | {2}".format(row.filename,row.filename_next,now))

    DataLoader.terminateConnections(DataLoader)
    timeend = time.time()
    print("FINISH Files: SUCCESS | {0} - {1} | {2} Files | {3}".format(process_files[0], process_files[-1], len(process_files),timeend-timestart))
    logger.info("FINISH Files: SUCCESS | {0} - {1} | {2} Files | {3}".format(process_files[0], process_files[-1], len(process_files),timeend-timestart))



    # i = 0
    # # DataLoader.removeIndex(DataLoader)
    # try:
    #
    #     for filename in process_files:
    #         if filename[:4]=='2018':
    #             i += 1
    #             print("             ")
    #             logger.info("           ")
    #             print("Loading {0} {1}/{2}".format(filename, i, len(csvfiles)))
    #             logger.info("Loading {0} {1}/{2}".format(filename, i, len(csvfiles)))
    #             # DataLoader().loadOptionsHistorical(filename, ticker)
    #             try:
    #                 DataLoader().loadOptionsHistoricaTest(filename, ticker)
    #             except Exception as e:
    #                 now = dt.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
    #                 print("Error Processing {0} | {1}".format(filename, now))
    #                 logger.info("Error Processing {0} | {1}".format(filename,now))
    #                 break
    #         # if i == 10:
    #         #     break
    #
    # except Exception as e:
    #     print("Process Files: ERROR | {0} - {1} | {2} Files | {3}".format(process_files[0], process_files[-1], len(process_files),e))
    #     logger.info("Process Files: ERROR | {0} - {1} | {2} Files | {3}".format(process_files[0], process_files[-1], len(process_files),e))
    # finally:
    #     # DataLoader.addIndex(DataLoader)
    #     DataLoader.terminateConnections(DataLoader)
    #     gc.collect()
    #     process = psutil.Process(os.getpid())
    #     DataLoader.addOptionDataIndex(DataLoader)
    #     timeend = time.time()
    #
    #     print("FINISH Files: SUCCESS | {0} - {1} | {2} Files | {3}".format(process_files[0], process_files[-1], len(process_files),timeend-timestart))
    #     logger.info("FINISH Files: SUCCESS | {0} - {1} | {2} Files | {3}".format(process_files[0], process_files[-1], len(process_files),timeend-timestart))
    #
    #
    #
