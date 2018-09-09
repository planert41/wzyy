# -*- coding: utf-8 -*-
"""
Created on Mon Jul  2 12:32:02 2018

@author: Yaos
"""
# ALPHA VANTAGE API KEY PR2GF4AKJDDZ10XJ

import datetime as dt
import os
import numpy as np
import multiprocessing as mp
import logging
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.sql import text
import psycopg2
from sqlalchemy import create_engine
import time
from scipy import stats
from pandas import DataFrame
import operator
from Underlying import existingTickers
from Underlying import DataManager
import csv

import numpy
from datetime import timedelta
from psycopg2.extensions import register_adapter, AsIs
from multiprocessing import Pool
import multiprocessing
from pandas.tseries.offsets import BDay
from functools import partial


from multiprocessing import Process
def addapt_numpy_float64(numpy_float64):
  return AsIs(numpy_float64)

from LoadOptionFileUpdated import DataLoader as dl

option_database = 'postgresql://postgres:inkstain@localhost:5432/option_data'
wzyy_database = 'postgresql://postgres:inkstain@localhost:5432/wzyy_options'

today = str(dt.date.today()).replace('-', '')

logger = logging.getLogger(__name__)
if not len(logger.handlers):
    formatter = logging.Formatter('%(asctime)s %(filename)s: %(funcName)s: %(message)s')
    logger.setLevel(logging.DEBUG)
    fh = logging.FileHandler('OPTION_FLAG_' + today + '.log', mode='a')
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(formatter)
    logger.addHandler(fh)

class Stat:
    @staticmethod
    def cap_stats(df, window=1):
        try:
            stat_df = pd.DataFrame()
            stat_df['data'] = df
            stat_df['mean'] = stat_df['data'].rolling(window=window, min_periods=1).mean()
            stat_df['std'] = stat_df['data'].rolling(window=window, min_periods=1).std()

            # Cap Values at 3 Times STD
            stat_df['adj_val'] = stat_df.apply(lambda x: min(x['data'], x['mean'] + 3 * x['std']) if x['std'] > 0 else x['data'], axis=1)

            stat_df['adj_mean'] = stat_df['adj_val'].rolling(window=window, min_periods=1).mean()
            stat_df['adj_std'] = stat_df['adj_val'].rolling(window=window, min_periods=1).std()
            stat_df['adj_mean'].fillna(0)
            stat_df['adj_std'].fillna(0)

            return stat_df['adj_mean'], stat_df['adj_std']
        except Exception as e:
            print("Error Cap_Stats", e)
            return 0, 0


class Flag:
    @staticmethod
    def flagtype(x):
        limit = 3
        call_put = x['call_put']

        if x['total_z'] > limit:
            return call_put, x['total_z']
        elif x['fm_z'] > limit:
            return 'FM' + call_put, x['fm_z']
        elif x['wk_z'] > limit:
            return 'W' + call_put, x['wk_z']
        else:
            return '0', '0'

    @staticmethod
    def option_category(df, data_date):

        if 14 < df.day < 22:
            # if abs((df - data_date.date()).days) <= 70:
            if abs((df - data_date).days) <= 70:
                return "FM"
            else:
                return "BM"
        else:
            return "WK"

    def export(self, filename):
        conn = create_engine(wzyy_database)
        try:
            request = "SELECT * FROM option_flag"
            df_daily = pd.read_sql(request, conn)
            print('test')
            stat_array = ['call_flag_dates','put_flag_dates','underlying_price_array','underlying_price_high_array','underlying_price_low_array', 'bid_price_array', 'ask_price_array', 'mid_price_array']
            for stat in stat_array:
                df_daily[stat] = [list(self.convert(sublist)) for sublist in df_daily[stat]]
                df_daily[stat] = df_daily[stat].apply(lambda x: ",".join(x))

            print(df_daily[stat_array].dtypes)
            df_daily.to_csv(filename)

            print("Flags Exported To ",filename)
        except Exception as e:
            print("EXPORT ERROR {0}".format(e))
        finally:
            conn.dispose()

    def convert(self, sequence):
        if sequence is None:
            return
        for item in sequence:
            try:
                # yield float(item)
                yield str(item)

            except ValueError as e:
                yield item



    def unusual_screenTest(self, tickers=[], date=None, start =None, days=1):
        #
        # 1) DAILY OPTION STATS ARE READ IN AND ROLLING VOLUME AVERAGES ARE CALCULATED
        # 2) DAILY OPTION DATA ARE READ VOLUME AVERAGES ARE MERGED IN
        # 3) DAILY Z SCORES ARE CALCULATED FOR EACH OPTION DAILY DATA POINT AND FILTERED IF HIGHER THAN LIMIT
        # 4) Z SCORES ARE CALCULATED BASED ON DAILY AVERAGES OF TOTAL CALL, TOTAL PUT, TOTAL FM CALL, TOTAL FM PUT, DAILY LARGEST VOL, DAILY LARGEST 5 DAY OI CHANGE


        # Set date as latest date as default
        if date is None:
            date = dt.datetime.now().date()
            print("Default Date: ", date)

        # Function defaults to screening ticker for today - 1 Day
        flag_start = time.time()
        total_flags = 0
        if start is not None:
            min_date = start
        elif days == 0:
            min_date = dt.date(1900, 1, 1)
        else:
            min_date = date - BDay(days)

        printid = "{0} TO {1} | {2} D".format(min_date, date, days)
        if len(tickers) > 0:
            printid += " | Filter {0} Tickers ".format(len(tickers))

        # LOAD ALL EXPIRIES
        try:
            connection = create_engine('postgresql://postgres:inkstain@localhost:5432/wzyy_options')

            # EXCLUDE OPTION EXPIRIES THAT EXPIRE BEFORE MIN_DATE, AND ONES WHERE THE EARLIEST DATA DATE IS MORE THAN MAX_DATE
            request = "select * from option_expiry_table where '{0}' <= option_expiration AND '{1}' >= added_data_date order by option_expiration asc".format(min_date, date)
            expiry = pd.read_sql_query(request, con=connection)
            print("SCREEN FLAG | {0} {1} |  Loaded Expiries: {2} | {3} TO {4}".format(date.strftime('%Y-%m-%d'), days, len(expiry), expiry['option_expiration'].min(), expiry['option_expiration'].max()))
            logger.info("SCREEN FLAG | {0} {1} |  Loaded Expiries: {2} | {3} TO {4}".format(date.strftime('%Y-%m-%d'), days, len(expiry), expiry['option_expiration'].min(), expiry['option_expiration'].max()))


        except Exception as e:
            print("SCREEN FLAG | {0} {1} | Load Expiries ERROR | {2}".format(date.strftime('%Y-%m-%d'), days, e))
            logger.info("SCREEN FLAG | {0} {1} | Load Expiries ERROR | {2}".format(date.strftime('%Y-%m-%d'), days, e))


        finally:
            connection.dispose()

        if len(expiry) == 0:
            print("SCREEN FLAG | ERROR | {0} {1} | NO EXPIRIES".format(date.strftime('%Y-%m-%d'), days, ))
            logger.info("SCREEN FLAG | ERROR | {0} {1} | NO EXPIRIES".format(date.strftime('%Y-%m-%d'), days, ))

            return

        # SCREEN FLAGS SPLIT BY EXPIRY TABLE
        try:
            i = 0
            table_count = len(expiry['table_name'])
            for index,expiry_table in expiry.iterrows():

                # READ OPTION DATA FROM EXPIRY TABLE
                i += 1
                expiry_start = time.time()
                tableName = expiry_table['table_name']
                exp_date = expiry_table['option_expiration']
                start_date = expiry_table['added_data_date']

                connection_data = create_engine('postgresql://postgres:inkstain@localhost:5432/option_data')
                try:
                    request = "select distinct(symbol) symbol from {0} ".format(tableName)
                    expiry_tickers = pd.read_sql_query(request, con=connection_data)
                    print("{0} | {1} | Read Option Symbols | {2} Tickers".format(printid,tableName,len(expiry_tickers)))
                    logger.info("{0} | {1} | Read Option Symbols | {2} Tickers".format(printid,tableName,len(expiry_tickers)))


                except Exception as e:
                    print("{0} | {1} | Read Option Symbols ERROR | {2}".format(printid,tableName,e))
                    logger.info("{0} | {1} | Read Option Symbols ERROR | {2}".format(printid,tableName,e))


                finally:
                    connection_data.dispose()

                if len(tickers) > 0:
                    ref_tickers = tickers
                else:
                    ref_tickers = expiry_tickers['symbol']

                try:
                    num_processes = multiprocessing.cpu_count() * 2 - 4
                    chunks = [chunk for chunk in ref_tickers]
                    pool = multiprocessing.Pool(processes=num_processes)

                    func = partial(self.processFlagByTicker, tableName, min_date, date)
                    # result = pool.map(func, chunks, chunksize=1)

                    temp_sum = sum(pool.map(func, chunks, chunksize=1))
                    if temp_sum != None:
                        total_flags += temp_sum
                    print("Cum Flags: ",total_flags)
                    logger.info("Cum Flags: {0}".format(total_flags))

                    pool.close()
                    pool.join()

                except Exception as e:
                    print("{0} | Process Data ERROR | {1} | {2} ".format(printid, expiry_table['table_name'], e))
                    logger.info("{0} | Process Data ERROR | {1} | {2} ".format(printid, expiry_table['table_name'], e))

                    return

                expiry_end = time.time()
                print("{0} | Processed {1} | Cum Flag: {2} | {3}/{4} | {5} ".format(printid, tableName, total_flags, i, table_count, expiry_end-expiry_start))
                logger.info("{0} | Processed {1} | Cum Flag: {2} | {3}/{4} | {5} ".format(printid, tableName, total_flags, i, table_count, expiry_end-expiry_start))


        except Exception as e:
            print("Flag Screen | ERROR {0} | {1} ".format(printid, e))
            logger.info("Flag Screen | ERROR {0} | {1} ".format(printid, e))


        finally:
            flag_end = time.time()
            print("Flag Screen | COMPLETE | {0} | {1} Flags | {2}".format(printid, total_flags, flag_end-flag_start))
            logger.info("Flag Screen | COMPLETE | {0} | {1} Flags | {2}".format(printid, total_flags, flag_end-flag_start))



    def processFlagByTicker(self, tableName, min_date, date, ticker):

        conn = create_engine(wzyy_database)
        ticker_start = time.time()

        try:
            conn_data = create_engine(option_database)
            read_start = time.time()
            request = "select * from {0} where symbol = '{1}' and date >= '{2}' and date <= '{3}'".format(tableName, ticker, min_date, date)
            df_daily = pd.read_sql_query(request, con=conn_data)
            read_end = time.time()
        except Exception as e:
            print("{0} | processFlagByTicker | Read Option Data ERROR | {1} | {2} ".format(tableName, ticker, e))
            logger.info("{0} | processFlagByTicker | Read Option Data ERROR | {1} | {2} ".format(tableName, ticker, e))

        finally:
            conn_data.dispose()

        date_count = df_daily['date'].nunique()
        # print("{0} | processFlagByTicker | Read Option Data | {1} | {2} | {3}".format(tableName, ticker, date_count,"{0:8.4f}".format(read_end-read_start)))

        if len(df_daily) ==0:
            print("processFlagByTicker | ERROR | No Data ")
            logger.info("processFlagByTicker | ERROR | No Data ")


            return 0

        if df_daily['symbol'].nunique() >1:
            print("processFlagByTicker | ERROR | More than 1 Ticker ",df_daily['symbol'].drop_duplicates())
            logger.info("processFlagByTicker | ERROR | More than 1 Ticker {0}".format(['symbol'].drop_duplicates()))

            return 0

        # Using 20 days avg to calculate historical option stats. Queries for days processed + avg length. Cap avg at 3 std
        avg_window = 20
        z_limit = 2.5
        total_flags = 0

        max_date = df_daily['date'].max()
        min_date = df_daily['date'].min()
        exp_date = df_daily['option_expiration'].max()
        total_days = np.busday_count(min_date, max_date)
        ticker_inp = df_daily['symbol'].iloc[0]
        printid = "Flag Screen | {0} | {1} D | {2} ".format(exp_date,  total_days, ticker_inp.ljust(6))
        # print("{0} | Start Screen".format(printid))

        #   READ IN OPTION STAT, TICKER FOR X DAYS + AVERAGE WINDOW
        try:
            request = "SELECT * FROM option_stat WHERE symbol = '{0}' AND date <= '{1}' ORDER BY date ASC LIMIT {2} ;".format(ticker_inp, max_date, total_days + avg_window)

            df_stat = pd.read_sql_query(request, con=conn)
            df_stat.set_index(pd.DatetimeIndex(df_stat['date']), inplace=True)

            # FIND START AND END DATES FOR FLAGS
            flag_dates = df_stat['date'].sort_values(ascending=False)
            start_date = flag_dates.min()
            end_date = flag_dates.max()

            # print(df_stat.info())
            # print("{0} | {1} Stat Record | {2} to {3}".format(printid, len(flag_dates), start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d')))

            # Add All FM Vol together (Weekly + Monthly)
            df_stat['total_fm_call_vol'] = (df_stat['fm_call_vol'] + df_stat['wk_call_vol'])
            df_stat['total_fm_put_vol'] = (df_stat['fm_put_vol'] + df_stat['wk_put_vol'])

            # CALCULATING 20 DAY MEAN AND STD #

            # TOTAL CALL PUT
            df_stat['call_vol_mean_adj'], df_stat['call_vol_std_adj'] = Stat.cap_stats(df_stat['total_call_vol'], window=avg_window)
            df_stat['put_vol_mean_adj'], df_stat['put_vol_std_adj'] = Stat.cap_stats(df_stat['total_put_vol'], window=avg_window)

            # FM CALL PUT
            df_stat['fm_call_vol_mean_adj'], df_stat['fm_call_vol_std_adj'] = Stat.cap_stats(df_stat['total_fm_call_vol'], window=avg_window)
            df_stat['fm_put_vol_mean_adj'], df_stat['fm_put_vol_std_adj'] = Stat.cap_stats(df_stat['total_fm_put_vol'], window=avg_window)

            # df_stat['fm_vol_mean_adj'], df_stat['fm_vol_std_adj'] = Stat.cap_stats(df_stat['total_fm_vol'], window=avg_window)
            # DAILY LARGEST VOL
            df_stat['largest_vol_mean_adj'], df_stat['largest_vol_std_adj'] = Stat.cap_stats(df_stat['max_opt_1_vol'], window=avg_window)

            # DAILY LARGEST 5 DAY OI CHANGE
            df_stat['largest_5dayoi_mean_adj'], df_stat['largest_5dayoi_vol_adj'] = Stat.cap_stats(df_stat['max_5day_oi_1_change'], window=avg_window)

            # print("{0} | {1} Days | {2} Averages".format(printid, len(df_stat), df_stat['call_vol_mean_adj'].astype(bool).sum(axis=0)))
            # df_stat.to_csv("stat_check.csv")

        except Exception as e:
            print("Error Retrieving Option Stats | {0} | {1} TO {2} | {3}".format(ticker_inp, min_date, max_date, e))
            logger.info("Error Retrieving Option Stats | {0} | {1} TO {2} | {3}".format(ticker_inp, min_date, max_date, e))

            return 0

        # MERGE IN OPTION AVERAGES/STD TO DAILY OPTION DATA FOR COMPARISON

        mergeCols = ['call_vol_mean_adj', 'call_vol_std_adj', 'put_vol_mean_adj', 'put_vol_std_adj', 'fm_call_vol_mean_adj', 'fm_call_vol_std_adj', 'fm_put_vol_mean_adj', 'fm_put_vol_std_adj', 'largest_vol_mean_adj',
                     'largest_vol_std_adj', 'largest_5dayoi_mean_adj', 'largest_5dayoi_vol_adj']
        df_stat = df_stat[mergeCols].reset_index()

        # Convert DateTime Index to Date
        df_stat['date'] = df_stat['date'].apply(lambda x: x.date())

        # Merge Avg Option Volumes to Daily Option Data
        df_daily = pd.merge(df_daily, df_stat, how='left', on=['date'])

        # DAYS TO EXPIRY
        df_daily['expiry_days'] = df_daily[['date','option_expiration']].apply(lambda x: np.busday_count(x['date'], x['option_expiration']),axis=1)



        # CALCULATE Z SCORES FOR TOTAL VOL, FM VOL, LARGEST OPTION VOL, LARGEST 5 DAY OI INCREASE. EACH OPTION CAN HAVE MULTIPLE FLAGS

        df_daily['category'] = df_daily[['option_expiration','date']].apply(lambda x: self.option_category(x['option_expiration'], x['date']),axis=1)

        df_daily['total_call_z'] = np.where(df_daily['call_put'] == 'C', (df_daily['volume'] - df_daily['call_vol_mean_adj']) / df_daily['call_vol_std_adj'], 0)
        df_daily['total_put_z'] = np.where(df_daily['call_put'] == 'P', (df_daily['volume'] - df_daily['put_vol_mean_adj']) / df_daily['put_vol_std_adj'], 0)
        df_daily['total_z'] = np.where(df_daily['call_put'] == 'C', df_daily['total_call_z'], df_daily['total_put_z'])

        df_daily['fm_call_z'] = np.where((df_daily['category'].isin(['WK', 'FM'])) & (df_daily['call_put'] == 'C'), (df_daily['volume'] - df_daily['fm_call_vol_mean_adj']) / df_daily['fm_call_vol_std_adj'], 0)
        df_daily['fm_put_z'] = np.where((df_daily['category'].isin(['WK', 'FM'])) & (df_daily['call_put'] == 'P'), (df_daily['volume'] - df_daily['fm_put_vol_mean_adj']) / df_daily['fm_put_vol_std_adj'], 0)
        df_daily['fm_z'] = np.where(df_daily['call_put'] == 'C', df_daily['fm_call_z'], df_daily['fm_put_z'])

        # df_daily['fm_z'] = np.where(df_daily['category'].isin(['WK', 'FM']), (df_daily['volume'] - df_daily['fm_vol_mean_adj'])/df_daily['fm_vol_std_adj'], 0)
        df_daily['largest_z'] = (df_daily['volume'] - df_daily['largest_vol_mean_adj']) / df_daily['largest_vol_std_adj']

        df_daily['largest_5dayoi_z'] = (df_daily['open_interest_change'] - df_daily['largest_5dayoi_mean_adj']) / df_daily['largest_5dayoi_vol_adj']


        df_daily['total_z_flag'] = np.where(df_daily['total_z'] > z_limit, 1, 0)
        df_daily['fm_z_flag'] = np.where(df_daily['fm_z'] > z_limit, 1, 0)
        df_daily['largest_z_flag'] = np.where(df_daily['largest_z'] > z_limit, 1, 0)
        df_daily['largest_5dayoi_z_flag'] = np.where(df_daily['largest_5dayoi_z'] > z_limit, 1, 0)

        df_daily['flag'] = df_daily['total_z_flag'] + df_daily['fm_z_flag'] + df_daily['largest_z_flag'] + df_daily['largest_5dayoi_z_flag']
        # print(df_daily['OI_Change_pct'].sum())

        # EXCLUDE FLAGS WHERE OI INCREASE < 50% OF VOL
        df_daily['open_interest_change_vol_pct'] = df_daily.apply(lambda x: (x['open_interest_change'] / x['volume']) if x['volume'] > 0 else 0, axis=1)
        df_daily['open_interest_change_vol_pct'].fillna(0)
        df_daily['flag'] = np.where((df_daily['open_interest_change_vol_pct'] < 0.5), 0, df_daily['flag'])

        df_daily['open_interest_5day_change_pct'] = df_daily.apply(lambda x: (x['open_interest'] / x['open_interest_5day']) - 1 if x['open_interest_5day'] > 0 else 10, axis=1)
        df_daily['open_interest_5day_change_pct'].fillna(0)

        # df_daily.to_csv(ticker_inp + "_Daily.csv")

        temp_flags = df_daily[df_daily['flag'] != 0].copy()
        # Dropping the ind call/put z scores as they are already reflected in the total/fm z scores. The other zscore would just be 0s
        # print("{0} | {1} - {2} | {3} Flags".format(printid, start_date, end_date, len(temp_flags)))

        if len(temp_flags) > 0:
            temp_flags.drop(columns=['total_call_z', 'total_put_z', 'fm_call_z', 'fm_put_z'], inplace=True)

    # ANALYZE FLAGS

        try:
            if len(temp_flags) > 0:
                temp_flags = self.analyze(temp_flags)
                temp_flags.drop(columns=['shares_float','shares_outstanding'],inplace=True)
                if 'short_int' in temp_flags.columns & temp_flags['short_int'] is not None:
                    temp_flags['short_int'] = temp_flags['short_int'].apply(lambda x: np.asscalar(x))
        except Exception as e:
            print("Error Analyzing Flag | {0} | {1} TO {2} | {3}".format(ticker_inp, min_date, max_date, e))
            logger.info("Error Analyzing Flag | {0} | {1} TO {2} | {3}".format(ticker_inp, min_date, max_date, e))

            return 0


    # UPLOAD ANALYZED FLAGS TO DATABASE
        try:
            if len(temp_flags) > 0:
                # print(temp_flags.dtypes)
                # CAP Z SCORE FOR UPLOAD/ MAX 10^6
                temp_flags['fm_z'] = np.minimum(10000, temp_flags['fm_z'])
                temp_flags['total_z'] = np.minimum(10000, temp_flags['total_z'])
                temp_flags['largest_z'] = np.minimum(10000, temp_flags['largest_z'])
                temp_flags['largest_5dayoi_z'] = np.minimum(10000, temp_flags['largest_5dayoi_z'])

                temp_flags.to_sql("option_flag", conn, if_exists='append', index=False)
                ticker_end = time.time()
                print('{0} | Flags: {1} | {2}'.format(printid, len(temp_flags), "{0:.4f}".format(ticker_end - ticker_start)))
                logger.info('{0} | Flags: {1} | {2}'.format(printid, len(temp_flags), "{0:.4f}".format(ticker_end - ticker_start)))

            else:
                ticker_end = time.time()
                # print('{0} | {1}'.format(printid, "{0:.4f}".format(ticker_end - ticker_start)))

            total_flags += len(temp_flags)
        except Exception as e:
            print('{0} | Uploaded Flags ERROR | {1}'.format(printid, e))
            logger.info('{0} | Uploaded Flags ERROR | {1}'.format(printid, e))

            return 0

        finally:
            conn.dispose()
            return total_flags



    def analyzeFlags(self, tickers=[], date=None, days=0):
        analyze_start = time.time()

        if date is None:
            date = dt.datetime.now().date()
            print("Default Date: ", date)

        if days == 0:
            min_date = dt.date(1900, 1, 1)
        else:
            min_date = date - BDay(days)

        printid = "{0} TO {1} | {2} Days | Analyze Flags |".format(min_date, date, days)

        if len(tickers) > 0:
            printid += " Filter {0} Tickers |".format(len(tickers))

        # Set date as latest date as default


        # LOAD ALL EXPIRIES
        try:
            connection = create_engine('postgresql://postgres:inkstain@localhost:5432/wzyy_options')

            if len(tickers) > 0:
                tickers = ["'" + s + "'" for s in tickers]
                request = "select * from option_flag where symbol in ({0}) and date >= '{1}' and date <= '{2}' order by symbol, date asc".format(",".join(tickers), min_date, date)
            else:
                request = "select * from option_flag where date >= '{0}' and date <= '{1}' order by symbol, date asc".format(min_date, date)

            df_flags = pd.read_sql_query(request, con=connection)
            print("{0} | Flags Read: {1}".format(printid, len(df_flags)))
            connection.dispose()

        except Exception as e:
            print("{0} | Flags Read | ERROR: {1}".format(printid, e))
            connection.dispose()

        try:
            self.analyze(df_flags)

        except Exception as e:
            print("{0} | Analyze Flags | ERROR: {1}".format(printid, e))

        finally:
            analyze_end = time.time()
            print("{0} | Flags Analyzed: {1} | FINISH | {2}".format(printid, len(df_flags), analyze_end-analyze_start))


######      ANALYSIS       #####################################################################################################


    # def analyze(self, tickers = []):
    #     conn = create_engine('postgresql://postgres:inkstain@localhost:5432/wzyy_options')
    #     count = 0
    #
    # # READ IN ALL FLAGS
    #     try:
    #         request = "SELECT * FROM option_flag where symbol = 'ACXM' ORDER BY symbol, date asc;"
    #         df_flags = pd.read_sql_query(request, con=conn)
    #
    #         if len(tickers) > 0:
    #             df_flags = df_flags[df_flags["symbol"].isin(tickers)]
    #         print("Analyze Flags: Read Flags | {0} Tickers | {1} Flags | {2} CALLS {3} PUTS".format(df_flags['symbol'].nunique(),len(df_flags), df_flags['call_ind'].sum(), df_flags['put_ind'].sum() ))
    #
    #     except Exception as e:
    #         print("Analyze Flags: Read Flags Error ", e)

    def analyze(self, df):

        df_flags = df.copy()
        if len(df_flags) == 0:
            print("Analyze | No Data")
            return

        conn_data = create_engine('postgresql://postgres:inkstain@localhost:5432/option_data')
        conn_wz = create_engine('postgresql://postgres:inkstain@localhost:5432/wzyy_options')

        count = 0

        df_flags['call_ind'] = df_flags.apply(lambda x: 1 if x['call_put'] == 'C' else 0, axis=1)
        df_flags['put_ind'] = df_flags.apply(lambda x: 1 if x['call_put'] == 'P' else 0, axis=1)
        df_flags['date'] = df_flags['date'].apply(lambda x: dt.datetime.strftime(x, '%Y-%m-%d'))
        # df_flags.set_index(['option_symbol','date'], inplace=True)


    # CREATE DUMMY FLAG STAT TABLE WITH OPTION SYMBOL AND FLAG DATE AS INDEX
        tuples = list(zip(df_flags['option_symbol'],df_flags['date']))
        index = pd.MultiIndex.from_tuples(tuples, names=['first', 'second'])

        stat_col= ['open_high', 'open_high_date', 'open_low', 'open_low_date', 'close_high', 'close_high_date', 'close_low', 'close_low_date']
        stat_col+= ['max_high', 'max_high_date', 'min_low', 'min_low_date', 'opt_last_high', 'opt_last_high_date', 'opt_mid_high', 'opt_mid_high_date', 'expiry_stock_price', 'opt_expiry_value']

    # OPTION RETURN DATA - KEY MOVING AVERAGES AND MAX MIN
        stat_col+= ['opt_mid_drawdown_bmax', 'opt_mid_drawdown_bmax_date', 'max_drawdown_bmax', 'max_drawdown_bmax_date', 'hit_itm_mid', 'hit_itm_date', 'close_itm_mid', 'close_itm_date']
        stat_col+= ['close_sma200_mid','close_sma200_date','close_sma100_mid','close_sma100_date','close_sma50_mid','close_sma50_date','close_ema8_mid','close_ema8_date']
        stat_col+= ['high_52w_mid','high_52w_date','low_52w_mid','low_52w_date','high_100d_mid','high_100d_date','low_100d_mid','low_100d_date','high_30d_mid','high_30d_date','low_30d_mid','low_30d_date']

    # OPTION RETURN DATA - PRICE CHANGES
        stat_col+= ['opt_n50ret_mid','opt_n50ret_date','opt_p100ret_mid','opt_p100ret_date','opt_p200ret_mid','opt_p200ret_date']
        stat_col+= ['stock_p5ret_mid','stock_p5ret_date','stock_p10ret_mid','stock_p10ret_date','stock_n5ret_mid','stock_n5ret_date','stock_n10ret_mid','stock_n10ret_date']

    # OPTION RETURN DATA - OI CHANGE
        stat_col += ['oi_n50pct_mid','oi_n50pct_mid_date','oi_n90pct_mid','oi_n90pct_mid_date']

    # OPTION RETURN DATA - ADD FLAGS
        stat_col+= ['addflag_call_mid','addflag_call_mid_date','addflag_put_mid','addflag_put_mid_date']

    # FLAG IND DATA
        stat_col+= ['call_flag', 'call_flag_dates', 'put_flag', 'put_flag_dates']
        stat_col += ['underlying_price_array','underlying_price_high_array','underlying_price_low_array','bid_price_array','ask_price_array','mid_price_array']

    # SHORT INTEREST DATA
        short_fields = ['short_date','short_int', 'short_float', 'shares_float', 'shares_outstanding', 'short_day_cover', 'insider_own_pct', 'institution_own_pct', 'sector', 'industry']
        stat_col += short_fields

        flag_underlying_stats = ['close', 'high', 'low', 'open', 'sma_200', 'sma_100', 'sma_50', 'ema_8', 'high_52w', 'low_52w', 'high_100d', 'low_100d', 'high_30d', 'low_30d']
        stat_col += flag_underlying_stats


        flag_stats = DataFrame(columns=stat_col, index=index)
        # print(flag_stats.head())
        # print(type(flag_stats.index))

        # CHECK IF TICKER IS ETF
        try:
            request = "SELECT * FROM etf_data"
            df_etf = pd.read_sql_query(request, con=conn_wz)
            df_flags['etf'] = df_flags['symbol'].apply(lambda x: 1 if x in df_etf['etf_symbol'].values else 0)
        except Exception as e:
            print("Flag Analysis | ERROR | ETF Read Error| ",e)
            logger.info("Flag Analysis | ERROR | ETF Read Error| {0}".format(e))
            raise


    # SPLIT FLAGS BY SYMBOL TO READ UNDERLYING DATA ONLY ONCE
        temp_df_flags = df_flags.groupby("symbol", as_index=False)

        for symbol, flags in temp_df_flags:

        # TICKER LEVEL - GROUP FLAGS BY TICKER, AND MERGE TICKER LEVEL DATA BEFORE ANALYZING INDIVIDUAL FLAG RETURN STATISTICS
        # UNDERLYING PRICE, TOTAL CALL/PUT FLAGS BY DATE, SHORT INTEREST

            symbol_min_date = flags['date'].min()
            symbol_max_date = flags['option_expiration'].max()
            # print("Flag Analysis Start| {0} | {1} to {2} | {3} CALLS {4} PUTS".format(symbol, symbol_min_date, symbol_max_date, flags['call_ind'].sum(),flags['put_ind'].sum()))

            # flag_merge.set_index(pd.DatetimeIndex(flag_merge['date']), inplace=True)
            # flag_merge.drop('date', axis=1, inplace=True)

        # READ IN UNDERLYING DATA FROM FLAG DATE TO OPTION EXPIRY
            try:
                request = "SELECT * FROM underlying_data where symbol = '{0}' and date >= '{1}' and date <= '{2}' order by date asc".format(symbol, symbol_min_date, symbol_max_date)
                df_price = pd.read_sql_query(request, con=conn_wz)
                df_price.drop(columns='volume', inplace=True)
                if len(df_price) ==0:
                    print("Read Price Error: No Price Data For ", symbol)
                    print("Attempting To Read Underlying Price for", symbol)
                    dm = DataManager()
                    test = dm.fetchUnderlyingMS(symbol, date_length='full')
                    print("{0} | Fetched Prices".format(symbol))
                    df_price = pd.read_sql_query(request, con=conn_wz)
                    df_price.drop(columns='volume', inplace=True)


            except Exception as e:
                print("Flag Analysis | ERROR | Underlying Price Data | {0} {1} - {2} | {3}".format(symbol, symbol_min_date, symbol_max_date,e))
                symbol.to_csv('Missing_Price_Data.csv', mode='a', header=False)
                return

        # CALCULATE TOTAL FLAG_IND FOR EACH UNDERLYING PRICE DAY
            try:
                pm = len(df_price)
                # flag_merge = flags[['date','call_ind','put_ind']].copy()
                # Handles days with multiple flags, calls and puts
                flag_merge = flags.groupby(['date'])[["call_ind", "put_ind"]].sum()
                flag_merge.reset_index(inplace=True)

                df_price['date'] = pd.to_datetime(df_price['date'], format="%Y-%m-%d")
                flag_merge['date'] = pd.to_datetime(flag_merge['date'], format="%Y-%m-%d")

                df_price = pd.merge(df_price, flag_merge, on='date', how='outer')
                df_price['call_ind'].fillna(0, inplace=True)
                df_price['put_ind'].fillna(0, inplace=True)

                df_price['call_ind'] = df_price['call_ind'].apply(lambda x: int(x))
                df_price['put_ind'] = df_price['put_ind'].apply(lambda x: int(x))


            except Exception as e:
                print("Flag Analysis | ERROR | Underlying Price & Flag Ind Merge | {0} | {1} - {2} | {3}".format(symbol, symbol_min_date, symbol_max_date,e))
                logger.info("Flag Analysis | ERROR | Underlying Price & Flag Ind Merge | {0} | {1} - {2} | {3}".format(symbol, symbol_min_date, symbol_max_date,e))

                return

            am = len(df_price)
            if (am != pm):
                print("Flag - Price Merge Error | {0} | {1} to {2}".format(symbol, pm, am))
                logger.info("Flag - Price Merge Error | {0} | {1} to {2}".format(symbol, pm, am))
                return


        # READ IN SHORT DATA
            try:
                short_fields_sql = ",".join(short_fields)
                request = "SELECT {0} FROM short_data where symbol = '{1}' order by short_date desc".format(short_fields_sql, symbol)
                df_short = pd.read_sql_query(request, con=conn_wz)
                df.rename(columns={"date":"short_date"}, inplace=True)

                if len(df_short) == 0:
                    print("Read Short Interest Error: No Short Interest Data For ", symbol)
                    logger.info("Read Short Interest Error: No Short Interest Data For {0}".format(symbol))

                    # print("Read OCHL Prices | {0} | {1} Recs".format(symbol, len(df_price)))

            # FORMAT SHORT DATA FOR BLANKS
                else:
                    df_short['shares_float'] = df_short['shares_float'].interpolate()
                    df_short['shares_float'].bfill(inplace=True)
                    df_short['shares_float'].fillna(0, inplace=True)

                    df_short['shares_outstanding'] = df_short['shares_outstanding'].interpolate()
                    df_short['shares_outstanding'].bfill(inplace=True)
                    df_short['shares_outstanding'].fillna(0, inplace=True)

                    df_short['short_float'] = df_short.apply(lambda x: self.flag_short_float_calc(x), axis=1)
                    # df_short['short_float'] = df_short['short_float'] * 100
                    df_short['short_float'].bfill(inplace=True)

                    df_short['insider_own_pct'].ffill(inplace=True)
                    df_short['insider_own_pct'].bfill(inplace=True)
                    df_short['institution_own_pct'].ffill(inplace=True)
                    df_short['institution_own_pct'].bfill(inplace=True)

                    df_short['short_int'].fillna(0, inplace=True)
                    # df_short['short_int'] = df_short['short_int'].apply(lambda x: int(x))
                    # df_short['short_int'] = df_short['short_int'].apply(lambda x: int(x))

                    df_short.drop(columns=['shares_float','shares_outstanding'])


            except Exception as e:
                print("Flag Analysis | ERROR | Short Interest | {0} | {1}".format(symbol,e))
                return

            for flag in flags.itertuples():

                # FLAG LEVEL
                flag_s = flag.option_symbol
                flag_date = flag.date
                flag_date_dt = dt.datetime.strptime(flag.date, '%Y-%m-%d').date()
                flag_strike = flag.strike
                # print(flag_date)

            # ADD SHORT INTEREST DATA
                df_short_input = df_short[df_short['short_date'] <= flag_date_dt]
                if len(df_short_input) > 0:
                    # print('test')
                    flag_stats.loc[(flag.option_symbol,flag_date),short_fields + ["short_date"]] = df_short_input.iloc[0]

            # READ FLAG OPTION DAILY DATA
                try:
                    tableName = "data_" + flag.option_expiration.strftime('%m_%d_%Y')
                    request = "SELECT * FROM {0} where option_symbol ='{1}' and date >= '{2}' order by date asc".format(tableName, flag.option_symbol, flag.date)
                    flag_price = pd.read_sql_query(request, con=conn_data)
                    # print("Flag Analysis | Option Data | {0} | {1} - {2} | {3} Prices".format(flag.option_symbol, flag_price['date'].min(), flag_price['date'].max(), len(flag_price)))

                # CALCULATED MID PRICE - LAST IF LAST IN (BID/ASK), ELSE IF BID/ASK SPREAD WIDER THAN BID, MID = 1.5 BID, ELSE MID(BID+ASK)
                    flag_price['mid'] = flag_price.apply(lambda x: x['last'] if x['bid'] <= x['last'] <= x['ask'] else (0 if x['bid'] == 0 else ((x['bid'] + x['ask'])/2 if ((x['ask'] - x['bid'])/x['bid']) < 1 else x['bid']*1.5)), axis=1)
                    flag_price['mid'] = flag_price.apply(lambda x: 0 if x['bid'] == 0 else x['mid'], axis=1)
                    flag_price['mid'] = flag_price['mid'].apply(lambda x: round(x, 2))


                except Exception as e:
                    print("Flag Analysis | ERROR | Flag Option Data Read | {0} - {1} | {2}".format(symbol, flag.date,e))
                    logger.info("Flag Analysis | ERROR | Flag Option Data Read | {0} - {1} | {2}".format(symbol, flag.date,e))

                    return

        # MERGE FLAG OPTION DAILY DATA WITH UNDERLYING OCHL DATA
                try:
                    pm = len(flag_price)
                    flag_price['date'] = pd.to_datetime(flag_price['date'], format="%Y-%m-%d")
                    flag_price = pd.merge(flag_price, df_price, on=['symbol','date'], how='left')

                    am = len(flag_price)
                    if (am != pm):
                        print("Ind Flag - Price Merge Error | {0} | {1} to {2}".format(flag.option_symbol, pm, am))
                        logger.info("Ind Flag - Price Merge Error | {0} | {1} to {2}".format(flag.option_symbol, pm, am))

                # FLAG OCHL UNDERLYING DATA
                    for stat in flag_underlying_stats:
                        flag_stats.loc[(flag_s, flag_date), stat] = flag_price[stat].iloc[0]


                except Exception as e:
                    print("Flag Analysis | ERROR | Flag Option Data & Underlying Merge | {0} | {1}".format(symbol,e))
                    logger.info("Flag Analysis | ERROR | Flag Option Data & Underlying Merge | {0} | {1}".format(symbol,e))

                    return
                # print("Option and OCHL Merged | {0} | {1}".format(flag.option_symbol, len(flag_price)))


                try:

                # FILL OUT UNDERLYING AND OPTION PRICE DATA
                #     print((flag_s, flag_date))
                #     flag_price.to_csv('TPX_flag_test.csv')
                #     print(flag_price['close'].tolist())


                    flag_stats.loc[(flag_s, flag_date), 'underlying_price_array'] = flag_price['close'].tolist()
                    flag_stats.loc[(flag_s, flag_date), 'underlying_price_high_array'] = flag_price['high'].tolist()
                    flag_stats.loc[(flag_s, flag_date), 'underlying_price_low_array'] = flag_price['low'].tolist()

                    flag_stats.loc[(flag_s, flag_date), 'bid_price_array'] = flag_price['bid'].tolist()
                    flag_stats.loc[(flag_s, flag_date), 'ask_price_array'] = flag_price['ask'].tolist()
                    flag_stats.loc[(flag_s, flag_date), 'mid_price_array'] = flag_price['mid'].tolist()

                # FILL OUT OPTION FLAG MIN MAX DETAILS
                    flag_stats.loc[(flag_s,flag_date),['open_high','open_high_date']] = self.flag_stat_minmax(flag_price,"max","open")
                    flag_stats.loc[(flag_s,flag_date),['open_low','open_low_date']] = self.flag_stat_minmax(flag_price,"min","open")
                    flag_stats.loc[(flag_s,flag_date),['close_high','close_high_date']] = self.flag_stat_minmax(flag_price,"max","close")
                    flag_stats.loc[(flag_s,flag_date),['close_low','close_low_date']] = self.flag_stat_minmax(flag_price,"min","close")
                    flag_stats.loc[(flag_s,flag_date),['max_high','max_high_date']] = self.flag_stat_minmax(flag_price,"max","high")
                    flag_stats.loc[(flag_s,flag_date),['min_low','min_low_date']] = self.flag_stat_minmax(flag_price,"min","low")

                    flag_stats.loc[(flag_s,flag_date),['opt_last_high','opt_last_high_date']] = self.flag_stat_minmax(flag_price,"max","last")
                    flag_stats.loc[(flag_s,flag_date),['opt_mid_high','opt_mid_high_date']] = self.flag_stat_minmax(flag_price,"max","mid")
                    flag_stats.loc[(flag_s,flag_date), ['expiry_stock_price','opt_expiry_value']] = self.flag_expiry_value(flag_price)



                    max_ret_date = flag_stats.loc[(flag.option_symbol,flag_date),'opt_mid_high_date']
                    flag_prehigh = flag_price[flag_price['date'] <= max_ret_date]
                    flag_stats.loc[(flag.option_symbol,flag_date),['opt_mid_drawdown_bmax','opt_mid_drawdown_bmax_date']] = self.flag_stat_minmax(flag_prehigh,"min","mid")

                    if flag.call_put == 'C':
                        flag_stats.loc[(flag_s, flag_date), ['max_drawdown_bmax', 'max_drawdown_bmax_date']] = self.flag_stat_minmax(flag_prehigh, "min", "low")
                    elif flag.call_put == 'P':
                        flag_stats.loc[(flag_s, flag_date), ['max_drawdown_bmax', 'max_drawdown_bmax_date']] = self.flag_stat_minmax(flag_prehigh, "max", "high")

                # OPTION MID PRICE - WHEN STOCK TOUCHES ITM
                    flag_stats.loc[(flag_s,flag_date),['hit_itm_mid','hit_itm_date']] = self.flag_stat_ITM(flag_price, flag.call_put,flag.strike,"touch","mid")

                # OPTION MID PRICE - WHEN STOCK CLOSES ITM
                    flag_stats.loc[(flag_s,flag_date),['close_itm_mid','close_itm_date']] = self.flag_stat_ITM(flag_price, flag.call_put,flag.strike,"close","mid")

                # OPTION MID PRICE - WHEN STOCK CLOSES ABOVE/BELOW KEY MA
                    flag_stats.loc[(flag_s,flag_date),['close_sma200_mid','close_sma200_date']] = self.flag_stat_touch(flag_price,"sma_200","close","mid")
                    flag_stats.loc[(flag_s,flag_date),['close_sma100_mid','close_sma100_date']] = self.flag_stat_touch(flag_price,"sma_100","close","mid")
                    flag_stats.loc[(flag_s,flag_date),['close_sma50_mid','close_sma50_date']] = self.flag_stat_touch(flag_price,"sma_50","close","mid")
                    flag_stats.loc[(flag_s,flag_date),['close_ema8_mid','close_ema8_date']] = self.flag_stat_touch(flag_price,"ema_8","close","mid")

                # OPTION MID PRICE - WHEN STOCK TOUCHES TIME RANGE HIGH/LOWS (HIGH OF DAY MAKES NEW RANGE HIGH, LOW OF DAY MAKE NEW RANGE LOW)
                    flag_stats.loc[(flag_s,flag_date),['high_52w_mid','high_52w_date']] = self.flag_stat_touch(flag_price,"high_52w","high","mid")
                    flag_stats.loc[(flag_s,flag_date),['low_52w_mid','low_52w_date']] = self.flag_stat_touch(flag_price,"low_52w","low","mid")
                    flag_stats.loc[(flag_s,flag_date),['high_100d_mid','high_100d_date']] = self.flag_stat_touch(flag_price,"high_100d","high","mid")
                    flag_stats.loc[(flag_s,flag_date),['low_100d_mid','low_100d_date']] = self.flag_stat_touch(flag_price,"low_100d","low","mid")
                    flag_stats.loc[(flag_s,flag_date),['high_30d_mid','high_30d_date']] = self.flag_stat_touch(flag_price,"high_30d","high","mid")
                    flag_stats.loc[(flag_s,flag_date),['low_30d_mid','low_30d_date']] = self.flag_stat_touch(flag_price,"low_30d","low","mid")

                # OPTION MID PRICE - WHEN OPTION PRICE CLOSES ABOVE/BELOW PERCENTAGE OF ASK PRICE FROM FLAG DAY (CLOSING OPT GAIN/LOSS TRIGGER)
                    flag_stats.loc[(flag_s,flag_date),['opt_n50ret_mid','opt_n50ret_date']] = self.flag_stat_return(flag_price,-0.5,"ask","mid")
                    flag_stats.loc[(flag_s,flag_date),['opt_p100ret_mid','opt_p100ret_date']] = self.flag_stat_return(flag_price,1,"ask","mid")
                    flag_stats.loc[(flag_s,flag_date),['opt_p200ret_mid','opt_p200ret_date']] = self.flag_stat_return(flag_price,2,"ask","mid")

                # OPTION MID PRICE - WHEN STOCK PRICE CLOSES ABOVE/BELOW PERCENTAGE OF CLOSING STOCK PRICE FROM FLAG DAY
                    flag_stats.loc[(flag_s,flag_date),['stock_p5ret_mid','stock_p5ret_date']] = self.flag_stat_return(flag_price,0.05,"close","mid")
                    flag_stats.loc[(flag_s,flag_date),['stock_p10ret_mid','stock_p10ret_date']] = self.flag_stat_return(flag_price,0.1,"close","mid")
                    flag_stats.loc[(flag_s,flag_date),['stock_n5ret_mid','stock_n5ret_date']] = self.flag_stat_return(flag_price,-0.05,"close","mid")
                    flag_stats.loc[(flag_s,flag_date),['stock_n10ret_mid','stock_n10ret_date']] = self.flag_stat_return(flag_price,-0.1,"close","mid")

                # OPTION MID PRICE - WHEN ANOTHER CALL/PUT FLAG IS FLAGGED AFTER INIT FLAG DAY
                    flag_stats.loc[(flag_s,flag_date),['addflag_call_mid','addflag_call_mid_date']] = self.flag_stat_add_flag(flag_price, 'C', 'mid')
                    flag_stats.loc[(flag_s,flag_date),['addflag_put_mid','addflag_put_mid_date']] = self.flag_stat_add_flag(flag_price, 'P', 'mid')

                # OPTION MID PRICE - WHEN OI CHANGES BY % of ORIGINAL FLAG VOL, MIN OF 250
                    flag_stats.loc[(flag_s,flag_date),['oi_n50pct_mid','oi_n50pct_mid_date']] = self.flag_stat_OI_change(flag_price, flag.open_interest_change, -0.5, 'mid')
                    flag_stats.loc[(flag_s,flag_date),['oi_n90pct_mid','oi_n90pct_mid_date']] = self.flag_stat_OI_change(flag_price, flag.open_interest_change, -0.9, 'mid')

                # OTHER FLAG DATES IN SEQUENCE

                    if flag_price['call_ind'].sum() > 0:
                        flag_stats.loc[(flag_s,flag_date),'call_flag'] = int(flag_price['call_ind'].sum())
                        call_dates = flag_price[flag_price['call_ind'] == 1]['date'].apply(lambda x: x.strftime('%Y-%m-%d')).tolist()
                        flag_stats.loc[(flag_s,flag_date),'call_flag_dates'] = call_dates
                    else:
                        flag_stats.loc[(flag_s, flag_date), 'call_flag'] = 0

                # print(flag_stats.loc[(flag_s, flag_date), 'call_flag_dates'])

                    if flag_price['put_ind'].sum() > 0:
                        flag_stats.loc[(flag_s,flag_date),'put_flag'] = int(flag_price['put_ind'].sum())
                        put_dates = flag_price[flag_price['put_ind'] == 1]['date'].apply(lambda x: x.strftime('%Y-%m-%d')).tolist()
                        flag_stats.loc[(flag_s,flag_date),'put_flag_dates'] = put_dates
                    else:
                        flag_stats.loc[(flag_s, flag_date), 'put_flag'] = 0

                    # MA DISTANCES
                    count += 1
                    # print("{0} | {5} Days | {3} Call_Ind | {4} Put Ind | {1}/{2} Complete |".format(flag.option_symbol, count, len(df_flags), flag_price['call_ind'].sum(), flag_price['put_ind'].sum(), len(df_price)))
                    print("Flag Analyze | {0} | {1}/{2} Complete |".format(flag.option_symbol, count, len(df_flags)))
                    logger.info("Flag Analyze | {0} | {1}/{2} Complete |".format(flag.option_symbol, count, len(df_flags)))



                except Exception as e:
                    print("Flag Analysis | ERROR | Flag Option Return Calcs | {0} | {1}".format(symbol,e))
                    logger.info("Flag Analysis | ERROR | Flag Option Return Calcs | {0} | {1}".format(symbol,e))

                    return


        flag_stats.index.names = ['option_symbol','date']
        # flag_stats.reset_index(inplace=True)
        pm = len(df_flags)
        # df_flags.drop(columns = stat_col, inplace=True)
        # df_flags.to_csv('df_flags_merge.csv')
        # flag_stats.to_csv('flag_stats_merge.csv')

        df_flags = pd.merge(df_flags, flag_stats, on=['option_symbol','date'], how='left')
        am = len(df_flags)
        if (am != pm):
            print("Final Flag - Stats Merge Error | {0} | {1} to {2}".format(flag.option_symbol, pm, am))
            logger.info("Final Flag - Stats Merge Error | {0} | {1} to {2}".format(flag.option_symbol, pm, am))

        # df_flags.to_csv('flag_merge.csv')
        # print("Finish Analyzing Flags | {0} Tickers | {1} Flags".format(df_flags['symbol'].nunique(), len(df_flags)))
        conn_data.dispose()
        # print("DF: ", df_flags.info(verbose=True))

        # CONVERT DTYPES
        for col in stat_col:
            if 'date' not in col:
                df_flags[col] = pd.to_numeric(df_flags[col], errors='ignore')

        return df_flags

    def flag_short_float_calc(self,df):

        if df['shares_float'] != 0:
            float = df['shares_float']
        elif df['shares_outstanding'] != 0:
            float = df['shares_outstanding']
        else:
            float = 0


        if df['short_float'] != 0:
            return df['short_float']
        elif float != 0:
            return (df['short_int'] / df['shares_float'])*100
        else:
            return None


# RETURN CALCULATIONS


    def flag_expiry_value(self,df_price):
        call_put = df_price['call_put'].iloc[0]
        strike = df_price['strike'].iloc[0]
        last_price = df_price['stock_price'].iloc[-1]

        if call_put == "C":
            exp_value = max(0,last_price-strike)
        elif call_put == "P":
            exp_value = max(0,strike-last_price)
        return [last_price,exp_value]


    def flag_stat_minmax(self,df_price, function, field):
        try:
            if function == 'max':
                index, value = max(enumerate(df_price[field]), key=operator.itemgetter(1))
            elif function == 'min':
                index, value = min(enumerate(df_price[field]), key=operator.itemgetter(1))

            # print(index,value)
            # print(function, field, "    ", value, df_price['date'].loc[index])
            return [value, df_price['date'].loc[index]]
        except Exception as e:
            print("flag_stat_minmax ERROR | {0} | {1} | {2}".format(function, field, e))
            return [None, None]

    def flag_stat_touch(self, df_price, level, ref_field, output_field):
        try:

        # DETERMINE IF CURRENT PRICE IS ABOVE OR BELOW LEVEL

            # CURRENT CLOSE > LEVEL, DETECT WHEN IT DROPS TO LEVEL
            df_price_filter = DataFrame()
            if df_price['close'].iloc[0] >= df_price[level].iloc[0]:
                df_price_filter = df_price[df_price[ref_field] <= df_price[level]]
            elif df_price['close'].iloc[0] < df_price[level].iloc[0]:
                df_price_filter = df_price[df_price[ref_field] >= df_price[level]]

            if len(df_price_filter) > 0:
                price = pd.to_numeric(df_price_filter[output_field].iloc[0])
                return [price, df_price_filter['date'].iloc[0]]
            else:
                return [None, None]

        except Exception as e:
            print("flag_stat_touch ERROR | {0} {1} | {2} | {3}".format(df_price['option_symbol'].iloc[0], df_price['date'].iloc[0], level + ref_field + output_field, e))
            return [None, None]

    # self.flag_stat_ITM(flag_price, flag.call_put, flag.strike, "touch", "mid")

    def flag_stat_ITM(self, df_price, call_put, strike, ref_field, output_field):

        try:
            if ref_field == "touch":
                if call_put == "C":
                    ref_field = "high"
                elif call_put == "P":
                    ref_field = "low"
                else:
                    print("ERROR. Call_Put Not identified. Has to be 'C' or 'P' :", call_put)
                    return [None, None]


            if call_put == "C":
                df_price_filter = df_price[df_price[ref_field] >= float(strike)]
            elif call_put == "P":
                df_price_filter = df_price[df_price[ref_field] <= float(strike)]
            else:
                print("ERROR. Call_Put Not identified. Has to be 'C' or 'P' :",call_put)
                return [None, None]

            if len(df_price_filter) > 0:
                price = pd.to_numeric(df_price_filter[output_field].iloc[0])
                return [price, df_price_filter['date'].iloc[0]]
            else:
                return [None, None]
        except Exception as e:
            print("flag_stat_ITM ERROR| {0} {1} | {2} | {3}".format(df_price['option_symbol'].iloc[0], df_price['date'].iloc[0], call_put + strike + ref_field + output_field, e))
            return [None, None]

    def flag_stat_return(self, df_price, min_ret, ref_field, output_field):

        try:
            if ref_field == "ask":
                if min_ret > 0:
                    df_price_filter = df_price[df_price["mid"] >= (df_price['ask'].iloc[0]*(1+min_ret))]
                elif min_ret < 0:
                    df_price_filter = df_price[df_price["mid"] <= (df_price['ask'].iloc[0]*(1+min_ret))]

            elif ref_field == "close":
                if min_ret > 0:
                    df_price_filter = df_price[df_price["close"] >= (df_price['close'].iloc[0]*(1+min_ret))]
                elif min_ret < 0:
                    df_price_filter = df_price[df_price["close"] <= (df_price['close'].iloc[0]*(1+min_ret))]
            else:
                print("flag_stat_return Error: Unrecognized Ref_Field. Needs to be 'ask' or 'close': ", ref_field)
                return [None, None]

            if len(df_price_filter) > 0:
                price = pd.to_numeric(df_price_filter[output_field].iloc[0])
                return [price, df_price_filter['date'].iloc[0]]
            else:
                return [None, None]
        except Exception as e:
            print("flag_stat_return ERROR | {0} {1} | {2} | {3}".format(df_price['option_symbol'].iloc[0], df_price['date'].iloc[0], min_ret + ref_field + output_field, e))
            return [None, None]

    def flag_stat_OI_change(self, df_price, OI_change, OI_change_pct, output_field):

        min_OI_change = max(250, abs(OI_change * OI_change_pct))

        try:
            if OI_change_pct < 0:
                df_price_filter = df_price[(df_price["open_interest_change"] <= -min_OI_change) & (df_price['volume'] >= min_OI_change)]
            elif OI_change_pct > 0:
                df_price_filter = df_price[(df_price["open_interest_change"] >= min_OI_change) & (df_price['volume'] >= min_OI_change)]

            if len(df_price_filter) > 0:
                price = pd.to_numeric(df_price_filter[output_field].iloc[0])
                return [price, df_price_filter['date'].iloc[0]]
            else:
                return [None, None]
        except Exception as e:
            print("flag_stat_OI_change ERROR | {0} {1} | {2}, {3} | {4}".format(df_price['option_symbol'].iloc[0], df_price['date'].iloc[0], OI_change, OI_change_pct, e))
            return [None, None]

    def flag_stat_add_flag(self, df_price, flag, output_field):

        try:
            df_temp = df_price.iloc[1:]

            if flag == 'C':
                df_price_filter = df_temp[df_temp["call_ind"]==1]
            elif flag == 'P':
                df_price_filter = df_temp[df_temp["put_ind"]==1]
            else:
                print("flag_stat_add_flag ERROR: Invalid flag, has to be 'C' or 'P' : ",flag)
                return [None, None]

            if len(df_price_filter) > 0:

                price = pd.to_numeric(df_price_filter[output_field].iloc[0])
                return [price, df_price_filter['date'].iloc[0]]
            else:
                return [None, None]
        except Exception as e:
            print("flag_stat_add_flag ERROR | {0} {1} | {2} | {3}".format(df_price['option_symbol'].iloc[0], df_price['date'].iloc[0], flag + output_field, e))
            return [None, None]






            # # UNDERLYING STATISTICS (MIN/MAX OPEN, CLOSE, HIGH, LOW, OPT MID, OPT LAST)
            #     index, value = max(enumerate(flag_price['open']), key=operator.itemgetter(1))
            #     flag_stats.loc[(flag_s,flag_date),'open_high'] = value
            #     flag_stats.loc[(flag_s,flag_date),'open_high_date'] = flag_price['date'].loc[index]
            #
            #
            # # STATISTICS BEFORE MAX RET (MAX DRAWDOWN FOR OPT MID, UNDERLYING)
            #     max_ret_date = flag_price['date'].loc[index]
            #     flag_prehigh = flag_price[flag_price['date'] <= max_ret_date]
            #     # print(flag_prehigh)
            #
            #     index, value = min(enumerate(flag_prehigh['mid']), key=operator.itemgetter(1))
            #     flag_stats.loc[(flag_s,flag_date),'opt_mid_drawdown_bmax'] = value
            #     flag_stats.loc[(flag_s,flag_date),'opt_mid_drawdown_bmax_date'] = flag_price['date'].loc[index]

                # ITM STATISTICS (TOUCH/CLOSE ITM OPT MID PRICE AND DATE)

                # if flag.call_put == 'C':
                #     flag_price_high_ITM = flag_price[flag_price['high'] >= flag_strike]
                #     if len(flag_price_high_ITM) > 0:
                #         flag_stats.loc[(flag_s,flag_date), 'hit_itm_mid'] = flag_price_high_ITM['mid'].iloc[0]
                #         flag_stats.loc[(flag_s,flag_date), 'hit_itm_date'] = flag_price_high_ITM['date'].iloc[0]
                #
                #     flag_price_close_ITM = flag_price[flag_price['close'] >= flag_strike]
                #     if len(flag_price_close_ITM) > 0:
                #         flag_stats.loc[(flag_s,flag_date), 'close_itm_mid'] = flag_price_close_ITM['mid'].iloc[0]
                #         flag_stats.loc[(flag_s,flag_date), 'close_itm_date'] = flag_price_close_ITM['date'].iloc[0]
                #
                # elif flag.call_put == 'P':
                #     flag_price_low_ITM = flag_price[flag_price['low'] <= flag_strike]
                #     if len(flag_price_low_ITM) > 0:
                #         flag_stats.loc[(flag_s,flag_date), 'hit_itm_mid'] = flag_price_low_ITM['mid'].iloc[0]
                #         flag_stats.loc[(flag_s,flag_date), 'hit_itm_date'] = flag_price_low_ITM['date'].iloc[0]
                #
                #     flag_price_close_ITM = flag_price[flag_price['close'] <= flag_strike]
                #     if len(flag_price_low_ITM) > 0:
                #         flag_stats.loc[(flag_s,flag_date), 'close_itm_mid'] = flag_price_close_ITM['mid'].iloc[0]
                #         flag_stats.loc[(flag_s,flag_date), 'close_itm_date'] = flag_price_close_ITM['date'].iloc[0]



        # print("1")
        # flag_stats.loc[(flag.option_symbol,flag_date),['open_high','open_high_date']] = self.flag_stat_extract(flag_price,"max","open")
        # print("2")
        # flag_stats.loc[(flag.option_symbol,flag_date),['open_low','open_low_date']] = self.flag_stat_extract(flag_price,"in","open")
        # flag_stats.loc[(flag.option_symbol,flag_date),['close_high','close_high_date']] = self.flag_stat_extract(flag_price,"max","close")
        # flag_stats.loc[(flag.option_symbol,flag_date),['close_low','close_low_date']] = self.flag_stat_extract(flag_price,"min","close")
        # flag_stats.loc[(flag.option_symbol,flag_date),['max_high','max_high_date']] = self.flag_stat_extract(flag_price,"max","high")
        # flag_stats.loc[(flag.option_symbol,flag_date),['min_low','min_low_date']] = self.flag_stat_extract(flag_price,"min","low")
        # flag_stats.loc[(flag.option_symbol,flag_date),['opt_last_high','opt_last_high_date']] = self.flag_stat_extract(flag_price,"max","last")
        # flag_stats.loc[(flag.option_symbol,flag_date),['opt_mid_high','opt_mid_high_date']] = self.flag_stat_extract(flag_price,"max","mid")
        #
        # max_ret_date = flag_stats.loc[(flag.option_symbol,flag_date),'opt_mid_high_date']
        # flag_prehigh = flag_price[flag_price['date'] <= max_ret_date]
        # flag_stats.loc[(flag.option_symbol,flag_date),['opt_mid_drawdown','opt_mid_drawdown_date']] = self.flag_stat_extract(flag_prehigh,"min","mid")
        # if flag.call_put == 'C':
        #     flag_stats.loc[(flag.option_symbol, flag_date), ['max_drawdown', 'max_drawdown_date']] = self.flag_stat_extract(flag_prehigh, "min", "low")
        # elif flag.call_put == 'P':
        #     flag_stats.loc[(flag.option_symbol, flag_date), ['max_drawdown', 'max_drawdown_date']] = self.flag_stat_extract(flag_prehigh, "max", "high")

        # flag_price['date'] = flag_price['date'].strftime('%Y-%m-%d')
        # print("Total Call Ind: {0} ; Total Put Ind {1}".format(flag_price['call_ind'].sum(),flag_price['put_ind'].sum()))


if __name__ == '__main__':
    # register_adapter(numpy.float64, addapt_numpy_float64)

    # ticker = ["GME", 'TPX', 'TROX', 'AAPL', 'JAG', 'BBBY', 'QCOM', 'FDC', 'BLL', 'XRT']
    #
    # ticker = ["GME",'TPX','TROX','AAPL','JAG','BBBY','QCOM','FDC','BLL','XRT','DPLO','USG','CPB','WWE','FOSL','WIN','ACXM']
    ticker = ['GNMK']

    fl = Flag()
    # fl.unusual_screenTest(days=0)
    fl.export("OPTION_FLAG.csv")

    # fl.analyzeFlags()
    # fl.analyze()
