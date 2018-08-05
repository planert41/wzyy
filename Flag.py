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
            if abs((df - data_date.date()).days) <= 70:
                return "FM"
            else:
                return "BM"
        else:
            return "WK"

    def unusual_screen(self, tickers =[], date=None, days=1):


    # Function defaults to screening ticker for today - 1 Day
        flag_start = time.time()
        total_flags = 0
        conn = create_engine('postgresql://postgres:inkstain@localhost:5432/wzyy_options')

    # Set date as latest date as default
        if date is None:
            date = dt.datetime.now()
            print("Default Date: ", date)

    # Using 20 days avg to calculate historical option stats. Queries for days processed + avg length. Cap avg at 3 std
            avg_window = 20
            z_limit = 2.5

        print("Flag Start : {0} Tickers | {1} | {2} ".format(len(tickers), date, days))

    # LOOP THROUGH TICKERS

        for ticker_inp in tickers:
            print(ticker_inp)

            if days == 0:
                days = conn.execute("select count(*) from option_stat where symbol = '{0}'".format(ticker_inp)).fetchone()[0]

            ticker_start = time.time()
            printid = "{0} {1}: {2} Day".format(ticker_inp, date.strftime('%Y-%m-%d'), days)
            print("{0} | Start Screen".format(printid))

        #   READ IN OPTION STAT, TICKER FOR X DAYS + AVERAGE WINDOW
            try:
                request = "SELECT * FROM option_stat WHERE symbol = '{0}' AND date <= '{1}' ORDER BY date ASC LIMIT {2} ;".format(ticker_inp, date, days + avg_window)
                df_stat = pd.read_sql_query(request, con=conn)
                df_stat.set_index(pd.DatetimeIndex(df_stat['date']), inplace=True)

            # FIND START AND END DATES FOR FLAGS
                flag_dates = df_stat['date'].sort_values(ascending=False)
                flag_dates = flag_dates[flag_dates <= date.date()].head(days)
                start_date = flag_dates.min()
                end_date = flag_dates.max()
                print("{0} | {1} Stat Record | {2} to {3}".format(printid, len(flag_dates), start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d')))

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


                print("{0} - {1} Daily Records {2} rolling averages".format(printid, len(df_stat), df_stat['call_vol_mean_adj'].astype(bool).sum(axis=0)))
                # df_stat.to_csv("stat_check.csv")

            except Exception as e:
                print("Error Retrieving Option Stats | {0} | {1} | {2}".format(ticker_inp, date, e))
                continue

    #   READ IN ALL DAILY OPTION DATA BETWEEN FLAG START AND END DATE

            try:
                request = "SELECT * FROM option_data WHERE symbol = '{0}' AND date >= '{1}' AND date <= '{2}' ORDER BY date DESC;".format(ticker_inp, start_date, end_date)
                df_daily = pd.read_sql(request, conn)
                print("{0} | {1} to {2} | {3} Recs".format(printid, start_date, end_date, len(df_daily)))

            except Exception as e:
                print("Query option_data ERROR: {0} | {1} to {2} | {3}".format(printid, start_date, end_date, e))
                continue

        # SKIP IF NO OPTIONS DATA

            if len(df_daily) == 0:
                print("{0} - ERROR: No daily options data".format(printid))
                continue

        # MERGE IN OPTION AVERAGES/STD TO DAILY OPTION DATA FOR COMPARISON

            mergeCols = ['call_vol_mean_adj', 'call_vol_std_adj', 'put_vol_mean_adj', 'put_vol_std_adj', 'fm_call_vol_mean_adj', 'fm_call_vol_std_adj', 'fm_put_vol_mean_adj', 'fm_put_vol_std_adj', 'largest_vol_mean_adj',
                         'largest_vol_std_adj','largest_5dayoi_mean_adj','largest_5dayoi_vol_adj']
            df_stat = df_stat[mergeCols].reset_index()

            # Convert DateTime Index to Date
            df_stat['date'] = df_stat['date'].apply(lambda x: x.date())

            # Merge Avg Option Volumes to Daily Option Data
            df_daily = pd.merge(df_daily, df_stat, how='left', on=['date'])

        # CALCULATE Z SCORES FOR TOTAL VOL, FM VOL, LARGEST OPTION VOL, LARGEST 5 DAY OI INCREASE. EACH OPTION CAN HAVE MULTIPLE FLAGS

            df_daily['category'] = df_daily['option_expiration'].apply(lambda x: self.option_category(x, date))

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

            df_daily['open_interest_5day_change_pct'] = df_daily.apply(lambda x: (x['open_interest']/x['open_interest_5day']) - 1 if x['open_interest_5day'] > 0 else 10, axis=1)
            df_daily['open_interest_5day_change_pct'].fillna(0)

            # df_daily.to_csv(ticker_inp + "_Daily.csv")

            temp_flags = df_daily[df_daily['flag'] != 0].copy()
            # Dropping the ind call/put z scores as they are already reflected in the total/fm z scores. The other zscore would just be 0s
            temp_flags.drop(columns=['total_call_z', 'total_put_z', 'fm_call_z', 'fm_put_z'], inplace=True)

            # print(temp_flags.head())

            try:
                temp_flags.to_sql("option_flag", conn, if_exists='append', index=False)
                ticker_end = time.time()
                print('{0} | {1} Flags Uploaded | {2}'.format(printid, len(temp_flags), ticker_end - ticker_start))
                total_flags += len(temp_flags)

            except Exception as e:
                print('{0} | Uploaded Flags ERROR | {1}'.format(printid, e))
                continue

        conn.dispose()
        flag_end = time.time()
        print("Flag Complete : {0} Tickers | {1} | {2} | {3} Flags | {4}".format(len(tickers), date, days, total_flags, flag_end - flag_start))


######################################################################################################################


    def analyze(self, tickers = []):
        conn = create_engine('postgresql://postgres:inkstain@localhost:5432/wzyy_options')
        count = 0

    # READ IN ALL FLAGS
        try:
            request = "SELECT * FROM option_flag ORDER BY symbol, date asc;"
            df_flags = pd.read_sql_query(request, con=conn)
            df_flags['call_ind'] = df_flags.apply(lambda x: 1 if x['call_put'] == 'C' else 0, axis=1)
            df_flags['put_ind'] = df_flags.apply(lambda x: 1 if x['call_put'] == 'P' else 0, axis=1)

            if len(tickers) > 0:
                df_flags = df_flags[df_flags["symbol"].isin(tickers)]
            print("Analyze Flags: Read Flags | {0} Tickers | {1} Flags".format(df_flags['symbol'].nunique(),len(df_flags)))

        except Exception as e:
            print("Analyze Flags: Read Flags Error ", e)

        if len(df_flags) == 0:
            return

     # SEPARATE FLAGS BASED ON TICKER

        temp_df_flags = df_flags.groupby("symbol", as_index=False)
        df_flags['date'] = df_flags['date'].apply(lambda x: dt.datetime.strftime(x, '%Y-%m-%d'))


        tuples = list(zip(df_flags['option_symbol'],df_flags['date']))
        index = pd.MultiIndex.from_tuples(tuples, names=['first', 'second'])
        flag_stats = DataFrame(columns=[], index=index)
        # print(flag_stats.info())
        # print(flag_stats)
        # print("Index Test ", flag_stats.loc[('AAPL  180316P00155000', '2018-02-09')])

        for symbol, flags in temp_df_flags:
            symbol_min_date = flags['date'].min()
            symbol_max_date = flags['option_expiration'].max()
            # print(symbol, symbol_min_date, symbol_max_date)

            # flag_merge.set_index(pd.DatetimeIndex(flag_merge['date']), inplace=True)
            # flag_merge.drop('date', axis=1, inplace=True)

            # READ IN UNDERLYING PRICE  FROM FLAG DATE TO OPTION EXPIRY
            request = "SELECT * FROM underlying_data where symbol = '{0}' and date >= '{1}' and date <= '{2}' order by date asc".format(symbol, symbol_min_date, symbol_max_date)
            df_price = pd.read_sql_query(request, con=conn)
            if len(df_price) ==0:
                print("Read Price Error: No Price Data For ", symbol)
            # print("Read OCHL Prices | {0} | {1} Recs".format(symbol, len(df_price)))

            # df_price.set_index(pd.DatetimeIndex(df_price['date']), inplace=True)
            # df_price.drop('date', axis=1, inplace=True)

        # MERGE IN ALL FLAGS INTO PRICE DATA
            pm = len(df_price)
            # flag_merge = flags[['date','call_ind','put_ind']].copy()
            # Handles days with multiple flags, calls and puts
            flag_merge = flags.groupby(['date'])[["call_ind", "put_ind"]].sum()

            df_price = pd.merge(df_price, flag_merge, on='date', how='left')
            am = len(df_price)
            if (am != pm):
                print("Flag - Price Merge Error | {0} | {1} to {2}".format(symbol, pm, am))

            # print("Merge Prices | {0} | {1} to {2} | {3} Prices".format(symbol, symbol_min_date, symbol_max_date, len(df_price)))
            # print(df_price.info())
            # print(df_price.head())

        # READ IN INDIVIDUAL OPTION DATA
            for flag in flags.itertuples():
                # flag_date = flag.date.strftime('%Y-%m-%d')
                flag_date = flag.date
                request = "SELECT * FROM option_data where option_symbol ='{0}' and date >= '{1}' order by date asc".format(flag.option_symbol, flag.date)
                flag_price = pd.read_sql_query(request, con=conn)
                # print('Read Option Prices | {0} | {1}'.format(flag.option_symbol, len(flag_price)))

                flag_price['mid'] = flag_price.apply(lambda x: x['last'] if x['bid'] <= x['last'] <= x['ask'] else (x['bid'] + x['ask'])/2, axis=1)
                # flag_price.set_index(pd.DatetimeIndex(flag_price['date']), inplace=True)
                # flag_price.drop('date', axis=1, inplace=True)

                # print(flag_price.info())
                # print(df_price.info())

            # MERGE FLAG OPTION DATA WITH OCHL PRICE DATA
                pm = len(flag_price)
                flag_price = pd.merge(flag_price, df_price, on='date', how='left')
                am = len(flag_price)
                if (am != pm):
                    print("Ind Flag - Price Merge Error | {0} | {1} to {2}".format(flag.option_symbol, pm, am))
                # print("Option and OCHL Merged | {0} | {1}".format(flag.option_symbol, len(flag_price)))

            # FILL OUT OPTION FLAG DETAILS
                index, value = max(enumerate(flag_price['open']), key=operator.itemgetter(1))
                flag_stats.loc[(flag.option_symbol,flag_date),'open_high'] = value
                flag_stats.loc[(flag.option_symbol,flag_date),'open_high_date'] = flag_price['date'].loc[index]

                index, value = min(enumerate(flag_price['open']), key=operator.itemgetter(1))
                flag_stats.loc[(flag.option_symbol,flag_date),'open_low'] = value
                flag_stats.loc[(flag.option_symbol,flag_date),'open_low_date'] = flag_price['date'].loc[index]

                index, value = max(enumerate(flag_price['close']), key=operator.itemgetter(1))
                flag_stats.loc[(flag.option_symbol,flag_date),'close_high'] = value
                flag_stats.loc[(flag.option_symbol,flag_date),'close_high_date'] = flag_price['date'].loc[index]

                index, value = min(enumerate(flag_price['close']), key=operator.itemgetter(1))
                flag_stats.loc[(flag.option_symbol,flag_date),'close_low'] = value
                flag_stats.loc[(flag.option_symbol,flag_date),'close_low_date'] = flag_price['date'].loc[index]

                index, value = max(enumerate(flag_price['high']), key=operator.itemgetter(1))
                flag_stats.loc[(flag.option_symbol,flag_date),'max_high'] = value
                flag_stats.loc[(flag.option_symbol,flag_date),'max_high_date'] = flag_price['date'].loc[index]

                index, value = min(enumerate(flag_price['low']), key=operator.itemgetter(1))
                flag_stats.loc[(flag.option_symbol,flag_date),'min_low'] = value
                flag_stats.loc[(flag.option_symbol,flag_date),'min_low_date'] = flag_price['date'].loc[index]

                index, value = max(enumerate(flag_price['last']), key=operator.itemgetter(1))
                flag_stats.loc[(flag.option_symbol,flag_date),'opt_last_high'] = value
                flag_stats.loc[(flag.option_symbol,flag_date),'opt_last_high_date'] = flag_price['date'].loc[index]

                index, value = max(enumerate(flag_price['mid']), key=operator.itemgetter(1))
                flag_stats.loc[(flag.option_symbol,flag_date),'opt_mid_high'] = value
                flag_stats.loc[(flag.option_symbol,flag_date),'opt_mid_high_date'] = flag_price['date'].loc[index]

                # print(flag_price)
                max_ret_date = flag_price['date'].loc[index]
                # print(max_ret_date)

                flag_prehigh = flag_price[flag_price['date'] <= max_ret_date]
                # print(flag_prehigh)

                index, value = min(enumerate(flag_prehigh['mid']), key=operator.itemgetter(1))
                flag_stats.loc[(flag.option_symbol,flag_date),'opt_mid_drawdown'] = value
                flag_stats.loc[(flag.option_symbol,flag_date),'opt_mid_drawdown_date'] = flag_price['date'].loc[index]

                if flag.call_put == 'C':
                    index, value = min(enumerate(flag_prehigh['low']), key=operator.itemgetter(1))
                    flag_stats.loc[(flag.option_symbol,flag_date), 'max_drawdown'] = value
                    flag_stats.loc[(flag.option_symbol,flag_date), 'max_drawdown_date'] = flag_price['date'].loc[index]
                elif flag.call_put == 'P':
                    index, value = max(enumerate(flag_prehigh['high']), key=operator.itemgetter(1))
                    flag_stats.loc[(flag.option_symbol,flag_date), 'max_drawdown'] = value
                    flag_stats.loc[(flag.option_symbol,flag_date), 'max_drawdown_date'] = flag_price['date'].loc[index]

                count += 1
                print("Processed | {0} | {1}/{2}".format(flag.option_symbol, count, len(df_flags)))
        # print(flag_stats)
        flag_stats.index.names = ['option_symbol','date']
        # flag_stats.reset_index(inplace=True)
        pm = len(df_flags)
        df_flags = pd.merge(df_flags, flag_stats, on=['option_symbol','date'], how='left')
        am = len(df_flags)
        if (am != pm):
            print("Final Flag - Stats Merge Error | {0} | {1} to {2}".format(flag.option_symbol, pm, am))

        df_flags.to_csv('flag_analyze.csv')
        print("Finish Analyzing Flags | {0} Tickers | {1} Flags".format(df_flags['symbol'].nunique(), len(df_flags)))
        conn.dispose()



if __name__ == '__main__':
    ticker = ["GME", 'TPX', 'TROX', 'AAPL', 'JAG', 'BBBY', 'QCOM', 'FDC', 'BLL', 'XRT']
    ticker = ['TPX']
    ticker = ["GME",'TPX','TROX','AAPL','JAG','BBBY','QCOM','FDC','BLL','XRT','DPLO','USG','CPB','WWE','FOSL','WIN','ACXM']

    fl = Flag()
    # fl.unusual_screen(ticker, days=0)
    fl.analyze()
