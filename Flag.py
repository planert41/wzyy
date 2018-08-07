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
                temp_flags = self.analyze(temp_flags)
            except Exception as e:
                print("Flag Analysis Error | ", e)

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
            return

        conn = create_engine('postgresql://postgres:inkstain@localhost:5432/wzyy_options')
        count = 0

        df_flags['call_ind'] = df_flags.apply(lambda x: 1 if x['call_put'] == 'C' else 0, axis=1)
        df_flags['put_ind'] = df_flags.apply(lambda x: 1 if x['call_put'] == 'P' else 0, axis=1)
        df_flags['date'] = df_flags['date'].apply(lambda x: dt.datetime.strftime(x, '%Y-%m-%d'))

    # CREATE DUMMY FLAG STAT TABLE WITH OPTION SYMBOL AND FLAG DATE AS INDEX
        tuples = list(zip(df_flags['option_symbol'],df_flags['date']))
        index = pd.MultiIndex.from_tuples(tuples, names=['first', 'second'])
        stat_col = ['open_high', 'open_high_date', 'open_low', 'open_low_date', 'close_high', 'close_high_date', 'close_low', 'close_low_date']
        stat_col+= ['max_high', 'max_high_date', 'min_low', 'min_low_date', 'opt_last_high', 'opt_last_high_date', 'opt_mid_high', 'opt_mid_high_date']
        stat_col+= ['opt_mid_drawdown_bmax', 'opt_mid_drawdown_bmax_date', 'max_drawdown_bmax', 'max_drawdown_bmax_date', 'hit_itm_mid', 'hit_itm_date', 'close_itm_mid', 'close_itm_date']
        stat_col+= ['close_sma200_mid','close_sma200_date','close_sma100_mid','close_sma100_date','close_sma50_mid','close_sma50_date','close_ema8_mid','close_ema8_date']
        stat_col+= ['high_52w_mid','high_52w_date','low_52w_mid','low_52w_date']
        stat_col+= ['opt_n50ret_mid','opt_n50ret_date','opt_p100ret_mid','opt_p100ret_date','opt_p200ret_mid','opt_p200ret_date']
        stat_col+= ['stock_p5ret_mid','stock_p5ret_date','stock_p10ret_mid','stock_p10ret_date','stock_n5ret_mid','stock_n5ret_date','stock_n10ret_mid','stock_n10ret_date']
        stat_col+= ['addflag_call_mid','addflag_call_mid_date','addflag_put_mid','addflag_put_mid_date']

        stat_col+= ['call_flag', 'call_flag_dates', 'put_flag', 'put_flag_dates']

        flag_stats = DataFrame(columns=stat_col, index=index)
        print(flag_stats)


        flag_stat_cols = []

        # print(flag_stats.info())
        # print(flag_stats)
        # print("Index Test ", flag_stats.loc[('AAPL  180316P00155000', '2018-02-09')])


    # SPLIT FLAGS BY SYMBOL TO READ UNDERLYING DATA ONLY ONCE
        temp_df_flags = df_flags.groupby("symbol", as_index=False)

        for symbol, flags in temp_df_flags:
            symbol_min_date = flags['date'].min()
            symbol_max_date = flags['option_expiration'].max()
            print("Processing | {0} | {1} to {2} | {3} CALLS {4} PUTS".format(symbol, symbol_min_date, symbol_max_date, flags['call_ind'].sum(),flags['put_ind'].sum()))

            # flag_merge.set_index(pd.DatetimeIndex(flag_merge['date']), inplace=True)
            # flag_merge.drop('date', axis=1, inplace=True)

            # READ IN UNDERLYING DATA FROM FLAG DATE TO OPTION EXPIRY
            request = "SELECT * FROM underlying_data where symbol = '{0}' and date >= '{1}' and date <= '{2}' order by date asc".format(symbol, symbol_min_date, symbol_max_date)
            df_price = pd.read_sql_query(request, con=conn)
            if len(df_price) ==0:
                print("Read Price Error: No Price Data For ", symbol)
            # print("Read OCHL Prices | {0} | {1} Recs".format(symbol, len(df_price)))

            # df_price.set_index(pd.DatetimeIndex(df_price['date']), inplace=True)
            # df_price.drop('date', axis=1, inplace=True)

        # MERGE FLAG_IND WITH UNDERLYING PRICE DATA
            pm = len(df_price)
            # flag_merge = flags[['date','call_ind','put_ind']].copy()
            # Handles days with multiple flags, calls and puts
            flag_merge = flags.groupby(['date'])[["call_ind", "put_ind"]].sum()
            flag_merge.reset_index(inplace=True)

            df_price['date'] = pd.to_datetime(df_price['date'], format="%Y-%m-%d")
            flag_merge['date'] = pd.to_datetime(flag_merge['date'], format="%Y-%m-%d")

            df_price = pd.merge(df_price, flag_merge, on='date', how='outer')
            df_price['call_ind'].fillna(0)
            df_price['put_ind'].fillna(0)


            am = len(df_price)
            if (am != pm):
                print("Flag - Price Merge Error | {0} | {1} to {2}".format(symbol, pm, am))

        # READ FLAG OPTION DAILY DATA
            for flag in flags.itertuples():

                # flag_date = flag.date.strftime('%Y-%m-%d')
                flag_s = flag.option_symbol
                flag_date = flag.date
                flag_strike = flag.strike
                request = "SELECT * FROM option_data where option_symbol ='{0}' and date >= '{1}' order by date asc".format(flag.option_symbol, flag.date)
                flag_price = pd.read_sql_query(request, con=conn)
                # print('Read Option Prices | {0} | {1}'.format(flag.option_symbol, len(flag_price)))
                print("{0} | {1} - {2} | {3} Prices".format(flag.option_symbol, flag_price['date'].min(), flag_price['date'].max(), len(flag_price)))

                # CALCULATED MID PRICE - LAST IF LAST IN (BID/ASK), ELSE IF BID/ASK SPREAD WIDER THAN BID, MID = 1.5 BID, ELSE MID(BID+ASK)
                flag_price['mid'] = flag_price.apply(lambda x: x['last'] if x['bid'] <= x['last'] <= x['ask'] else ((x['bid'] + x['ask'])/2 if ((x['ask'] - x['bid'])/x['bid']) < 1 else x['bid']*1.5), axis=1)
                # flag_price.set_index(pd.DatetimeIndex(flag_price['date']), inplace=True)
                # flag_price.drop('date', axis=1, inplace=True)

                # print(flag_price.info())
                # print(df_price.info())

        # MERGE FLAG OPTION DAILY DATA WITH UNDERLYING OCHL DATA
                pm = len(flag_price)
                flag_price['date'] = pd.to_datetime(flag_price['date'], format="%Y-%m-%d")
                flag_price = pd.merge(flag_price, df_price, on=['symbol','date'], how='left')
                am = len(flag_price)
                if (am != pm):
                    print("Ind Flag - Price Merge Error | {0} | {1} to {2}".format(flag.option_symbol, pm, am))
                # print("Option and OCHL Merged | {0} | {1}".format(flag.option_symbol, len(flag_price)))

            # FILL OUT OPTION FLAG DETAILS
            #     print(flag_price.info())

                flag_stats.loc[(flag.option_symbol,flag_date),['open_high','open_high_date']] = self.flag_stat_minmax(flag_price,"max","open")
                flag_stats.loc[(flag.option_symbol,flag_date),['open_low','open_low_date']] = self.flag_stat_minmax(flag_price,"min","open")
                flag_stats.loc[(flag.option_symbol,flag_date),['close_high','close_high_date']] = self.flag_stat_minmax(flag_price,"max","close")
                flag_stats.loc[(flag.option_symbol,flag_date),['close_low','close_low_date']] = self.flag_stat_minmax(flag_price,"min","close")
                flag_stats.loc[(flag.option_symbol,flag_date),['max_high','max_high_date']] = self.flag_stat_minmax(flag_price,"max","high")
                flag_stats.loc[(flag.option_symbol,flag_date),['min_low','min_low_date']] = self.flag_stat_minmax(flag_price,"min","low")


                flag_stats.loc[(flag.option_symbol,flag_date),['opt_last_high','opt_last_high_date']] = self.flag_stat_minmax(flag_price,"max","last")
                flag_stats.loc[(flag.option_symbol,flag_date),['opt_mid_high','opt_mid_high_date']] = self.flag_stat_minmax(flag_price,"max","mid")

                max_ret_date = flag_stats.loc[(flag.option_symbol,flag_date),'opt_mid_high_date']
                flag_prehigh = flag_price[flag_price['date'] <= max_ret_date]
                flag_stats.loc[(flag.option_symbol,flag_date),['opt_mid_drawdown_bmax','opt_mid_drawdown_bmax_date']] = self.flag_stat_minmax(flag_prehigh,"min","mid")

                if flag.call_put == 'C':
                    flag_stats.loc[(flag.option_symbol, flag_date), ['max_drawdown_bmax', 'max_drawdown_bmax_date']] = self.flag_stat_minmax(flag_prehigh, "min", "low")
                elif flag.call_put == 'P':
                    flag_stats.loc[(flag.option_symbol, flag_date), ['max_drawdown_bmax', 'max_drawdown_bmax_date']] = self.flag_stat_minmax(flag_prehigh, "max", "high")

                # print('test')

                flag_stats.loc[(flag.option_symbol,flag_date),['hit_itm_mid','hit_itm_date']] = self.flag_stat_ITM(flag_price, flag.call_put,flag.strike,"touch","mid")
                flag_stats.loc[(flag.option_symbol,flag_date),['close_itm_mid','close_itm_date']] = self.flag_stat_ITM(flag_price, flag.call_put,flag.strike,"close","mid")

                flag_stats.loc[(flag.option_symbol,flag_date),['close_sma200_mid','close_sma200_date']] = self.flag_stat_touch(flag_price,"sma_200","close","mid")
                flag_stats.loc[(flag.option_symbol,flag_date),['close_sma100_mid','close_sma100_date']] = self.flag_stat_touch(flag_price,"sma_100","close","mid")
                flag_stats.loc[(flag.option_symbol,flag_date),['close_sma50_mid','close_sma50_date']] = self.flag_stat_touch(flag_price,"sma_50","close","mid")
                flag_stats.loc[(flag.option_symbol,flag_date),['close_ema8_mid','close_ema8_date']] = self.flag_stat_touch(flag_price,"ema_8","close","mid")
                flag_stats.loc[(flag.option_symbol,flag_date),['high_52w_mid','high_52w_date']] = self.flag_stat_touch(flag_price,"high_52w","high","mid")
                flag_stats.loc[(flag.option_symbol,flag_date),['low_52w_mid','low_52w_date']] = self.flag_stat_touch(flag_price,"low_52w","low","mid")

                flag_stats.loc[(flag.option_symbol,flag_date),['opt_n50ret_mid','opt_n50ret_date']] = self.flag_stat_return(flag_price,-0.5,"ask","mid")
                flag_stats.loc[(flag.option_symbol,flag_date),['opt_p100ret_mid','opt_p100ret_date']] = self.flag_stat_return(flag_price,1,"ask","mid")
                flag_stats.loc[(flag.option_symbol,flag_date),['opt_p200ret_mid','opt_p200ret_date']] = self.flag_stat_return(flag_price,2,"ask","mid")

                flag_stats.loc[(flag.option_symbol,flag_date),['stock_p5ret_mid','stock_p5ret_date']] = self.flag_stat_return(flag_price,0.05,"close","mid")
                flag_stats.loc[(flag.option_symbol,flag_date),['stock_p10ret_mid','stock_p10ret_date']] = self.flag_stat_return(flag_price,0.1,"close","mid")
                flag_stats.loc[(flag.option_symbol,flag_date),['stock_n5ret_mid','stock_n5ret_date']] = self.flag_stat_return(flag_price,-0.05,"close","mid")
                flag_stats.loc[(flag.option_symbol,flag_date),['stock_n10ret_mid','stock_n10ret_date']] = self.flag_stat_return(flag_price,-0.1,"close","mid")

                flag_stats.loc[(flag.option_symbol,flag_date),['addflag_call_mid','addflag_call_mid_date']] = self.flag_stat_add_flag(flag_price, 'C', 'mid')
                flag_stats.loc[(flag.option_symbol,flag_date),['addflag_put_mid','addflag_put_mid_date']] = self.flag_stat_add_flag(flag_price, 'P', 'mid')
                print('test')





            # # UNDERLYING STATISTICS (MIN/MAX OPEN, CLOSE, HIGH, LOW, OPT MID, OPT LAST)
            #     index, value = max(enumerate(flag_price['open']), key=operator.itemgetter(1))
            #     flag_stats.loc[(flag_s,flag_date),'open_high'] = value
            #     flag_stats.loc[(flag_s,flag_date),'open_high_date'] = flag_price['date'].loc[index]
            #
            #     index, value = min(enumerate(flag_price['open']), key=operator.itemgetter(1))
            #     flag_stats.loc[(flag_s,flag_date),'open_low'] = value
            #     flag_stats.loc[(flag_s,flag_date),'open_low_date'] = flag_price['date'].loc[index]
            #
            #     index, value = max(enumerate(flag_price['close']), key=operator.itemgetter(1))
            #     flag_stats.loc[(flag_s,flag_date),'close_high'] = value
            #     flag_stats.loc[(flag_s,flag_date),'close_high_date'] = flag_price['date'].loc[index]
            #
            #     index, value = min(enumerate(flag_price['close']), key=operator.itemgetter(1))
            #     flag_stats.loc[(flag_s,flag_date),'close_low'] = value
            #     flag_stats.loc[(flag_s,flag_date),'close_low_date'] = flag_price['date'].loc[index]
            #
            #     index, value = max(enumerate(flag_price['high']), key=operator.itemgetter(1))
            #     flag_stats.loc[(flag_s,flag_date),'max_high'] = value
            #     flag_stats.loc[(flag_s,flag_date),'max_high_date'] = flag_price['date'].loc[index]
            #
            #     index, value = min(enumerate(flag_price['low']), key=operator.itemgetter(1))
            #     flag_stats.loc[(flag_s,flag_date),'min_low'] = value
            #     flag_stats.loc[(flag_s,flag_date),'min_low_date'] = flag_price['date'].loc[index]
            #
            #     index, value = max(enumerate(flag_price['last']), key=operator.itemgetter(1))
            #     flag_stats.loc[(flag_s,flag_date),'opt_last_high'] = value
            #     flag_stats.loc[(flag_s,flag_date),'opt_last_high_date'] = flag_price['date'].loc[index]
            #
            #     index, value = max(enumerate(flag_price['mid']), key=operator.itemgetter(1))
            #     flag_stats.loc[(flag_s,flag_date),'opt_mid_high'] = value
            #     flag_stats.loc[(flag_s,flag_date),'opt_mid_high_date'] = flag_price['date'].loc[index]
            #
            #     # print(flag_price)
            #     # print(max_ret_date)
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
            #
            #     if flag.call_put == 'C':
            #         index, value = min(enumerate(flag_prehigh['low']), key=operator.itemgetter(1))
            #         flag_stats.loc[(flag_s,flag_date), 'max_drawdown_bmax'] = value
            #         flag_stats.loc[(flag_s,flag_date), 'max_drawdown_bmax_date'] = flag_price['date'].loc[index]
            #
            #
            #     elif flag.call_put == 'P':
            #         index, value = max(enumerate(flag_prehigh['high']), key=operator.itemgetter(1))
            #         flag_stats.loc[(flag_s,flag_date), 'max_drawdown_bmax'] = value
            #         flag_stats.loc[(flag_s,flag_date), 'max_drawdown_bmax_date'] = flag_price['date'].loc[index]

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

                # OTHER FLAG DATES IN SEQUENCE

                if flag_price['call_ind'].sum() > 0:
                    flag_stats.loc[(flag_s,flag_date),'call_flag'] = flag_price['call_ind'].sum()
                    call_dates = flag_price[flag_price['call_ind'] == 1]['date'].apply(lambda x: x.strftime('%Y-%m-%d')).tolist()
                    flag_stats.loc[(flag_s,flag_date),'call_flag_dates'] = ",".join(call_dates)

                    # print(flag_stats.loc[(flag_s, flag_date), 'call_flag_dates'])

                if flag_price['put_ind'].sum() > 0:
                    flag_stats.loc[(flag_s,flag_date),'put_flag'] = flag_price['put_ind'].sum()
                    put_dates = flag_price[flag_price['put_ind'] == 1]['date'].apply(lambda x: x.strftime('%Y-%m-%d')).tolist()
                    flag_stats.loc[(flag_s,flag_date),'put_flag_dates'] = ",".join(put_dates)

                # MA DISTANCES
                count += 1
                print("{0} | Processed | {3} Call_Ind | {4} Put Ind | {1}/{2} ".format(flag.option_symbol, count, len(df_flags), flag_price['call_ind'].sum(), flag_price['put_ind'].sum()))

        # print(flag_stats)


        flag_stats.index.names = ['option_symbol','date']
        # flag_stats.reset_index(inplace=True)
        pm = len(df_flags)
        df_flags = pd.merge(df_flags, flag_stats, on=['option_symbol','date'], how='left')
        am = len(df_flags)
        if (am != pm):
            print("Final Flag - Stats Merge Error | {0} | {1} to {2}".format(flag.option_symbol, pm, am))

        # df_flags.to_csv('flag_analyze.csv')
        print("Finish Analyzing Flags | {0} Tickers | {1} Flags".format(df_flags['symbol'].nunique(), len(df_flags)))
        conn.dispose()
        return df_flags


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
            print("ERROR Extracting Flag Stat: {0} {1} | {2}".format(function, field, e))
            return [None, None]

    def flag_stat_touch(self, df_price, level, ref_field, output_field):
        try:

        # DETERMINE IF CURRENT PRICE IS ABOVE OR BELOW LEVEL

            # CURRENT CLOSE > LEVEL, DETECT WHEN IT DROPS TO LEVEL
            if df_price['close'].iloc[0] >= df_price[level].iloc[0]:
                df_price_filter = df_price[df_price[ref_field] <= df_price[level]]
            elif df_price['close'].iloc[0] < df_price[level].iloc[0]:
                df_price_filter = df_price[df_price[ref_field] >= df_price[level]]

            if len(df_price_filter) > 0:
                return [df_price_filter[output_field].iloc[0], df_price_filter['date'].iloc[0]]
            else:
                return [None, None]

        except Exception as e:
            print("ERROR Calc flag_stat_touch | {0} {1} | {2}".format(df_price['option_symbol'].iloc[0], df_price['date'].iloc[0], level + ref_field + output_field))
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
                return [df_price_filter[output_field].iloc[0], df_price_filter['date'].iloc[0]]
            else:
                return [None, None]
        except Exception as e:
            print("ERROR Calc flag_stat_ITM | {0} {1} | {2}".format(df_price['option_symbol'].iloc[0], df_price['date'].iloc[0], call_put + strike + ref_field + output_field))
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
                return [df_price_filter[output_field].iloc[0], df_price_filter['date'].iloc[0]]
            else:
                return [None, None]
        except Exception as e:
            print("flag_stat_return Error: | {0} {1} | {2}".format(df_price['option_symbol'].iloc[0], df_price['date'].iloc[0], min_ret + ref_field + output_field))
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

                print([df_price_filter[output_field].iloc[0], df_price_filter['date'].iloc[0]])
                return [df_price_filter[output_field].iloc[0], df_price_filter['date'].iloc[0]]
            else:
                return [None, None]
        except Exception as e:
            print("test")
            print("flag_stat_add_flag Error: | {0} {1} | {2}".format(df_price['option_symbol'].iloc[0], df_price['date'].iloc[0], flag + output_field))
            return [None, None]




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
    ticker = ["GME", 'TPX', 'TROX', 'AAPL', 'JAG', 'BBBY', 'QCOM', 'FDC', 'BLL', 'XRT']

    ticker = ["GME",'TPX','TROX','AAPL','JAG','BBBY','QCOM','FDC','BLL','XRT','DPLO','USG','CPB','WWE','FOSL','WIN','ACXM']
    ticker = ['TPX']
    fl = Flag()
    fl.unusual_screen(ticker, days=0)
    # fl.analyze()
