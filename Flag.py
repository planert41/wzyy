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

    def unusual_screen(self, tickers, date=None, days=1):


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

        for ticker_inp in np.nditer(tickers):

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


if __name__ == '__main__':
    ticker = ["GME", 'TPX', 'TROX', 'AAPL', 'JAG', 'BBBY', 'QCOM', 'FDC', 'BLL', 'XRT']
    ticker = ['TPX']

    fl = Flag()
    fl.unusual_screen(ticker, days=0)
