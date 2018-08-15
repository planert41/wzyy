# -*- coding: utf-8 -*-
"""
Created on Sun Jun 24 16:27:38 2018

@author: Yaos
"""

import pandas as pd
import datetime as dt
import numpy as np
import os
import logging
import zipfile
import psycopg2
from sqlalchemy import create_engine

from multiprocessing import Pool
import multiprocessing
from multiprocessing import Process



# OPTION DATABASE
# Option Data Creation Files

def createOptionsInit():

    tableName = 'option_data'

    commands = (
        """
        CREATE TABLE {0} (
        date date,
        symbol varchar,
        stock_price numeric(10,2),
        option_symbol varchar,
        option_expiration date,
        strike numeric(10,2),
        call_put char,
        bid numeric(10,2),
        ask numeric(10,2),
        last numeric(10,2),
        volume integer,
        open_interest integer,
        open_interest_new integer,
        open_interest_change integer,
        open_interest_5day integer,
        open_interest_5day_change integer,
        iv numeric(10,4),
        delta numeric(10,4),
        gamma numeric(10,4),
        vega numeric(10,4)
        )
        """.format(tableName))
    try:
        conn = psycopg2.connect("dbname = 'option_data' user='postgres' host = 'localhost' password = 'inkstain'")
        cur = conn.cursor()

        cur.execute("select exists(select * from information_schema.tables where table_name= '{0}')".format(tableName))

        exists = cur.fetchall()[0][0]

        if (not exists):
            cur.execute(commands)
            # cur.execute("alter table {0} add unique (date, option_symbol)".format(tableName))
            # cur.execute("CREATE INDEX IF NOT EXISTS option_data_symbol_index ON option_data(date, symbol);")
            # cur.execute("CREATE INDEX IF NOT EXISTS option_data_option_symbol_index ON option_data(date, option_symbol );")

            cur.close()
            conn.commit()
            print("Create {0} Init".format(tableName))

        else:
            print("Table {0} Already Exists".format(tableName))

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


def createOptionsStatInit():

    tableName = 'option_stat'

    commands = (
        """
        CREATE TABLE {0} (
        date date,            
        symbol varchar,
        stock_price numeric(10,2),

        total_call_vol integer,
        total_put_vol integer,
        total_call_oi integer,
        total_put_oi integer,                 

        wk_call_vol integer,
        wk_put_vol integer,
        wk_call_oi integer,
        wk_put_oi integer,

        fm_call_vol integer,
        fm_put_vol integer,
        fm_call_oi integer,
        fm_put_oi integer,   

        max_opt_1_vol integer,
        max_opt_1_oi integer,
        max_opt_1_call_put char,
        max_opt_1_option varchar,

        max_opt_2_vol integer,
        max_opt_2_oi integer,
        max_opt_2_call_put char,
        max_opt_2_option varchar,           

        max_opt_3_vol integer,
        max_opt_3_oi integer,
        max_opt_3_call_put char,
        max_opt_3_option varchar,         
        
        max_5day_oi_1_change integer,
        max_5day_oi_1_option varchar,
        
        max_5day_oi_2_change integer,
        max_5day_oi_2_option varchar
        

        )
        """.format(tableName))
    try:
        conn = psycopg2.connect("dbname = 'option_data' user='postgres' host = 'localhost' password = 'inkstain'")
        cur = conn.cursor()

        cur.execute("select exists(select * from information_schema.tables where table_name= '{0}')".format(tableName))

        exists = cur.fetchall()[0][0]

        if (not exists):
            cur.execute(commands)
            cur.execute("alter table {0} add unique (date , symbol)".format(tableName))
            cur.execute("CREATE INDEX option_stat_index ON option_stat (date, symbol);")

            cur.close()
            conn.commit()
            print("Create {0} Init".format(tableName))
        else:
            print("Table {0} Already Exists".format(tableName))

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()

# Underlying Info Creation Files

def createProcessLog():
    tableName = 'process_log'

    commands = (
        """
        CREATE TABLE {0} (
        source_date date,
        source_file varchar,
        date timestamp with time zone,  
        record_count integer,
        ticker_count int,
        prev_oi_update timestamp with time zone,
        prev_oi_update_file varchar,
        prev_oi_data_hole_rec_count int,
        prev_oi_data_hole_ticker_count int,
        prev_5day_oi_update timestamp with time zone,
        prev_5day_oi_update_file varchar,
        upload_date timestamp with time zone,  
        stat_date timestamp with time zone,
        flag_date timestamp with time zone,
        process_time numeric(10,2)
        )
        """.format(tableName))

    try:
        conn = psycopg2.connect("dbname = 'option_data' user='postgres' host = 'localhost' password = 'inkstain'")
        cur = conn.cursor()

        cur.execute("select exists(select * from information_schema.tables where table_name= '{0}')".format(tableName))

        exists = cur.fetchall()[0][0]

        if (not exists):
            cur.execute(commands)
            cur.execute("alter table {0} add unique (source_date, date)".format(tableName))


            cur.close()
            conn.commit()
            print("Create {0} Init".format(tableName))
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


# WZYY_OPTION DATABASE


def createETFdataInit():
    tableName = 'etf_data'

    commands = (
        """
        CREATE TABLE {0} (
        etf_name varchar,
        etf_symbol varchar
        )
        """.format(tableName))
    try:
        conn = psycopg2.connect("dbname = 'wzyy_options' user='postgres' host = 'localhost' password = 'inkstain'")
        cur = conn.cursor()

        cur.execute("select exists(select * from information_schema.tables where table_name= '{0}')".format(tableName))
        exists = cur.fetchall()[0][0]

        if (not exists):
            cur.execute(commands)

            cur.close()
            conn.commit()
            print("Create {0} Init".format(tableName))

        else:
            print("Table {0} Already Exists".format(tableName))

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()

def createUnderlyingInit():
    tableName = 'underlying_data'

    commands = (
        """
        CREATE TABLE {0} (
        date date,
        symbol varchar,
        close numeric(10,2),
        high numeric(10,2),
        low numeric(10,2),
        open numeric(10,2),
        volume bigint,
        sma_200 numeric(10,2),
        sma_100 numeric(10,2),
        ema_8 numeric(10,2)
        )
        """.format(tableName))
    try:
        conn = psycopg2.connect("dbname = 'option_data' user='postgres' host = 'localhost' password = 'inkstain'")
        cur = conn.cursor()

        cur.execute("select exists(select * from information_schema.tables where table_name= '{0}')".format(tableName))
        exists = cur.fetchall()[0][0]

        if (not exists):
            cur.execute(commands)
            cur.execute("alter table {0} add unique (symbol , date)".format(tableName))
            cur.execute("CREATE INDEX underlying_data_index ON underlying_data(symbol, date);")


            cur.close()
            conn.commit()
            print("Create {0} Init".format(tableName))

        else:
            print("Table {0} Already Exists".format(tableName))

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


def createOptionFlagInit():
    tableName = 'option_flag'

    stat_col = ['open_high', 'open_high_date', 'open_low', 'open_low_date', 'close_high', 'close_high_date', 'close_low', 'close_low_date']
    stat_col += ['max_high', 'max_high_date', 'min_low', 'min_low_date', 'opt_last_high', 'opt_last_high_date', 'opt_mid_high', 'opt_mid_high_date','opt_expiry_value']
    stat_col += ['opt_mid_drawdown_bmax', 'opt_mid_drawdown_bmax_date', 'max_drawdown_bmax', 'max_drawdown_bmax_date', 'hit_itm_mid', 'hit_itm_date', 'close_itm_mid', 'close_itm_date']
    stat_col += ['close_sma200_mid', 'close_sma200_date', 'close_sma100_mid', 'close_sma100_date', 'close_sma50_mid', 'close_sma50_date', 'close_ema8_mid', 'close_ema8_date']
    stat_col += ['high_52w_mid', 'high_52w_date', 'low_52w_mid', 'low_52w_date', 'high_100d_mid', 'high_100d_date', 'low_100d_mid', 'low_100d_date', 'high_30d_mid', 'high_30d_date', 'low_30d_mid', 'low_30d_date']
    stat_col += ['opt_n50ret_mid', 'opt_n50ret_date', 'opt_p100ret_mid', 'opt_p100ret_date', 'opt_p200ret_mid', 'opt_p200ret_date']
    stat_col += ['stock_p5ret_mid', 'stock_p5ret_date', 'stock_p10ret_mid', 'stock_p10ret_date', 'stock_n5ret_mid', 'stock_n5ret_date', 'stock_n10ret_mid', 'stock_n10ret_date']
    stat_col += ['addflag_call_mid', 'addflag_call_mid_date', 'addflag_put_mid', 'addflag_put_mid_date']
    stat_col += ['oi_n50pct_mid', 'oi_n50pct_mid_date', 'oi_n90pct_mid', 'oi_n90pct_mid_date']





    setup_text = ""

    for stat in stat_col:
        if 'date' in stat:
            setup_text += """{0} date,
            """.format(stat)
        else:
            setup_text += """{0} numeric(10,4),
            """.format(stat)

    commands = (
        """
        CREATE TABLE {0} (
        date date,        
        symbol varchar,
        stock_price numeric(10,2),
        option_symbol varchar,
        option_expiration date,
        strike numeric(10,2),
        call_put char,
        ask numeric(10,2),
        bid numeric(10,2),
        last numeric(10,2),
        volume integer,
        open_interest integer,
        open_interest_new integer,
        open_interest_change integer,
        open_interest_change_vol_pct numeric(10,4),
        open_interest_5day integer,
        open_interest_5day_change integer,
        open_interest_5day_change_pct numeric(10,4),
        iv numeric(10,4),
        delta numeric(10,4),
        gamma numeric(10,4),
        vega numeric(10,4),

        category varchar,

        flag integer,
        
        total_z_flag integer,
        fm_z_flag integer,
        largest_z_flag integer,
        largest_5dayoi_z_flag integer,

        total_z numeric(10,4),
        fm_z numeric(10,4), 
        largest_z numeric(10,4),   
        largest_5dayoi_z numeric(10,4),        

        call_vol_mean_adj numeric(10,4),
        call_vol_std_adj numeric(10,4), 
        put_vol_mean_adj numeric(10,4),    
        put_vol_std_adj numeric(10,4), 

        fm_call_vol_mean_adj numeric(10,4),
        fm_call_vol_std_adj numeric(10,4), 
        fm_put_vol_mean_adj numeric(10,4),    
        fm_put_vol_std_adj numeric(10,4), 

        largest_vol_mean_adj numeric(10,4),
        largest_vol_std_adj numeric(10,4),
        
        largest_5dayoi_mean_adj numeric(10,4),
        largest_5dayoi_vol_adj numeric(10,4),
        
        {1}
        
        call_ind integer,
        put_ind integer,
        
        call_flag integer,
        call_flag_dates varchar[],
        
        put_flag integer,
        put_flag_dates varchar[],
        
        underlying_price_array numeric(10,2)[],
        bid_price_array numeric(10,2)[], 
        ask_price_array numeric(10,2)[],               
        mid_price_array numeric(10,2)[],

        short_date date,
        short_int bigint,
        short_day_cover numeric(10,2),
        short_float numeric(10,2),
        insider_own_pct numeric(10,2),
        institution_own_pct numeric(10,2),
        sector varchar,
        industry varchar,
        etf integer
        
        )
        """.format(tableName,setup_text))

    try:
        conn = psycopg2.connect("dbname = 'wzyy_options' user='postgres' host = 'localhost' password = 'inkstain'")
        cur = conn.cursor()

        cur.execute("select exists(select * from information_schema.tables where table_name= '{0}')".format(tableName))

        exists = cur.fetchall()[0][0]

        if (not exists):
            cur.execute(commands)
            cur.execute("alter table {0} add unique (option_symbol , date)".format(tableName))
            cur.execute("CREATE INDEX option_flag_index ON option_flag(symbol, option_symbol, date);")

            cur.close()
            conn.commit()
            print("Create {0} Init".format(tableName))

        else:
            print("Table {0} Already Exists".format(tableName))

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()




def createProcessLogTicker():
    tableName = 'process_log_ticker'

    commands = (
        """
        CREATE TABLE {0} (
        symbol varchar,
        source_date date,
        source_file varchar,
        upload_date timestamp with time zone,  
        record_count integer,
        prev_oi_update timestamp with time zone,
        prev_oi_update_file varchar,
        prev_5day_oi_update timestamp with time zone,
        prev_5day_oi_update_file varchar,
        stat_date timestamp with time zone,
        flag_date timestamp with time zone,
        process_time numeric(10,2)

        )
        """.format(tableName))

    try:
        conn = psycopg2.connect("dbname = 'option_data' user='postgres' host = 'localhost' password = 'inkstain'")
        cur = conn.cursor()

        cur.execute("select exists(select * from information_schema.tables where table_name= '{0}')".format(tableName))
        exists = cur.fetchall()[0][0]

        if (not exists):
            cur.execute(commands)
            cur.execute("alter table {0} add unique (source_date, symbol)".format(tableName))
            cur.execute("CREATE INDEX process_log_ticker_index ON process_log_ticker(symbol, source_date);")
            cur.close()
            conn.commit()
            print("Create {0} Init".format(tableName))
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()

def refreshTickerLog(date):
    connection_info = create_engine('postgresql://postgres:inkstain@localhost:5432/wzyy_options')
    command = """
        UPDATE
            ticker_log
        SET
            end_date = null
        WHERE
            end_date = '{0}'
    """.format(date)

    connection_info.execute(command)
    connection_info.dispose()

def createTickerLog():
    tableName = 'ticker_log'

    commands = (
        """
        CREATE TABLE {0} (
        symbol varchar,
        start_date date,
        end_date date,
        new_symbol varchar
        )
        """.format(tableName))

    try:
        conn = psycopg2.connect("dbname = 'wzyy_options' user='postgres' host = 'localhost' password = 'inkstain'")
        cur = conn.cursor()

        cur.execute("select exists(select * from information_schema.tables where table_name= '{0}')".format(tableName))

        exists = cur.fetchall()[0][0]

        if (not exists):
            cur.execute(commands)
            cur.execute("alter table {0} add unique (symbol)".format(tableName))

            cur.close()
            conn.commit()
            print("Create {0} Init".format(tableName))
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


def createShortInt():
    tableName = 'short_data'

    commands = (
        """
        CREATE TABLE {0} (
        short_date date,
        symbol varchar,
        name varchar,
        short_int bigint,
        short_day_cover numeric(10,2),
        short_float numeric(10,2),
        short_int_prior bigint,
        short_change_pct numeric(10,2),
        avg_daily_vol bigint,
        shares_float bigint,
        shares_outstanding bigint,
        insider_own_pct numeric(10,2),
        institution_own_pct numeric(10,2),
        price numeric(10,2),
        market_cap  bigint,
        sector varchar,
        industry varchar
        )
        """.format(tableName))

    try:
        conn = psycopg2.connect("dbname = 'wzyy_options' user='postgres' host = 'localhost' password = 'inkstain'")
        cur = conn.cursor()

        cur.execute("select exists(select * from information_schema.tables where table_name= '{0}')".format(tableName))

        exists = cur.fetchall()[0][0]

        if (not exists):
            cur.execute(commands)
            # cur.execute("alter table {0} add unique (short_date, symbol)".format(tableName))

            cur.close()
            conn.commit()
            print("Create {0} Init".format(tableName))
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


# Clearing Code

def deleteOptionsStatDatabase():
    DATA_PATH = 'C:\\Users\\Yaos\\Desktop\\Trading\\IVolData\\'
    today = str(dt.date.today()).replace('-', '')

    conn = psycopg2.connect("dbname = 'wz_options_stat' user='postgres' host = 'localhost' password = 'inkstain'")
    cur = conn.cursor()
    deleteQuery = """    
        select 'drop table if exists "' || tablename || '" cascade;' 
        from pg_tables
        where schemaname = 'public';
    """
    cur.execute(deleteQuery)
    result = cur.fetchall()

    print("Total Rows {0}".format(len(result)))
    for row in result:
        try:
            com = row[0]
            print(com)
            cur.execute(com)
            conn.commit()
        except Exception as e:
            print("Error Running {0}".format(com))

    cur.close()
    conn.close()


def deleteUnderlyingDatabase():
    DATA_PATH = 'C:\\Users\\Yaos\\Desktop\\Trading\\IVolData\\'
    today = str(dt.date.today()).replace('-', '')

    conn = psycopg2.connect("dbname = 'wz_underlying' user='postgres' host = 'localhost' password = 'inkstain'")
    cur = conn.cursor()
    deleteQuery = """    
        select 'drop table if exists "' || tablename || '" cascade;' 
        from pg_tables
        where schemaname = 'public';
    """
    cur.execute(deleteQuery)
    result = cur.fetchall()

    print("Total Rows {0}".format(len(result)))
    for row in result:
        try:
            com = row[0]
            print(com)
            cur.execute(com)
            conn.commit()
        except Exception as e:
            print("Error Running {0}".format(com))

    cur.close()
    conn.close()


def deleteOptionsDatabase():
    DATA_PATH = 'C:\\Users\\Yaos\\Desktop\\Trading\\IVolData\\'
    today = str(dt.date.today()).replace('-', '')

    # DELETE ALL TABLES

    #    conn=psycopg2.connect("dbname = 'wz_options' user='postgres' host = 'localhost' password = 'inkstain'")
    #    cur=conn.cursor()
    #    cur.execute("DROP SCHEMA public CASCADE;CREATE SCHEMA public;")
    #    print("Delete Options Database")
    #    conn.commit()
    #    cur.close()
    #    conn.close()

    conn = psycopg2.connect("dbname = 'wz_options' user='postgres' host = 'localhost' password = 'inkstain'")
    cur = conn.cursor()
    deleteQuery = """    
        select 'drop table if exists "' || tablename || '" cascade;' 
        from pg_tables
        where schemaname = 'public';
    """
    cur.execute(deleteQuery)
    result = cur.fetchall()

    #    print("Fetch All {0}".format(cur.fetchall()))
    #    print("Fetch All Test {0}".format(cur.fetchall()[0][0]))

    #    com = cur.fetchall()[0][0]
    #    try:
    #        print(com)
    #        cur.execute(com)
    #    except Exception as e:
    #        print("Error Running {0}".format(com))

    print("Total Rows {0}".format(len(result)))
    for row in result:
        try:
            com = row[0]
            print(com)
            cur.execute(com)
            conn.commit()
        except Exception as e:
            print("Error Running {0}".format(com))

    cur.close()
    conn.close()

def createAll():
    createOptionsInit()
    createOptionsStatInit()
    createOptionFlagInit()
    createUnderlyingInit()
    createProcessLog()
    createProcessLogTicker()
    createTickerLog()
    createShortInt()
    createETFdataInit()
    print("CREATE ALL - FINISH")

def dropAllTables():
    conn = psycopg2.connect("dbname = 'wzyy_options' user='postgres' host = 'localhost' password = 'inkstain'")
    cur = conn.cursor()
    tableNames = ["option_data", "option_stat", "process_log",'process_log_ticker']
    # tableNames = ["process_log"]
    # KEEPING UNDERLYING DATA

    try:
        for table in tableNames:
            cur.execute("drop table if exists {0}".format(table))
            print("Dropped ", table)

        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        cur.close()
        conn.close()
        print("DROP ALL TABLES - ERROR", e)

    print("DROP ALL TABLES - FINISH")

def clearFilesForDate(date):

    print("Deleting ",date)
    conn = psycopg2.connect("dbname = 'wzyy_options' user='postgres' host = 'localhost' password = 'inkstain'")
    cur = conn.cursor()

    cur.execute("Delete From option_data where date='{0}'".format(date))
    print("option_data | Deleted | ",date)
    cur.execute("Delete From option_stat where date='{0}'".format(date))
    print("option_stat | Deleted | ",date)
    cur.execute("Delete From process_log where source_date='{0}'".format(date))
    print("process_log | Deleted | ",date)


    conn.commit()
    conn.close()
    print("FINISH DELETE FOR ", date)

def clearAll():
    # DELETE ALL TABLES

    conn = psycopg2.connect("dbname = 'wzyy_options' user='postgres' host = 'localhost' password = 'inkstain'")
    cur = conn.cursor()

    tableNames = ["option_data", "option_stat", "process_log"]

    try:
        for table in tableNames:
            cur.execute("select exists(select * from information_schema.tables where table_name= '{0}')".format(table))
            exists = cur.fetchall()[0][0]

            if exists:
                cur.execute("Truncate table {0}".format(table))
                print("Deleted Table {0}".format(table))
            else:
                print("Table {0} Does Not Exist".format(table))

        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        cur.close()
        conn.close()
        print("CLEAR ALL - ERROR", e)

    print("CLEAR ALL - FINISH")

if __name__ == '__main__':
    #
    # dropAllTables()
    # clearAll()
    # clearFilesForDate('2017-01-03')
    # clearFilesForDate('2017-01-04')
    # clearFilesForDate('2017-01-05')
    # clearFilesForDate('2017-01-10')
    # clearFilesForDate('2017-01-11')
    # clearFilesForDate('2018-04-23')
    # clearFilesForDate('2018-05-15')
    # createShortInt()
    # clearFilesForDate('2018-05-17')

    createAll()

    # createOptionFlagInit()


    # clearFilesForDate('2018-07-23')
    # createOptionsInitFile()
    # createFlagInitFile()
    #
    # # clearOptionsInfoFile()
    # #
    # # deleteOptionsStatDatabase()


    # createOptionsStatInitFile()
    # #
    # # deleteOptionsDatabase()
    # createOptionsInitFile()
    # #
    # createFlagInitFile()
    # #
    # # deleteUnderlyingDatabase()
    # createUnderlyingInitFile()

#    select 'drop table if exists "' || tablename || '" cascade;'
#  from pg_tables
# where schemaname = 'public'; -- or any other schema
