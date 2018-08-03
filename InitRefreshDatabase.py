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
        conn = psycopg2.connect("dbname = 'wzyy_options' user='postgres' host = 'localhost' password = 'inkstain'")
        cur = conn.cursor()

        cur.execute("select exists(select * from information_schema.tables where table_name= '{0}')".format(tableName))

        exists = cur.fetchall()[0][0]

        if (not exists):
            cur.execute(commands)
            cur.execute("alter table {0} add unique (option_symbol , date)".format(tableName))
            cur.execute("CREATE INDEX option_data_index ON option_data(symbol, option_symbol, date);")

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
        conn = psycopg2.connect("dbname = 'wzyy_options' user='postgres' host = 'localhost' password = 'inkstain'")
        cur = conn.cursor()

        cur.execute("select exists(select * from information_schema.tables where table_name= '{0}')".format(tableName))

        exists = cur.fetchall()[0][0]

        if (not exists):
            cur.execute(commands)
            cur.execute("alter table {0} add unique (date , symbol)".format(tableName))
            cur.execute("CREATE INDEX option_stat_index ON option_stat (symbol, date);")

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
        conn = psycopg2.connect("dbname = 'wzyy_options' user='postgres' host = 'localhost' password = 'inkstain'")
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
        largest_5dayoi_vol_adj numeric(10,4)
        
        
        )
        """.format(tableName))

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
        ticker_count int
        )
        """.format(tableName))

    try:
        conn = psycopg2.connect("dbname = 'wzyy_options' user='postgres' host = 'localhost' password = 'inkstain'")
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
        flag_date timestamp with time zone
        )
        """.format(tableName))

    try:
        conn = psycopg2.connect("dbname = 'wzyy_options' user='postgres' host = 'localhost' password = 'inkstain'")
        cur = conn.cursor()

        cur.execute("select exists(select * from information_schema.tables where table_name= '{0}')".format(tableName))
        cur.execute("CREATE INDEX process_log_ticker_index ON process_log_ticker(symbol, source_date);")

        exists = cur.fetchall()[0][0]

        if (not exists):
            cur.execute(commands)
            cur.execute("alter table {0} add unique (source_date, symbol)".format(tableName))

            cur.close()
            conn.commit()
            print("Create {0} Init".format(tableName))
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()

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
    print("CREATE ALL - FINISH")

def dropAllTables():
    conn = psycopg2.connect("dbname = 'wzyy_options' user='postgres' host = 'localhost' password = 'inkstain'")
    cur = conn.cursor()
    tableNames = ["option_data", "option_stat", "option_flag", "process_log",'process_log_ticker']
    # KEEPING UNDERLYING DATA

    try:
        for table in tableNames:
            cur.execute("drop table if exists {0}".format(table))

        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        cur.close()
        conn.close()
        print("DROP ALL TABLES - ERROR", e)

    print("DROP ALL TABLES - FINISH")

def clearFilesForDate(date):

    conn = psycopg2.connect("dbname = 'wzyy_options' user='postgres' host = 'localhost' password = 'inkstain'")
    cur = conn.cursor()

    cur.execute("Delete From option_data where date='{0}'".format(date))
    cur.execute("Delete From option_stat where date='{0}'".format(date))
    cur.execute("Delete From process_log where source_date='{0}'".format(date))

    conn.commit()
    conn.close()
    print("FINISH DELETE FOR ", date)

def clearAll():
    # DELETE ALL TABLES

    conn = psycopg2.connect("dbname = 'wzyy_options' user='postgres' host = 'localhost' password = 'inkstain'")
    cur = conn.cursor()

    tableNames = ["option_data", "option_stat", "option_flag", "process_log"]

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
    # # clearAll()
    # createAll()

    createOptionFlagInit()


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
