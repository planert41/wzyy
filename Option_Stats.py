# -*- coding: utf-8 -*-
"""
Created on Sat Jun 30 16:52:05 2018

@author: Yaos
"""


from pandas import DataFrame
from sqlalchemy import create_engine
import logging


DATA_PATH = '../Data/'

logger = logging.getLogger(__name__)
if not len(logger.handlers):
    formatter = logging.Formatter('%(asctime)s %(filename)s: %(funcName)s: %(message)s')
    logger.setLevel(logging.DEBUG)
    fh = logging.FileHandler('OptionMetricP' + '.log', mode='w')
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(formatter)
    logger.addHandler(fh)


class OptionStats():

    def __init__(self, symbol, date, data):

        self.date = date
        self.symbol = symbol
        self.stock_price = data['stock_price'].iloc[0]

        self.total_call_vol = None
        self.total_put_vol = None
        self.total_call_oi = None
        self.total_put_oi = None

        self.max_call_vol = None
        self.max_put_vol = None
        self.max_call_oi = None
        self.max_put_oi = None

        self.weekly_call_vol = None
        self.weekly_put_vol = None
        self.weekly_call_oi = None
        self.weekly_put_oi = None

        self.fm_call_vol = None
        self.fm_put_vol = None
        self.fm_call_oi = None
        self.fm_put_oi = None

        self.bm_call_vol = None
        self.bm_put_vol = None
        self.bm_call_oi = None
        self.bm_put_oi = None

        self.data = data
        self.result = self.summarize()

    def summarize(self):

        data_cols = ['option_symbol','option_expiration','call_put', 'volume', 'open_interest','open_interest_change','open_interest_5day_change']

        df = self.data[data_cols]
        cur_date = self.date

        df['category'] = df['option_expiration'].apply(lambda x: self.option_category(x, cur_date))

        fields = ['category', 'call_put', 'volume', 'open_interest']
        df_total = df.groupby(['call_put'])[fields].sum().reset_index()
        #        print("DF_TOTAL: ",df_total[df_total['call_put']=='C']['volume'].sum())

        df_category = df.groupby(['category', 'call_put'])[fields].sum().reset_index()
        #        print("DF_CATEGORY: ",df_category.head())

        result = DataFrame(columns=[], index=[cur_date])

        result['date'] = self.date
        result['symbol'] = self.symbol
        result['stock_price'] = self.stock_price

        result['total_call_vol'] = df_total[df_total['call_put'] == 'C']['volume'].sum()
        result['total_put_vol'] = df_total[df_total['call_put'] == 'P']['volume'].sum()
        result['total_call_oi'] = df_total[df_total['call_put'] == 'C']['open_interest'].sum()
        result['total_put_oi'] = df_total[df_total['call_put'] == 'P']['open_interest'].sum()

        result['wk_call_vol'] = df_category[((df_category["category"] == 'WK') & (df_category['call_put'] == 'C'))]['volume'].sum()
        result['wk_put_vol'] = df_category[((df_category["category"] == 'WK') & (df_category['call_put'] == 'P'))]['volume'].sum()
        result['wk_call_oi'] = df_category[((df_category["category"] == 'WK') & (df_category['call_put'] == 'C'))]['open_interest'].sum()
        result['wk_put_oi'] = df_category[((df_category["category"] == 'WK') & (df_category['call_put'] == 'P'))]['open_interest'].sum()

        result['fm_call_vol'] = df_category[((df_category["category"] == 'FM') & (df_category['call_put'] == 'C'))]['volume'].sum()
        result['fm_put_vol'] = df_category[((df_category["category"] == 'FM') & (df_category['call_put'] == 'P'))]['volume'].sum()
        result['fm_call_oi'] = df_category[((df_category["category"] == 'FM') & (df_category['call_put'] == 'C'))]['open_interest'].sum()
        result['fm_put_oi'] = df_category[((df_category["category"] == 'FM') & (df_category['call_put'] == 'P'))]['open_interest'].sum()


        df_max = sorted(zip(df['volume'], df['open_interest'], df['call_put'], df['option_symbol']), reverse=True)[:3]

        # print("DF_MAX: ",df_max)
        if len(df_max)>0:
            result['max_opt_1_vol'] = df_max[0][0]
            result['max_opt_1_oi'] = df_max[0][1]
            result['max_opt_1_call_put'] = df_max[0][2]
            result['max_opt_1_option'] = df_max[0][3]
        if len(df_max)>1:
            result['max_opt_2_vol'] = df_max[1][0]
            result['max_opt_2_oi'] = df_max[1][1]
            result['max_opt_2_call_put'] = df_max[1][2]
            result['max_opt_2_option'] = df_max[1][3]

        if len(df_max)>2:
            result['max_opt_3_vol'] = df_max[2][0]
            result['max_opt_3_oi'] = df_max[2][1]
            result['max_opt_3_call_put'] = df_max[2][2]
            result['max_opt_3_option'] = df_max[2][3]
        if 'open_interest_5day_change' in df.columns:
            df_max_5day = sorted(zip(df['open_interest_5day_change'], df['option_symbol']), reverse=True)[:3]
            if len(df_max_5day) > 0:
                result['max_5day_oi_1_change'] = df_max_5day[0][0]
                result['max_5day_oi_1_option'] = df_max_5day[0][1]
            if len(df_max_5day) > 1:
                result['max_5day_oi_2_change'] = df_max_5day[1][0]
                result['max_5day_oi_2_option'] = df_max_5day[1][1]

        return result

    #        result.to_csv('Results.csv')
    #        print("Result: ",result)

    def option_category(self, df, data_date):

        if 14 < df.day < 22:
            if abs((df - data_date).days) <= 70:
                return "FM"
            else:
                return "BM"
        else:
            return "WK"

    def ema(self, data, window):
        if len(data) < 2 * window:
            raise ValueError("data is too short")
        c = 2.0 / (window + 1)
        current_ema = self.sma(data[-window * 2:-window], window)
        for value in data[-window:]:
            current_ema = (c * value) + ((1 - c) * current_ema)
        return current_ema

# if __name__ == '__main__':
#
#     engine = create_engine('postgresql://postgres:inkstain@localhost:5432/wz_options')
#     df = pd.read_sql_query('select * from "gme_src"',con=engine)
#     engine.dispose()
#
#     ticker = df['symbol'].iloc[0].lower()
#     dataDate = df['date'].iloc[0]
#
#     chunks = [chunk[1] for chunk in df.groupby('date')]
#     dates = [chunk[0] for chunk in df.groupby('date')]
#
#     for chunk in df.groupby('date'):
#         print("Stats For {0} {1}".format(chunk[0], ticker))
#         stats = OptionStats(ticker, chunk[0], chunk[1])
#
#         statsTableName = ticker + "_stat"
#
#         connection= create_engine('postgresql://postgres:inkstain@localhost:5432/wz_options_stat')
#         stats.result.to_sql(statsTableName, connection, if_exists='append', index_label='date')
#         print("Adding {0} {1} to {2}".format(ticker,dataDate,statsTableName))
#         connection.dispose()

#        print(stats)

