"""
This script is to obtain stock daily information from mysql database.

Written by mumu-2014 on Oct. 25, 2019 in Shanghai, China.
"""
import os
import tushare as ts
import pandas as pd
import pymysql
import datetime
import numpy as np
from collections import Counter

import sys
MAX_INT = sys.maxsize

import sys
EPSILON = sys.float_info.epsilon

# ======================建立数据库连接,剔除已入库的部分==============================
# connect database
config = {
    'host': 'localhost',
    'user': 'root',
    'password': '123456',
    'database': 'ts_stock',
    'charset': 'utf8'
}
db = pymysql.connect(**config)
# ------create an object cursor: 模块主要的作用就是用来和数据库交互的--------------
cursor = db.cursor()


def getStockFromeSQL( stock_code, start_date, end_date='', sqlTable='stock_all_qfq'):
    #
    sql_table = "select * from %s where ts_code = '%s' and trade_date >= '%s' and trade_date <= '%s'" \
                %( sqlTable, stock_code, start_date, end_date )
    cursor.execute(sql_table)
    res = cursor.fetchall()

    #obtain columns names: tuple structure
    column_resul = cursor.description

    columns = [ ]
    for i in range( len( column_resul ) ):
        columns.append( column_resul[ i ][ 0 ] )

    #
    data = pd.DataFrame( list( res ), columns=columns )
    data.drop( columns=[ 'id' ], inplace=True )
    #convert str into float
    data[ [ 'open', 'high', 'low', 'close', 'pre_close', 'vol',
            'amount', 'close_chg', 'pct_chg' ] ] \
        = data[ [ 'open', 'high', 'low', 'close', 'pre_close', 'vol',
              'amount', 'close_chg', 'pct_chg' ] ].apply( pd.to_numeric )

    return data


if __name__ == '__main__':

    start = '20190101'
    end = '20191128'
    stock_code = '000001.SZ'
    ##获取行情数据
    df = getStockFromeSQL( stock_code, start, end_date=end, sqlTable='stock_all_qfq')

