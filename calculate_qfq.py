"""
This script is to 计算前复权的。 前复权计算方法from https://zhuanlan.zhihu.com/p/29386020

下面举一个具体的例子来说明如何计算除权价格以及复权涨跌幅：

易事特(SZ300376)在2015年6月5日的收盘价是89.00元，当天晚上每股分红0.184元，并且每10股转增4股，
那么这个股票除权之后的收盘价应该是(89.00 - 0.184) * 10 / (10 + 4) = 63.44元。
下一个交易日6月8日的收盘价是57.10，真实涨跌幅应该是
      57.10 / 63.44 - 1 =-9.993695%，
而不是57.10 / 89.00 - 1 = -35.842697%。

其中真实涨跌幅57.10 / 63.44 - 1 = -9.993695%也被称为是复权涨跌幅。

Written by mumu-2014 on Dec. 10, 2019 in Shanghai, China.
"""

import numpy as np
import tushare as ts
import pandas as pd
import pymysql

import sys
EPSILON = sys.float_info.epsilon


def getStockFromeSQL( stock_code, start_date, end_date='', sqlTable='stock_all_daily_wfq' ):
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
    if 'adjfactor' in columns:
        data[['open', 'high', 'low', 'close', 'pre_close', 'vol',
              'amount', 'close_chg', 'pct_chg', 'adjfactor' ]] \
            = data[['open', 'high', 'low', 'close', 'pre_close', 'vol',
                    'amount', 'close_chg', 'pct_chg', 'adjfactor' ]].apply(pd.to_numeric)
    else:
        data[ [ 'open', 'high', 'low', 'close', 'pre_close', 'vol',
                'amount', 'close_chg', 'pct_chg' ] ] \
            = data[ [ 'open', 'high', 'low', 'close', 'pre_close', 'vol',
                  'amount', 'close_chg', 'pct_chg' ] ].apply( pd.to_numeric )

    return data


def stcok_qfq_calculation( df, ts_code ):
    """进行前复权"""
    dt = df.copy()
    # --------obtain 股票分红数据---------------
    div = pro.dividend(ts_code=ts_code)
    div = div.loc[div['div_proc'] == '实施']
    div = div[ [ 'ts_code', 'end_date', 'ex_date', 'stk_div', 'cash_div_tax' ] ]
    # pick up the dividend dates
    div = div[ ( div[ 'ex_date' ] > df[ 'trade_date' ].iloc[ 0 ] ) ]
    div = div[ ( div[ 'ex_date' ] < df[ 'trade_date' ].iloc[ -1 ] ) ]
    #
    div.sort_values( by='end_date', inplace=True )
    div.reset_index( drop=True, inplace=True )
    """
         ts_code  end_date   ex_date  stk_div  cash_div_tax
    0  300376.SZ  20131231  20140710      0.0      0.370000
    1  300376.SZ  20140630  20140922      1.0      0.000000
    2  300376.SZ  20141231  20150608      0.4      0.184000
    3  300376.SZ  20151231  20160321      1.0      0.260000
    4  300376.SZ  20161231  20170407      3.0      0.090000
    5  300376.SZ  20171231  20180806      0.0      0.030982
    6  300376.SZ  20181231  20190711      0.0      0.025000
    """
    #--比较end_date-----
    dt[ 'close_qfq' ] = dt[ 'close' ].copy()
    for ctx in range( 0, len( div ) ):
        ex_date = div['ex_date'].iloc[ ctx ]
        stk_div = div['stk_div'].iloc[ ctx ]
        cash_div_tax = div['cash_div_tax'].iloc[ ctx ]
        #data in in this time range
        temp = dt.loc[ dt[ 'trade_date' ] < ex_date ]
        # 除权除息日， 每股送转，每股分红（税前）
        close_qfq = ( temp[ 'close_qfq' ] - cash_div_tax) * 1. / (1 + stk_div)
        dt[ 'close_qfq' ].iloc[ temp.index ] = close_qfq

    return dt[ 'close_qfq' ]


def get_stock_wfq(  ts_code, start_dt, end_dt, mysql_flag=True ):
    # ---------获取未复权的日线行情数据--------
    if mysql_flag:
        df = getStockFromeSQL( ts_code, start_dt, end_date=end_dt, sqlTable='stock_all_daily_wfq')

    else:
        # 获取复权因子
        adjfactor = pro.adj_factor(ts_code=ts_code,
                                   start_date=start_dt, end_date=end_dt)
        # 返回未复权数据和复权因子，自己下来算前复权&后复权:最多4000天记录
        df1 = ts.pro_bar(ts_code=ts_code, asset='E',
                         api=pro, adj=None, freq='D', adjfactor=False,
                         start_date=start_dt, end_date=end_dt)
        #
        adjfactor = adjfactor.drop(['ts_code'], axis=1)
        # merge together
        df = df1.merge(adjfactor, how='inner', on='trade_date')
        #
        df = df.sort_values( by='trade_date', ascending=True )
        df = df.reset_index( drop=True )

    return df


def get_stock_close_qfq( dt, ts_code, my_method=True ):

    #------进行close前复权-------
    if my_method:
        close_qfq = stcok_qfq_calculation( dt, ts_code )
    else:
        adj_factor = dt.drop_duplicates( subset=[ 'adj_factor' ], keep='first' )
        close_qfq = dt[ 'close' ] * dt[ 'adj_factor' ] / adj_factor[ 'adj_factor' ].iloc[ -1 ]

    return close_qfq

#----------compare with tushare 中的qfq数据---------
def qfq_comparson( df, ts_code, start_dt, end_dt ):
    """
    This script is to compare the calcuated qfq close value with the ones directly download from tushare.
    """
    #-------通用行情接口获取前复权数据------
    dt = ts.pro_bar(ts_code=ts_code, asset='E',
                     api=pro, adj='qfq', freq='D', adjfactor=False,
                     start_date=start_dt, end_date=end_dt)
    dt = dt.sort_values( by='trade_date', ascending=True )
    dt = dt.reset_index( drop=True )
    #du = dt.iloc[ (ex_date_index - 5 ) : ( ex_date_index + 5 ) ]

    dx = pd.DataFrame()
    dx[ 'ts_code' ] = [ts_code] * dt.shape[ 0 ]
    dx['trade_date'] = dt['trade_date']
    dx[ 'close' ] = df[ 'close' ]
    dx[ 'ts_qfq' ] = dt[ 'close' ]
    dx[ 'me_qfq' ] = df[ 'close_qfq']
    dx['diff'] = dx['ts_qfq'] - dx['me_qfq']

    return dx


if __name__ == '__main__':
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

    # -----------设置tushare pro的token并获取连接---------------
    token = 'xxxx'
    pro = ts.pro_api(token)

    #==============检验最近日期的前复权================
    start_dt = '20030101'
    end_dt = '20191209'
    ts_code = '300376.SZ'
    div_end_date = '20181231' #最近的分红年度

    #---------获取未复权的日线行情数据--------
    df = get_stock_wfq( ts_code, start_dt, end_dt, mysql_flag=True )


    #---获取close前复权数据------
    close_qfq = get_stock_close_qfq( df[ [ 'trade_date', 'close', 'adj_factor' ] ], ts_code, my_method=True )

    df[ 'close_qfq' ] = close_qfq

    #----------compare with tushare 中的qfq数据---------
    # the maximum differernt value is from the begining date, while the difference in recent dates are very small
    dx = qfq_comparson( df[ [ 'close', 'close_qfq'] ], ts_code, start_dt, end_dt )
    print( 'Initial 10 days: ' )
    print( dx.iloc[ 0 : 10 ])
    print( '=========================' )
    print( 'the most recent dividend dates: ')
    print( dx.iloc[ 1200 : 1210 ])
