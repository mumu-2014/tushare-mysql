"""
The ts_mysql_stock_index_qfq.py（从tushare下载指数数据到mysql数据库 -- 注意指数是没有复权这个概念的） script is to download 指数日线行情 from https://tushare.pro/document/2?doc_id=95
复权类型(只针对股票)：qfq前复权

Modified by mumu-2014 Wei on Nov.30, 2019 in Shanghai, China.
"""
import tushare as ts
import pymysql
import datetime
import numpy as np


def preprocess_index_QFQ(cursor, pro, ts_symbol='000001', market='SH' ):
    #获取交易的指数代码
    ts_code = ts_symbol + '.' + market
    # check download data in mysql database
    sql_dabase = 'use ts_stock;'
    cursor.execute(sql_dabase)

    # ------- 利润表： 创建表格---------
    sql_comm = "create table if not exists index_%s%s_qfq " \
               "( id int not null auto_increment primary key," % ( market, ts_symbol )

    sql_insert = "INSERT INTO index_%s%s_qfq ( " % ( market, ts_symbol )
    sql_value = "VALUES ( "

    df = ts.pro_bar( ts_code=ts_code, asset='I',
                     api=pro, adj='qfq', start_date='20190102', end_date='20190104' )
    #---改变列名
    df.rename( columns={ 'change': 'close_chg' }, inplace=True )

    cols = df.columns.tolist()
    str_index = []
    for ctx in range( 0, len( cols ) ):
        col = cols[ctx]
        if isinstance(df[col].iloc[0], str):
            sql_comm += col + " varchar(40),"
            sql_insert += col + ', '
            sql_value += "'%s', "
            str_index.append(ctx)

        elif isinstance( df[col].iloc[0], float  ):
            sql_comm += col + " decimal(20, 2), "
            sql_insert += col + ', '
            sql_value += "'%.2f', "
    #
    sql_comm = sql_comm[0: len(sql_comm) - 2]
    sql_comm += ") engine=innodb default charset=utf8mb4;"
    #
    sql_insert = sql_insert[0: len(sql_insert) - 2]
    sql_insert += " )"
    sql_value = sql_value[0: len(sql_value) - 2]
    sql_value += " )"
    #
    cursor.execute(sql_comm)

    # ------------------------
    #
    sql_table = "select trade_date from index_%s%s_qfq where ts_code = %s " \
                "order by trade_date desc limit 0, 1; " % ( market, ts_symbol, ts_symbol )
    cursor.execute(sql_table)
    res = cursor.fetchall()

    if len(res) == 0:
        # =====================设定获取日线行情的初始日期和终止日期=======================
        start_dt = '19900101'  # '19910101' --- 下载时候中间改错日期
        time_temp = datetime.datetime.now() - datetime.timedelta(days=1)
        end_dt = time_temp.strftime('%Y%m%d')
        print('start_date: ', start_dt, ', end_date: ', end_dt)
    else:
        last_trade_date = res[0][0]
        #
        start_dt = (datetime.datetime.strptime(last_trade_date, '%Y%m%d')
                    + datetime.timedelta(days=1)).strftime("%Y%m%d")
        #
        time_temp = datetime.datetime.now()
        end_dt = time_temp.strftime('%Y%m%d')
        print('start_date: ', start_dt, ', end_date: ', end_dt)

    return sql_insert, sql_value, start_dt, end_dt


def mysql_index_QFQ( db, cursor, pro, ts_code,
                  start_dt, end_dt, sql_insert, sql_value ):
    # -------------获取交易的指数数据------------
    df = ts.pro_bar( ts_code=ts_code, asset='I',
                     api=pro, adj='qfq',
                     start_date=start_dt, end_date=end_dt )

    if df is not None:
        #---改变列名
        df.rename( columns={ 'change': 'close_chg' }, inplace=True )

        df.drop_duplicates( inplace=True )
        df = df.sort_values( by=[ 'trade_date' ], ascending=False )
        df.reset_index( inplace=True, drop=True )
        c_len = df.shape[0]

        for jtx in range( 0, c_len ):
            resu0 = list( df.iloc[ c_len - 1 - jtx ] )
            resu = []
            for k in range( len( resu0 ) ):
                if isinstance( resu0[ k ], str ):
                    resu.append( resu0[ k ] )
                elif isinstance( resu0[ k ], float ):
                    if np.isnan( resu0[k] ):
                        resu.append( -1 )
                    else:
                        resu.append( resu0[ k ] )
                elif resu0[ k ] == None:
                    resu.append( -1 )

            #save into mysql database
            try:
                sql_impl = sql_insert + sql_value
                sql_impl = sql_impl  % tuple( resu )
                cursor.execute(sql_impl )
                db.commit()

            except Exception as err:
                print( err )
                continue


def run_index_QFQ( db, pro, index_list=[ '沪深300' ] ):
    for index_name in index_list:
        ts_code = index[ index_name ]
        print( 'index_name: ', index_name, ', ts_code: ', ts_code )

        #split into market & symbol
        ts_symbol, market = ts_code.split( '.' )
        # ----- create an object cursor: 模块主要的作用就是用来和数据库交互的
        cursor = db.cursor()
        # 获得跟数据库互动的参数
        sql_insert, sql_value, start_dt, end_dt \
            = preprocess_index_QFQ( cursor, pro, ts_symbol, market )

        mysql_index_QFQ(db, cursor, pro, ts_code, start_dt, end_dt, sql_insert, sql_value)

    #========================================
    print('Download Finished!')
    cursor.close()
    db.close()


if __name__ == '__main__':
    # ===============建立数据库连接,剔除已入库的部分============================
    # connect database
    config = {
        'host': 'localhost',
        'user': 'root',
        'password': '123456',
        'database': 'ts_stock',
        'charset': 'utf8'
    }
    db = pymysql.connect( **config )

    # -----------设置tushare pro的token并获取连接---------------
    token = 'xxxx'
    pro = ts.pro_api( token )

    #上证综指代码：000001.SH,
    index = {'上证综指': '000001.SH',
             '深证成指': '399001.SZ',
             '沪深300': '000300.SH',
             '创业板指': '399006.SZ',
             '上证50': '000016.SH',
             '中证500': '000905.SH',
             '中小板指': '399005.SZ',
             '上证180': '000010.SH'}

    run_index_QFQ(db, pro, index_list=list(index.keys() ) )
