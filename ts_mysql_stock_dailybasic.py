"""
This script is to download 获取全部股票每日重要的基本面指标 from tushare, and save it in mysql.

1。如果第一次使用这个程序，那么就需要下载所有的数据（这里默认的时间是从'19900101'开始--preprocess_stockQFQ.py中设置），
   那么在"run_stockQFQ.py"中，设置first_update_flag=True

2。如果只是更新当天或者过去几天的数据，则设置first_update_flag=False -- 基础积分每分钟内最多调取200次

Written by mumu-2014 on Oct. 25, 2019 in Shanghai, China.
"""
import tushare as ts
import pymysql
import datetime
import time
import numpy as np

def preprocess_stock_dailybasic(cursor, pro ):
    # check download data in mysql database
    sql_dabase = 'use ts_stock;'
    cursor.execute(sql_dabase)

    # ------- 利润表： 创建表格---------
    sql_comm = "create table if not exists stock_dailybasic " \
               "( id int not null auto_increment primary key,"

    sql_insert = "INSERT INTO stock_dailybasic( "
    sql_value = "VALUES ( "

    df = pro.daily_basic(ts_code='000001.SZ')
    cols = df.columns.tolist()
    str_index = []
    for ctx in range(len(cols)):
        col = cols[ctx]
        if isinstance(df[col].iloc[0], str):
            sql_comm += col + " varchar(40),"
            sql_insert += col + ', '
            sql_value += "'%s', "
            str_index.append(ctx)
        else:
            # elif isinstance( df[col].iloc[0], float  ):
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
    sql_table = "select * from stock_dailybasic where ts_code = '000001.SZ' " \
                "order by trade_date desc limit 0, 1; "
    cursor.execute(sql_table)
    res = cursor.fetchall()

    if len(res) == 0:
        # =====================设定获取日线行情的初始日期和终止日期=======================
        start_dt = '19900101'  # '19910101' --- 下载时候中间改错日期
        time_temp = datetime.datetime.now() - datetime.timedelta(days=1)
        end_dt = time_temp.strftime('%Y%m%d')
        print('start_date: ', start_dt, ', end_date: ', end_dt)
    else:
        last_trade_date = res[0][2]
        #
        start_dt = (datetime.datetime.strptime(last_trade_date, '%Y%m%d')
                    + datetime.timedelta(days=1)).strftime("%Y%m%d")
        #
        time_temp = datetime.datetime.now()
        end_dt = time_temp.strftime('%Y%m%d')
        print('start_date: ', start_dt, ', end_date: ', end_dt)

    return sql_insert, sql_value, start_dt, end_dt

def mysql_stock_dailybasic( db, cursor, pro, itx, stock_pool,
                            start_dt, end_dt, sql_insert, sql_value ):
    # -------------获取上市公司财务利润表数据------------
    df = pro.daily_basic(ts_code=stock_pool[itx], start_date=start_dt, end_date=end_dt )
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

def run_stock_dailybasic( db, pro, first_update_flag=False ):
    # -----查询当前所有正常上市交易的股票列表---------
    data = pro.stock_basic(exchange='', list_status='L' )
    # 设定需要获取数据的股票池
    stock_pool = data['ts_code'].tolist()

    # ----- create an object cursor: 模块主要的作用就是用来和数据库交互的
    cursor = db.cursor()
    # 获得跟数据库互动的参数
    sql_insert, sql_value, start_dt, end_dt = preprocess_stock_dailybasic( cursor, pro )

    if first_update_flag:
        itx = 0
        while itx < len(stock_pool):
            print('itx = ', itx, ', code = ', stock_pool[itx])
            mysql_stock_dailybasic(db, cursor, pro, itx, stock_pool, start_dt, end_dt, sql_insert, sql_value)
            # ======update index=========
            itx += 1
    else:
        itx = 0
        itx_org = itx
        time_start = int(time.time())
        while itx < len( stock_pool ):
            print('itx = ', itx, ', code = ', stock_pool[itx])

            mysql_stock_dailybasic( db, cursor, pro, itx, stock_pool, start_dt, end_dt, sql_insert, sql_value )
            #---
            time_curr = int(time.time())
            if ( ( itx - itx_org ) ) >= 198 or ( ( int(time.time())- time_start ) > 55 ):
                print('Enter sleep......, ', (itx - itx_org), ', time: ', (time_curr - time_start))
                time.sleep( 60 )
                itx_org = itx
                time_start = int( time.time() )
            #======update index=========
            itx += 1

        #========================================
    print('All Finished!')

    #------------
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

    #如何第一次使用这个程序，那么需要下载所有的数据：
    # 这里默认的时间是从'19900101'开始--preprocess_stockQFQ.py中设置
    # first_update_flag=True
    #如何只是更新当天或者过去几天的数据，则设置first_update_flag=False -- 基础积分每分钟内最多调取200次

    run_stock_dailybasic(db, pro, first_update_flag=True)
