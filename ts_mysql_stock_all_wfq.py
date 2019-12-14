"""
The ts_mysql_stock_all_wfq.py (从tushare下载所有的股票未复权股票数据到mysql数据库）script is to download data from https://tushare.pro/document/2?doc_id=25
复权类型(只针对股票)：wfq未复权

Note：由于权限问题，最多拿到4000条历史记录

Written by mumu-2014 on Dec.3, 2019 in Shanghai, China.
Modified by mumu-2014 on Dec. 14, 2019 in Shanghai, China.
"""
import tushare as ts
import pymysql
import datetime
import time
import numpy as np
import pandas as pd


def preprocess_stock_WFQ(cursor, pro, sqlTable='stock_all_wfq' ):
    # check download data in mysql database
    sql_dabase = 'use ts_stock;'
    cursor.execute(sql_dabase)

    # ------- 创建表格---------
    sql_comm = "create table if not exists %s " \
               "( id int not null auto_increment primary key," % ( sqlTable )

    sql_insert = "INSERT INTO %s ( " % ( sqlTable )
    sql_value = "VALUES ( "

    df = ts.pro_bar( ts_code='000001.SZ', asset='E',
                     api=pro, adj=None, freq='D', adjfactor=False,
                     start_date='20190102', end_date='20190104' )
    #获取复权因子
    adjfactor = pro.adj_factor( ts_code='000001.SZ',
                                start_date='20190102', end_date='20190104' )
    #merge together
    df[ 'adj_factor' ] = adjfactor[ 'adj_factor' ]

    #---改变列名
    df.rename( columns={ 'change': 'close_chg' }, inplace=True )

    cols = df.columns.tolist()
    str_index = []
    for ctx in range( 0, len( cols ) ):
        col = cols[ctx]
        if isinstance(df[col].iloc[0], str):
            sql_comm += col + " varchar(40), "
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
    sql_table = "select trade_date from %s where ts_code = '000001.SZ' " \
                "order by trade_date desc limit 0, 1; " % ( sqlTable )
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
        time_temp = datetime.datetime.now() #- datetime.timedelta(days=1)
        end_dt = time_temp.strftime('%Y%m%d')
        print('start_date: ', start_dt, ', end_date: ', end_dt)

    return sql_insert, sql_value, start_dt, end_dt


def mysql_stock_WFQ( db, cursor, pro, itx, stock_pool,
                  start_dt, end_dt, sql_insert, sql_value ):
    # -------------获取获取股票行情数据------------
    #获取复权因子
    adjfactor = pro.adj_factor( ts_code=stock_pool[ itx ],
                                start_date=start_dt, end_date=end_dt )

    #返回未复权数据和复权因子，自己下来算前复权&后复权:最多4000天记录
    df1 = ts.pro_bar( ts_code=stock_pool[ itx ], asset='E',
                     api=pro, adj=None, freq='D', adjfactor=False,
                     start_date=start_dt, end_date=end_dt )
    #two options: 1)继续下载以前的历史数据--》不过这些都没有多大价值; 2）只保存4000记录
    #-------modified by mumu-2014 on Dec. 14, 2019 in Shanghai, China----
    if len( df1 ) == 4000: #最多下载4000条记录
        last_download_date = df1[ 'trade_date' ].iloc[ -1 ]
        #
        last_download_date = (datetime.datetime.strptime( last_download_date, '%Y%m%d')
                              - datetime.timedelta(days=1)).strftime("%Y%m%d")

        df2 = ts.pro_bar( ts_code=stock_pool[ itx ], asset='E',
                          api=pro, adj=None, freq='D', adjfactor=False,
                          start_date=start_dt, end_date=last_download_date )

        if len(df2 ) > 0:
            df1 = pd.concat( [ df1, df2 ], axis=0 )
    #---合并---
    adjfactor = adjfactor.drop( [ 'ts_code' ], axis=1 )
    #merge together
    df = df1.merge(  adjfactor, how='inner', on='trade_date' )

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


def run_stock_WFQ( db, pro, first_update_flag=False ):
    # -----查询当前所有正常上市交易的股票列表---------
    data = pro.stock_basic(exchange='', list_status='L' )
    # 设定需要获取数据的股票池
    stock_pool = data['ts_code'].tolist()
    #print( stock_pool.index( '002427.SZ') )

    # ----- create an object cursor: 模块主要的作用就是用来和数据库交互的
    cursor = db.cursor()
    # 获得跟数据库互动的参数
    sql_insert, sql_value, start_dt, end_dt \
        = preprocess_stock_WFQ( cursor, pro, sqlTable='stock_all_daily_wfq' )


    if first_update_flag:
        itx = 0
        while itx < len(stock_pool):
            print('itx = ', itx, ', code = ', stock_pool[itx])

            mysql_stock_WFQ(db, cursor, pro, itx, stock_pool,
                            start_dt, end_dt, sql_insert, sql_value)
            # ======update index=========
            itx += 1
    else:
        itx = 0
        itx_org = itx
        time_start = int(time.time())
        while itx < len( stock_pool ):
            print('itx = ', itx, ', code = ', stock_pool[itx])

            mysql_stock_WFQ(db, cursor, pro, itx, stock_pool, start_dt, end_dt, sql_insert, sql_value)
            #---
            time_curr = int(time.time())
            if ( ( itx - itx_org ) ) >= 198 or ( ( int( time.time() ) - time_start ) > 55 ):
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

    #===================================================
    #----获取复权因子----
    #----需要存为未复权，然后自己利用公式计算前复权&后复权
    time_start = int(time.time())

    run_stock_WFQ(db, pro, first_update_flag=False )

    time_end = int(time.time())
    print( 'spend time: ', time_end - time_start )
