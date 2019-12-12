1. 从tushare 网址 （https://tushare.pro） 获取token 

2. 安装mysql (本人使用mac电脑，因此windows 和ubuntu用户请自行解决，推荐使用docker )

https://blog.csdn.net/CatStarXcode/article/details/78940385 

https://www.jianshu.com/p/3e681b2110a1

3. 在mysql中建立一个数据库 ts_stock： 
   create database ts_stock; 
   
4。然后就可以使用我提供的程序了(程序主要是从tushare网址中下载数据，然后存入mysql数据库，最后从mysql数据库中获取数据）：

    1) ts_mysql_stock_all_qfq,下载前复权的日线或分钟行情数据 ( 用了通用行情接口ts.pro_bar获取数据 —如果积分不够，可以使用pro.daily)
    
    2）ts_mysql_stock_dailybasic.py获取全部股票每日重要的基本面指标，使用了pro.daily_basic; 
    
    注意：对于以上两个用于下载数据到mysql，请在run_stockQFQ.py和run_stock_dailybasic.py中注意first_update_flag的设置：    
      a)如果第一次使用这个程序，那么就需要下载所有的数据（这里默认的时间是从'19900101'开始--     preprocess_stockQFQ.py中设置），那么在"run_stockQ.py"中，设置first_update_flag=True

      b) 如果只是更新当天或者过去几天的数据，则设置first_update_flag=False -- 基础积分每分钟内最多调取200次
    
    3）get_stock_from_sql.py是从mysql中读取数据，然后转换成pandas的dataframe
    
    4) ts_mysql_stock_index_qfq.py获取常用的几种指数日线行情（前复权）

5. 给mysql中的每张表格直接建立索引,用来加快提取数据的速度（感谢tushare进阶群里某位大佬给的建议）
    create index stock_all_qfq_idx on stock_all_qfq ( stock_code, trade_date);
    
    create index stock_dailybasic_idx on stock_dailybasic (ts_code, trade_date); 
    
    
更新 Dec. 3, 2019: 
    根据tushare进阶群里讨论的意见，每日更新时候可以每次得到最多100只股票，而不是像现在每次只获取一只股票，因此在ts_mysql_stock_all_qfq.py中添加两个程序：mysql_stockQFQ_batch.py & run_stockQFQ_batch.py,这样更新速度至少是加快100倍？（没有验证，等待大家告诉我^_^


#---------------------------------
更新 Dec. 12, 2019  --------未复权数据下载以及前复权close股价计算: 存储数据最好是未复权
1) 基于群友的善意提醒和建议，修复了bug： 在ts_mysql_stock_all_qfq.py中的run_stockQFQ_batch.py， when first_update_flag=False时，mysql_stockQFQ(db, cursor, pro, itx, start_dt, end_dt, sql_insert, sql_value) ---》多加了一个stock_pool这个参数

2）基于群友的善意提醒和建议，添加了一个未复权的下载程序 ts_mysql_stock_all_wfq.py 和一个用于计算前复权的程序：calculate_qfq.py.
因为一只股票在分红后，其股价都会改变，因此如果采用我以前提供的前复权程序下载程序，那么每次分红后都得删除旧数据，重新下载。

3) 在calculate_qfq.py这个程序中，提供了两种方法计算前复权：1）直接利用tushare提供的复权因子adj_factor进行计算； 2） 我自己利用分红直接计算。两者存在着一些误差，以'300376.SZ'股票为例，其在过去存在着下面这些分红

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
            
    因此前复权计算出来的数据我所提到的两种方法存在误差，
    
           “”“
                    ts_code trade_date  close  ts_qfq  me_qfq    diff
            1200  300376.SZ   20190702   5.02  4.9981   4.995  0.0031
            1201  300376.SZ   20190703   4.92  4.8986   4.895  0.0036
            1202  300376.SZ   20190704   4.92  4.8986   4.895  0.0036
            1203  300376.SZ   20190705   4.91  4.8886   4.885  0.0036
            1204  300376.SZ   20190708   4.67  4.6497   4.645  0.0047
            1205  300376.SZ   20190709   4.64  4.6198   4.615  0.0048
            1206  300376.SZ   20190710   4.60  4.5800   4.575  0.0050
            1207  300376.SZ   20190711   4.60  4.6000   4.600  0.0000
            1208  300376.SZ   20190712   4.60  4.6000   4.600  0.0000
            1209  300376.SZ   20190715   4.62  4.6200   4.620  0.0000
            ”“”
            
                 ts_code trade_date  close  ts_qfq    me_qfq      diff
            0  300376.SZ   20140127  26.50  1.1544  1.039108  0.115292
            1  300376.SZ   20140128  29.15  1.2698  1.157411  0.112389
            2  300376.SZ   20140129  32.07  1.3970  1.287768  0.109232
            3  300376.SZ   20140130  35.28  1.5369  1.431072  0.105828
            4  300376.SZ   20140210  38.81  1.6906  1.588661  0.101939
            5  300376.SZ   20140211  42.69  1.8596  1.761875  0.097725
            6  300376.SZ   20140212  46.96  2.0457  1.952500  0.093200
            7  300376.SZ   20140217  51.66  2.2504  2.162322  0.088078
            8  300376.SZ   20140218  56.83  2.4756  2.393125  0.082475
            9  300376.SZ   20140219  62.51  2.7230  2.646697  0.076303



如果在运行过程中有任何关于code问题，请联系qq:296867865,注明：tushare-mysql。  
