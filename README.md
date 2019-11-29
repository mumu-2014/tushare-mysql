1. 从tushare 网址 （https://tushare.pro） 获取token 

2. 安装mysql (本人使用mac电脑，因此windows 和ubuntu用户请自行解决，推荐使用docker )

https://blog.csdn.net/CatStarXcode/article/details/78940385 

https://www.jianshu.com/p/3e681b2110a1

3. 在mysql中建立一个数据库 ts_stock： 
   create database ts_stock; 
   
4。然后就可以使用我提供的程序了(程序主要是从tushare网址中下载数据，然后存入mysql数据库，最后从mysql数据库中获取数据）：

    1) ts_mysql_stock_all_qfq,下载前复权的日线行情数据 ( 用了通用行情接口ts.pro_bar获取数据 —如果积分不够，可以使用pro.daily)
    
    2）ts_mysql_stock_dailybasic.py获取全部股票每日重要的基本面指标，使用了pro.daily_basic; 
    
    注意：对于以上两个用于下载数据到mysql，请在run_stockQFQ.py和run_stock_dailybasic.py中注意first_update_flag的设置：    a)如果第一次使用这个程序，那么就需要下载所有的数据（这里默认的时间是从'19900101'开始--preprocess_stockQFQ.py中设置），那么在"run_stockQFQ.py"中，设置first_update_flag=True

      b) 如果只是更新当天或者过去几天的数据，则设置first_update_flag=False -- 基础积分每分钟内最多调取200次
    
    3）get_stock_from_sql.py是从mysql中读取数据，然后转换成pandas的dataframe


如果在运行过程中有任何关于code问题，请联系qq:296867865,注明：tushare-mysql。  
