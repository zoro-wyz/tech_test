import psycopg2

# 1. qa脚本连接数据库的信息具有隐私性，已经从脚本内容中移除，运行前需要补充。连接信息在笔试题文档中。
# 2. 具有意外数值问题，发现 open_price 的值有效小数位没有遵循 digits 的规范。脚本中展示了部分样例。
# 3. 具有数据重复问题，users 表中共有1000条数据，去重后有666条数据。trades 表去重前后数据都为100000条。
# 4. 具有数据不完整问题，trades 表与 users 表进行连接时，数据为8047条，有部分 trade 数据没有对应 user。
# 5. 具有边缘问题，open_time 和 close_time 完全一致的数据有36条，需要跟业务部门确认该情况是否正常。
db_params = {
    'dbname': '',  
    'user': '',         
    'password': '',     
    'host': '',            
    'port': '5432'                   
}

def count_decimal_places(number):
    number_str = str(number)
    if '.' in number_str:
        integer_part, decimal_part = number_str.split('.')
        return len(decimal_part)
    else:
        return 0
    
try:
    connection = psycopg2.connect(**db_params)
    cursor = connection.cursor()
    print("Database connection successed.")

    # 检查意外数值
    cursor.execute("SELECT * FROM trades limit 10000")
    trades = cursor.fetchall()
    print("checking for unexpected data:")
    for trade in trades:
        login_hash , ticket_hash, server_hash, symbol, digits, cmd, volume, open_time, open_price, close_time, contractsize = trade
        decimal_places = count_decimal_places(open_price)
        if(digits < decimal_places-1 ):
            print(f"login_hash: {login_hash}, ticket_hash： {ticket_hash}，server_hash： {server_hash}， digits: {digits}, open_price:{open_price}")

    # 检查数据重复
    cursor.execute("SELECT (SELECT count(*) FROM users) AS user_count,(SELECT count(*) FROM (SELECT DISTINCT login_hash, server_hash, country_hash, currency, enable FROM users) u) AS distinct_user_count,(SELECT count(*) FROM trades) AS trade_count,(SELECT count(*) FROM (SELECT DISTINCT login_hash, ticket_hash, server_hash, symbol, digits, cmd, volume, open_time, open_price, close_time, contractsize FROM trades) t) AS distinct_trade_count")
    results = cursor.fetchall()
    for result in results:
        user_count, distinct_user_count,trade_count, distinct_trade_count = result
        print("checking for duplicate data:")
        print(f"user_count: {user_count}, distinct_user_count: {distinct_user_count}, trade_count: {trade_count}, distinct_trade_count:{distinct_trade_count}")

    # 检查数据完整性
    cursor.execute("SELECT COUNT(*) AS trade_with_user_count FROM trades t LEFT JOIN(SELECT DISTINCT login_hash, server_hash, country_hash, currency, enable FROM users) uu ON t.login_hash = uu.login_hash AND t.server_hash = uu.server_hash WHERE uu.login_hash IS NOT NULL AND uu.server_hash IS NOT NULL")
    results = cursor.fetchall()
    for result in results:
        trade_with_user_count = result
        print("checking for data integrity:")
        print(f"trade_with_user_count:{trade_with_user_count}")

    # 检查边缘情况
    cursor.execute("SELECT count(*) AS edge_count FROM public.trades WHERE open_time = close_time")
    results = cursor.fetchall()
    for result in results:
        edge_count = result
        print("checking for edge cases:")
        print(f"edge_count:{edge_count}")

except Exception as e:
    print(f"An error occurred: {e}")

finally:
    if cursor:
        cursor.close()
    if connection:
        connection.close()
        print("Database connection closed.")

