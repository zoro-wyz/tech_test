import psycopg2

db_params = {
    'dbname': 'technical_test',  
    'user': 'candidate',         
    'password': 'NW337AkNQH76veGc',     
    'host': 'technical-test-1.cncti7m4kr9f.ap-south-1.rds.amazonaws.com',            
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
    cursor.execute("SELECT count(*) AS trade_with_user_count FROM trades LEFT JOIN users ON users.login_hash = trades.login_hash AND users.server_hash = trades.server_hash WHERE users.login_hash is NOT null AND users.server_hash is NOT null")
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

