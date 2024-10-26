# tech_test

tech_test_qa_wangyizun.python
1. qa脚本连接数据库的信息具有隐私性，已经从脚本内容中移除，运行前需要补充。连接信息在笔试题文档中。
2. 具有意外数值问题，发现 open_price 的值有效小数位没有遵循 digits 的规范。脚本中展示了部分样例。
3. 具有数据重复问题，users 表中共有1000条数据，去重后有666条数据。trades 表去重前后数据都为100000条。
4. 具有数据不完整问题，trades 表与 users 表进行连接时，数据为8047条，有部分 trade 数据没有对应 user。
5. 具有边缘问题，open_time 和 close_time 完全一致的数据有36条，需要跟业务部门确认该情况是否正常。
