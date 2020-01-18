第7题
现有图书管理数据库的三个数据模型如下：
图书（数据表名：BOOK）
序号  字段名称    字段描述    字段类型
1   BOOK_ID 总编号 文本
2   SORT    分类号 文本
3   BOOK_NAME   书名  文本
4   WRITER  作者  文本
5   OUTPUT  出版单位    文本
6   PRICE   单价  数值（保留小数点后2位）
读者（数据表名：READER）
序号  字段名称    字段描述    字段类型
1   READER_ID   借书证号    文本
2   COMPANY 单位  文本
3   NAME    姓名  文本
4   SEX 性别  文本
5   GRADE   职称  文本
6   ADDR    地址  文本
借阅记录（数据表名：BORROW LOG）
序号  字段名称    字段描述    字段类型
1   READER_ID   借书证号    文本
2   BOOK_ID  总编号 文本
3   BORROW_DATE  借书日期    日期
（1）创建图书管理库的图书、读者和借阅三个基本表的表结构。请写出建表语句。
（2）找出姓李的读者姓名（NAME）和所在单位（COMPANY）。
（3）查找“高等教育出版社”的所有图书名称（BOOK_NAME）及单价（PRICE），结果按单价降序排序。
（4）查找价格介于10元和20元之间的图书种类(SORT）出版单位（OUTPUT）和单价（PRICE），结果按出版单位（OUTPUT）和单价（PRICE）升序排序。
（5）查找所有借了书的读者的姓名（NAME）及所在单位（COMPANY）。
（6）求”科学出版社”图书的最高单价、最低单价、平均单价。
（7）找出当前至少借阅了2本图书（大于等于2本）的读者姓名及其所在单位。
（8）考虑到数据安全的需要，需定时将“借阅记录”中数据进行备份，请使用一条SQL语句，在备份用户bak下创建与“借阅记录”表结构完全一致的数据表BORROW_LOG_BAK.井且将“借阅记录”中现有数据全部复制到BORROW_L0G_ BAK中。
（9）现在需要将原Oracle数据库中数据迁移至Hive仓库，请写出“图书”在Hive中的建表语句（Hive实现，提示：列分隔符|；数据表数据需要外部导入：分区分别以month＿part、day＿part 命名）
（10）Hive中有表A，现在需要将表A的月分区　201505　中　user＿id为20000的user＿dinner字段更新为bonc8920，其他用户user＿dinner字段数据不变，请列出更新的方法步骤。（Hive实现，提示：Hlive中无update语法，请通过其他办法进行数据更新）




1.创建图书表book
    create  table   book(book_id string,sort string,book_name string,writer string,output string,price decimal(10,2));
创建读者表reader
    create table reader (reader_id string,company string,name string,sex string ,grade string,addr string);

        insert into table reader values ('001','sgg','lisi','man','1','beijing');
        insert into table reader values ('002','tencent','wt','man','2','shanghai');
创建借阅记录表borrow_log
    create table borrow_log(reader_id string,book_id string ,borrow_date string);

2.
    select 
        name ,
        company
    from 
    reader 
    where name like 'li%';
3.  select
        book_name,
        price
    from book
    where  output='高等教育出版社'
    order by price desc;
4.  select 
        sort,
        output,
        price
    from book
    where   price <=20 and price >=10 
    order by output,price ;
5.  select 
        t1.name,
        t1.company
    from 
    (
    select 
        r.name name,
        r.company company
    from borrow_log b
    join reader r on
    b.reader_id=r.reader_id
    ) t1
    group by t1.name,t1.company;

6.  select 
        max(price),
        min(price),
        avg(price)
    from book 
    where output ='科学出版社';
7.  select 
        t1.name,
        t1.company
    from 
    (
    select 
        r.name name,
        r.company company
    from borrow_log b
    join reader r on
    b.reader_id=r.reader_id
    ) t1
    group by t1.name,t1.company having count(*)>=2;
8.  create table if not exists borrow_log_bak 
    select * from borrow_log;

9.  create table book_hive(book_id string,sort string,book_name string,writer string,output string,price decimal(10,2)) 
    partitioned by (month_part string,day_part string)
    row format delimited fields terminated by '\\|'
    stored as textfile;

10. hive在1.1.0版本之前不可以更新数据，在之后可以更改
    同样在建表后面添加: stored as orc TBLPROPERTIES('transactional'='true')
    但update操作非常慢













