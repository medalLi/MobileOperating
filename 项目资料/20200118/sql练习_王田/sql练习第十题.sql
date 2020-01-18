第10题
有一个账号表如下，请写出SQL语句，查询各自区组的money排名前十的账号（分组取前10）
CREATE TABIE `account` 
(
    `dist_id` int（11）DEFAULT NULL COMMENT '区组id'，
    `account` varchar（100）DEFAULT NULL COMMENT '账号' ,
    `gold` int（11）DEFAULT NULL COMMENT '金币' 
    PRIMARY KEY （`dist_id`，`account_id`），
）ENGINE=InnoDB DEFAULT CHARSET-utf8





create table ten_log(
    dist_id int,
    account string,
    money int
)


insert into table ten_log values (1,'001',100);
insert into table ten_log values (1,'002',500);
insert into table ten_log values (2,'001',200);

--------------------mysql写法------------------
不会






-----------------hive的写法---------------------
select
    t1.dist_id,
    t1.account,
    t1.money
from 
(

select 
dist_id,
account,
money,
rank() over (partition by dist_id order by money desc ) rank
from ten_log
)t1
where t1.rank <=10;