第9题
有一个充值日志表如下：
CREATE TABLE `credit log` 
(
    `dist_id` int（11）DEFAULT NULL COMMENT '区组id',
    `account` varchar（100）DEFAULT NULL COMMENT '账号',
    `money` int(11) DEFAULT NULL COMMENT '充值金额',
    `create_time` datetime DEFAULT NULL COMMENT '订单时间'
)ENGINE=InnoDB DEFAUILT CHARSET-utf8
请写出SQL语句，查询充值日志表2015年7月9号每个区组下充值额最大的账号，要求结果：
区组id，账号，金额，充值时间




create table nine_log(
    dist_id int,
    account string,
    money int,
    create_time string
)


insert into table nine_log values (1,'001',100,'2015-07-09');
insert into table nine_log values (1,'002',500,'2015-07-09');
insert into table nine_log values (2,'001',200,'2015-07-09');


select 
    t1.dist_id,
    t1.account,
    t1.money,
    t1.create_time
from 
(
select 
    dist_id,
    account,
    create_time,
    money,
    rank() over(partition by dist_id order by money desc) rank
    from nine_log
where create_time='2015-07-09'
)t1
where rank=1;






