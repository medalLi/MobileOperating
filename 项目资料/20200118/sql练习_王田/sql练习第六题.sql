第6题
请用sql写出所有用户中在今年10月份第一次购买商品的金额，表ordertable字段
（购买用户：userid，金额：money，购买时间：paymenttime(格式：2017-10-01)，订单id：orderid）

create table sixth (userid string,monty string ,paymenttime string,orderid string);


insert into table sixth values('001','100','2017-10-01','123123');
insert into table sixth values('001','200','2017-10-02','123124');
insert into table sixth values('002','500','2017-10-01','222222');
insert into table sixth values('001','100','2017-11-01','123123');


选出所有用户十月份的购买记录
然后选出每个人在十月份第一条购买记录的金额


select 
    userid,
    paymenttime,
    monty
from 
(
select 
    paymenttime,
    userid,
    monty,
    orderid,
    row_number() over(partition by userid order by paymenttime) row_con
from sixth
where date_format(paymenttime,'yyyy-MM')='2017-10'
) t1 
where t1.row_con=1;