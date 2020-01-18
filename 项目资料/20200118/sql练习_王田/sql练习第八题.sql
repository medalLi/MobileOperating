第8题
有一个线上服务器访问日志格式如下（用sql答题）
时间                          接口                           ip地址
2016-11-09 11:22:05    /api/user/login                  110.23.5.33
2016-11-09 11:23:10    /api/user/detail                 57.3.2.16
2016-11-09 23:59:40    /api/user/login                  200.6.5.166
求11月9号下午14点（14-15点），访问api/user/login接口的top10的ip地址


create table eight_log(`date` string,interface string ,ip string);

insert into table eight_log values ('2016-11-09 11:22:05','/api/user/login','110.23.5.23');
insert into table eight_log values ('2016-11-09 11:23:10','/api/user/detail','57.3.2.16');
insert into table eight_log values ('2016-11-09 23:59:40','/api/user/login','200.6.5.166');

insert into table eight_log values('2016-11-09 11:14:23','/api/user/login','136.79.47.70');
insert into table eight_log values('2016-11-09 11:15:23','/api/user/detail','94.144.143.141');
insert into table eight_log values('2016-11-09 11:16:23','/api/user/login','197.161.8.206');
insert into table eight_log values('2016-11-09 12:14:23','/api/user/detail','240.227.107.145');
insert into table eight_log values('2016-11-09 13:14:23','/api/user/login','79.130.122.205');
insert into table eight_log values('2016-11-09 14:14:23','/api/user/detail','65.228.251.189');
insert into table eight_log values('2016-11-09 14:15:23','/api/user/detail','245.23.122.44');
insert into table eight_log values('2016-11-09 14:17:23','/api/user/detail','22.74.142.137');
insert into table eight_log values('2016-11-09 14:19:23','/api/user/detail','54.93.212.87');
insert into table eight_log values('2016-11-09 14:20:23','/api/user/detail','218.15.167.248');
insert into table eight_log values('2016-11-09 14:24:23','/api/user/detail','20.117.19.75');
insert into table eight_log values('2016-11-09 15:14:23','/api/user/login','183.162.66.97');
insert into table eight_log values('2016-11-09 16:14:23','/api/user/login','108.181.245.147');

insert into table eight_log values('2016-11-09 14:17:23','/api/user/login','22.74.142.137');
insert into table eight_log values('2016-11-09 14:19:23','/api/user/login','22.74.142.137');

select
    t1.ip,
    t1.con
from 
(
select 
    ip,
    count(*)    con

from eight_log
where date_format(`date`,'yyyy-MM-dd HH')='2016-11-09 14' and interface ='/api/user/login'
group by ip
)t1
order by con desc limit 10;





