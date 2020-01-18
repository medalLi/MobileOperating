第5题
有日志如下，请写出代码求得所有用户和活跃用户的总数及平均年龄。（活跃用户指连续两天都有访问记录的用户）
日期 用户 年龄
11,test_1,23
11,test_2,19
11,test_3,39
11,test_1,23
11,test_3,39
11,test_1,23
12,test_2,19
13,test_1,23




create table fiveth(`date` string,user_id string ,age int );


insert into table fiveth values ('11','test_1',23);
insert into table fiveth values ('11','test_2',19);
insert into table fiveth values ('11','test_3',39);
insert into table fiveth values ('11','test_1',23);
insert into table fiveth values ('11','test_3',39);
insert into table fiveth values ('11','test_1',23);
insert into table fiveth values ('12','test_2',19);
insert into table fiveth values ('13','test_1',23);



总人数和平均年龄

1.每个人的年龄相同，按照user_id，和age分组
select 
    count(*),
    avg(age)
from 
(
select 
    user_id,age
from 
fiveth
group by user_id,age
)t1


活跃人数和平均年龄

1.先按照日期和用户去重
2.开窗函数，利用等差数列



select 
    user_id,`date`,age

from 
fiveth group by 
user_id,`date`,age         --fiveth1


select 
    `date`
    user_id,
    age,
    rank() over(partition by user_id order by `date`) rank
from 
fiveth1      --t1


select 
    t1.`date`-rank `date_dif`,
    user_id,
    age
from t1         ---t2


select 
   t2.age,
    t2.user_id
from t2
group by t2.user_id,t2.`date_fid`,t2.age
having count(*) >=2;        --t3


select 
    count(*),
    avg(t3.age)
from t3




终极sql


select 
    count(*),
    avg(t3.age)
from 
(
    select 
        t2.age,
        t2.user_id
    from 
    (
        select 
            t1.`date`- rank `date_dif`,
            user_id,
            age
        from 
        (
            select 
            `date`,
            user_id,
            age,
            rank() over(partition by user_id order by `date`) rank
            from 
            (
                select 
                    user_id,`date`,age

                from 
                fiveth
                group by 
                user_id,`date`,age

                )fiveth1
        )t1

    )t2
    group by t2.user_id,t2.`date_dif`,t2.age
    having count(*) >=2
)t3






