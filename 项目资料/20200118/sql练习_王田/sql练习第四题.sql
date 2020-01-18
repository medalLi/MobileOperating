
第4题
有一个5000万的用户文件(user_id，name，age)，一个2亿记录的用户看电影的记录文件(user_id，url)，根据年龄段观看电影的次数进行排序？




1.建表
create table forth_user(user_id string,name string,age int);
create table forth_log(user_id string,url string);

insert into table forth_user values('001','wt',10);
insert into table forth_user values('002','ls',18);
insert into table forth_user values('003','zz',30);
insert into table forth_user values('004','zz',50);



insert into table forth_log values('001','sdf');
insert into table forth_log values('001','wss');
insert into table forth_log values('002','sdf');
insert into table forth_log values('003','sdf');
insert into table forth_log values('004','sdf');



2.分析需求


先求出每个人看了几次电影,t1
然后t1和user表join，拼接age字段 t2表
划分年龄段，0-20，20-40，40-60，60--
按年龄段分组，按照次数排序



select 
    user_id,
    count(*)    con
from forth_log
group by user_id;     ----t1


select
    u1.age age ,
    t1.user_id,
    t1.con con
from forth_user u1
join 
(
    select 
    user_id,
    count(*)    con
from forth_log
group by user_id   
)t1
on u1.user_id=t1.user_id        --t2



select 
    t2.con con,
    case 
        when  0<=t2.age and t2.age<20 then 'a'
        when 20<=t2.age and t2.age<40 then 'b' 
        when 40<=t2.age and t2.age<60 then 'c' 
        else 'd'
    end as category
from t2                         ---t3



select 
    t3.category 
    sum(t3.con)
from t3
gruop by t3.category 



=====================================================================



3.终极sql
select
    t4.category,
    t4.sumcon
from 

(
select 
    t3.category category,
    sum(t3.con) sumcon
from 
(
    select 
        t2.con con,
        case 
            when  0<=t2.age  and t2.age<20 then 'a'
            when  20<=t2.age and t2.age<40 then 'b'
            when  40<=t2.age and t2.age<60 then 'c'
            else    'd'
        end as category
    from 
    (
        select
            u1.age,
            t1.user_id,
            t1.con con
        from forth_user u1
        join 
        (
        select 
            user_id,
            count(*)    con
        from forth_log
        group by user_id
        )t1
        on u1.user_id=t1.user_id
    )t2 

)t3
group by t3.category 
)t4
order by t4.sumcon











                                