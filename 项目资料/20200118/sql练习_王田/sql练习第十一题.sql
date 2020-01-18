第11题
1）有三张表分别为会员表（member）销售表（sale）退货表（regoods）
（1）会员表有字段memberid（会员id，主键）credits（积分）；
（2）销售表有字段memberid（会员id，外键）购买金额（MNAccount）；
（3）退货表中有字段memberid（会员id，外键）退货金额（RMNAccount）；
2）业务说明：
（1）销售表中的销售记录可以是会员购买，也可是非会员购买。（即销售表中的memberid可以为空）
（2）销售表中的一个会员可以有多条购买记录
（3）退货表中的退货记录可以是会员，也可是非会员4、一个会员可以有一条或多条退货记录
查询需求：分组查出销售表中所有会员购买金额，同时分组查出退货表中所有会员的退货金额，
把会员id相同的购买金额-退款金额得到的结果更新到表会员表中对应会员的积分字段（credits）



1.  select
        s.memberid memberid,
        s.mnaccount mnaccount,
        r.rmnaccount rmnaccount
    from sale s
    join regoods r
    on s.memberid=r.memberid    --------t1


    select 
        t1.memberid,
        sum(t1.mnaccount) sum_buy,
        sum(t1.rmnaccount) sum_tui,
        sum_buy-sum_tui  sum_dif
    from t1
    group by t1.memberid        -----t2


    insert into member select  t2.memberid,t2.sum_dif  from t2 ;
