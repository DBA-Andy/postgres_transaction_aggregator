--Analysis by account
select acct.institution,
       acct.account_name,
       trans.merchant_tag,
       count(*) as count_transactions,
       sum(trans.amount) as total_transactions
from transactions trans
inner join accounts acct on acct.account_id = trans.account_id
group by acct.institution,
         acct.account_name,
         normaliztrans.merchant_taged_description
order by count_transactions desc;


--analysis by top count of payments
select trans.merchant_tag,
       count(*) as count_transactions,
       sum(trans.amount) as total_transactions
from transactions trans
inner join accounts acct on acct.account_id = trans.account_id
group by trans.merchant_tag
order by count_transactions desc;

--analysis by top total paid
select trans.merchant_tag,
       count(*) as count_transactions,
       sum(trans.amount) as total_transactions,
       abs(sum(trans.amount)) as absolute_total_transactions
from transactions trans
inner join accounts acct on acct.account_id = trans.account_id
group by trans.merchant_tag
HAVING COUNT(*) > 1
order by absolute_total_transactions desc;
