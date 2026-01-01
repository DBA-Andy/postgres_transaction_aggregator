--Analysis by account
select acct.institution,
       acct.account_name,
       CASE when trans.merchant_tag is not null then trans.merchant_tag else trans.description END AS transaction_description,
       count(*) as count_transactions,
       sum(trans.amount) as total_transactions
from transactions trans
inner join accounts acct on acct.account_id = trans.account_id
group by acct.institution,
         acct.account_name,
         transaction_description
order by count_transactions desc;


--analysis by top count of payments
select CASE when trans.merchant_tag is not null then trans.merchant_tag else trans.description END AS transaction_description,
       count(*) as count_transactions,
       sum(trans.amount) as total_transactions
from transactions trans
inner join accounts acct on acct.account_id = trans.account_id
group by transaction_description
order by count_transactions desc;

--analysis by top total paid
select CASE when trans.merchant_tag is not null then trans.merchant_tag else trans.description END AS transaction_description,
       count(*) as count_transactions,
       sum(trans.amount) as total_transactions,
       abs(sum(trans.amount)) as absolute_total_transactions
from transactions trans
inner join accounts acct on acct.account_id = trans.account_id
group by transaction_description
HAVING COUNT(*) > 1
order by absolute_total_transactions desc;
