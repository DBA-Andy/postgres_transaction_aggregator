--Analysis by account
select acct.institution,
       acct.account_name,
       CASE
           when upper(trans.description) like '%PAYMENT%' and trans.amount > 0
               then 'CC Payment'
           when upper(trans.description) like '%DOORDASH%' then 'Doordash'
           when upper(trans.description) like '%WALMART%' or upper(trans.description) like '%WM SUPERCENTER%' then 'Walmart'
           when upper(trans.description) like '%EVERYDAY%' then 'Everyday App'
           when upper(trans.description) like '%COVETRUS%' then 'Covetrus - Pet Meds'
           when upper(trans.description) like '%FIGO%' then 'Figo - Pet Insurance'
           when upper(trans.description) like '%LIFEPOINT%' then 'Lifepoint - Tithes'
           when upper(trans.description) like '%KROGER%' then 'Kroger'
           when upper(trans.description) like '%NATIONWIDE%' then 'Nationwide - Andy Payroll'
           when upper(trans.description) like '%CHEWY%' then 'Chewy - Pet Supplies'
           when upper(trans.description) like '%AMAZON%' and trans.amount > 0 then 'Bridget - Amazon Payroll'
           when upper(trans.description) like '%COMPONE%' and trans.amount > 0 then 'Bridget - Absence Plus Payroll'
           when upper(trans.description) like '%CARDINAL LAWNS%' then 'Cardinal Lawns'
           else trans.description
       END as normalized_description,
       count(*) as count_transactions,
       sum(trans.amount) as total_transactions
from transactions trans
inner join accounts acct on acct.account_id = trans.account_id
group by acct.institution,
         acct.account_name,
         normalized_description
order by count_transactions desc;


--analysis by top count of payments
select CASE
           when upper(trans.description) like '%PAYMENT%' and trans.amount > 0
               then 'CC Payment'
           when upper(trans.description) like '%DOORDASH%' then 'Doordash'
           when upper(trans.description) like '%WALMART%' or upper(trans.description) like '%WM SUPERCENTER%' then 'Walmart'
           when upper(trans.description) like '%EVERYDAY%' then 'Everyday App'
           when upper(trans.description) like '%COVETRUS%' then 'Covetrus - Pet Meds'
           when upper(trans.description) like '%FIGO%' then 'Figo - Pet Insurance'
           when upper(trans.description) like '%LIFEPOINT%' then 'Lifepoint - Tithes'
           when upper(trans.description) like '%KROGER%' then 'Kroger'
           when upper(trans.description) like '%NATIONWIDE%' then 'Nationwide - Andy Payroll'
           when upper(trans.description) like '%CHEWY%' then 'Chewy - Pet Supplies'
           when upper(trans.description) like '%AMAZON%' and trans.amount > 0 then 'Bridget - Amazon Payroll'
           when upper(trans.description) like '%COMPONE%' and trans.amount > 0 then 'Bridget - Absence Plus Payroll'
           when upper(trans.description) like '%CARDINAL LAWNS%' then 'Cardinal Lawns'
           else trans.description
       END as normalized_description,
       count(*) as count_transactions,
       sum(trans.amount) as total_transactions
from transactions trans
inner join accounts acct on acct.account_id = trans.account_id
group by normalized_description
order by count_transactions desc;

--analysis by top total paid
select CASE
           when upper(trans.description) like '%PAYMENT%' and trans.amount > 0
               then 'CC Payment'
           when upper(trans.description) like '%DOORDASH%' then 'Doordash'
           when upper(trans.description) like '%WALMART%' or upper(trans.description) like '%WM SUPERCENTER%' or upper(trans.description) like '%WAL-MART%' then 'Walmart'
           when upper(trans.description) like '%EVERYDAY%' then 'Everyday App'
           when upper(trans.description) like '%COVETRUS%' then 'Covetrus - Pet Meds'
           when upper(trans.description) like '%FIGO%' then 'Figo - Pet Insurance'
           when upper(trans.description) like '%LIFEPOINT%' then 'Lifepoint - Tithes'
           when upper(trans.description) like '%KROGER%' then 'Kroger'
           when (upper(trans.description) like '%NATIONWIDE%') or (upper(trans.description) like '%AMAZON%' and trans.amount > 0) or upper(trans.description) like '%COMPONE%' and trans.amount > 0 then 'Payroll'
           when upper(trans.description) like '%CHEWY%' then 'Chewy - Pet Supplies'
           when upper(trans.description) like '%CARDINAL LAWNS%' then 'Cardinal Lawns'
           when upper(trans.description) like '%COSTCO TRAVEL%' then 'COSTCO Travel'
           when upper(trans.description) like '%COSTCO WHSE%' then 'COSTCO'
           when upper(trans.description) like '%DIVIDEND DEPOSIT%' or upper(trans.description) like '%ANNUAL PERCENTAGE YIELD EARNED%' or upper(trans.description) like '%DIVIDEND DEPOSIT%' then 'Account Interest earnings'
           when upper(trans.description) like '%WORTHINGTON WOODS%' then 'Worthington Woods - Vet'
           when upper(trans.description) like '%AMAZON%' and trans.amount < 0 then 'Amazon Marketplace'
           when upper(trans.description) like '%BLACK WING%' or upper(trans.description) like '%CARDINAL CENTER%'  then 'Sporting Clays'
           else trans.description
       END as normalized_description,
       count(*) as count_transactions,
       sum(trans.amount) as total_transactions,
       abs(sum(trans.amount)) as absolute_total_transactions
from transactions trans
inner join accounts acct on acct.account_id = trans.account_id
group by normalized_description
HAVING COUNT(*) > 1
order by absolute_total_transactions desc;
