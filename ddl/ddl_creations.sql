--This script assumes you have a postgres database already.  You could easily create a new postgres database to hold these tables, though.

CREATE TABLE accounts_stage (
    account_id      INTEGER PRIMARY KEY,
    institution     TEXT NOT NULL,
    account_name    TEXT NOT NULL,
    account_type    TEXT NOT NULL,
    source_file     TEXT NOT NULL
);

CREATE TABLE transactions_stage (
    transaction_id     BIGSERIAL PRIMARY KEY,
    account_id         INTEGER NOT NULL,
    transaction_date   DATE NOT NULL,
    amount             NUMERIC(12,2) NOT NULL,
    transaction_type   TEXT NOT NULL,
    description        TEXT,

    CONSTRAINT fk_transactions_account_stage
        FOREIGN KEY (account_id)
        REFERENCES accounts_stage (account_id)
);

CREATE TABLE accounts (
    account_id      INTEGER PRIMARY KEY,
    institution     TEXT NOT NULL,
    account_name    TEXT NOT NULL,
    account_type    TEXT NOT NULL,
    source_file     TEXT NOT NULL
);

CREATE TABLE transactions (
    transaction_id     BIGSERIAL PRIMARY KEY,
    account_id         INTEGER NOT NULL,
    transaction_date   DATE NOT NULL,
    amount             NUMERIC(12,2) NOT NULL,
    transaction_type   TEXT NOT NULL,
    description        TEXT,
    merchant_tag       TEXT,

    CONSTRAINT fk_transactions_account
        FOREIGN KEY (account_id) REFERENCES accounts (account_id)
);

CREATE OR REPLACE PROCEDURE load_from_staging()
LANGUAGE plpgsql
AS $$
BEGIN
    -- 1️⃣ Insert new accounts from accounts_stage
    INSERT INTO accounts (account_id, institution, account_name, account_type, source_file)
    SELECT s.account_id, s.institution, s.account_name, s.account_type, s.source_file
    FROM accounts_stage s
    LEFT JOIN accounts a ON a.account_id = s.account_id
    WHERE a.account_id IS NULL;

    -- 2️⃣ Insert new transactions from transactions_stage
    INSERT INTO transactions (account_id, transaction_date, amount, transaction_type, description)
    SELECT s.account_id, s.transaction_date, s.amount, s.transaction_type, s.description
    FROM transactions_stage s
    LEFT JOIN transactions t
        ON t.account_id = s.account_id
        AND t.transaction_date = s.transaction_date
        AND t.amount = s.amount
        AND COALESCE(t.description,'') = COALESCE(s.description,'')
    WHERE t.transaction_id IS NULL;

    update transactions
    set merchant_tag =        
    CASE
        when upper(description) like '%PAYMENT%' and amount > 0
           then 'CC Payment'
        when upper(description) like '%DOORDASH%' then 'Doordash'
        when upper(description) like '%WALMART%' or upper(description) like '%WM SUPERCENTER%' or upper(description) like '%WAL-MART%' then 'Walmart'
        when upper(description) like '%EVERYDAY%' then 'Everyday App'
        when upper(description) like '%COVETRUS%' then 'Covetrus - Pet Meds'
        when upper(description) like '%FIGO%' then 'Figo - Pet Insurance'
        when upper(description) like '%LIFEPOINT%' then 'Lifepoint - Tithes'
        when upper(description) like '%KROGER%' then 'Kroger'
        when (upper(description) like '%NATIONWIDE%') or (upper(description) like '%AMAZON%' and amount > 0) or upper(description) like '%COMPONE%' and amount > 0 then 'Payroll'
        when upper(description) like '%CHEWY%' then 'Chewy - Pet Supplies'
        when upper(description) like '%CARDINAL LAWNS%' then 'Cardinal Lawns'
        when upper(description) like '%COSTCO TRAVEL%' then 'COSTCO Travel'
        when upper(description) like '%COSTCO WHSE%' then 'COSTCO'
        when upper(description) like '%DIVIDEND DEPOSIT%' 
           or upper(description) like '%ANNUAL PERCENTAGE YIELD EARNED%' 
           or upper(description) like '%DIVIDEND DEPOSIT%' 
               then 'Account Interest earnings'
        when upper(description) like '%WORTHINGTON WOODS%' then 'Worthington Woods - Vet'
        when upper(description) like '%AMAZON%' and amount < 0 then 'Amazon Marketplace'
        when upper(description) like '%BLACK WING%' or upper(description) like '%CARDINAL CENTER%'  then 'Sporting Clays'
        when upper(description) like '%COOPERSHAWK%' then 'Coopers Hawk - Wine Membership'
           else description
    END;

END;
$$;
