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
END;
$$;
