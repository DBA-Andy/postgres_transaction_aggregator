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
        when upper(description) like '%ANNUAL PERCENTAGE YIELD EARNED%'
          or upper(description) like '%CHASE CREDIT CRD%'
          or upper(description) like '%CREDIT DIVIDEND%'
          or upper(description) like '%DIVIDEND DEPOSIT%' 
          or upper(description) like '%REWARDS CENTER  - REDEMPTION%' 
             then 'Account Interest earnings & CC Rewards'
        when upper(description) like '%CLASSIC CAR WASY%'
          or upper(description) like '%FIRESTONE%'
          or upper(description) like '%GEICO%'
          or upper(description) like '%GM FINANCIAL%'
          or upper(description) like '%JEFF WYLER%'
          or upper(description) like '%KRIEGER%'
          or upper(description) like '%LINE-X%'
          or upper(description) like '%OHIO OPLATES%'
          or upper(description) like '%PCJD%'
          or upper(description) like '%REALTRUCK%'
          or upper(description) like '%SPARKLE N SHINE%'
          or upper(description) like '%STELLANTIS%' 
             then 'Auto Expenses'
        when (upper(description) like '%ACH WITHDRAWAL CARDMEMBER SVC%'
          or upper(description) like '%ACH WITHDRAWAL CITIBANK CRDT CD%'
          or upper(description) like '%BANK OF AMERICA CREDIT CARD PAYMENT - ONLINE PMT%'
          or upper(description) like '%BILL PAYMT%COSTCO ANYWHERE%'
          or upper(description) like '%CARDMEMBER SVC  - ONLINE PMT%'
          or upper(description) like '%CHASE CARD SERV  - ONLINE PMT%'
          or upper(description) like '%CITIBANK CRDT CD  - ONLINE PMT%'
          or upper(description) like '%ELAN FIN%'
          or upper(description) like '%WITHDRAWAL CARDMEMBER SERVI CO: CARDMEMBER SERVI ENTRY CLASS CODE: PPD'
          or upper(description) like '%WRIGHT PATT CRED  - ONLINE PMT%')
         and amount < 0 
             then 'CC Payment from Checking'
        when (upper(description) like '%PAYMENT%' and amount > 0)
             then 'CC Payment'
        when upper(description) like '%COOPERSHAWK%' 
             then 'Coopers Hawk - Wine Membership'
        when upper(description) like '%ARBY%'
          or upper(description) like '%APPLEBEES%'
          or upper(description) like '%BDS MONGOLIAN%'
          or upper(description) like '%BILLS SEAFOOD%'
          or upper(description) like '%BILTMORE%'
          or upper(description) like '%BISTRO%'
          or upper(description) like '%BLIMPIE%'
          or upper(description) like '%BOB EVANS%'
          or upper(description) like '%BOSTONS%'
          or upper(description) like '%BUFFALO WILD%'
          or upper(description) like '%BWR%'
          or upper(description) like '%CAPRE CARIB%'
          or upper(description) like '%CARRIAGE HOUSE%'
          or upper(description) like '%CEDRICS%'
          or upper(description) like '%CHEESECAKE%'
          or upper(description) like '%CHICK FIL A%'
          or upper(description) like '%CHICK-FIL-A%'
          or upper(description) like '%CHIPOTLE%'
          or upper(description) like '%CONDADO TACOS%'
          or upper(description) like '%COOPERS HAWK%'
          or upper(description) like '%CRAZEE MULE%'
          or upper(description) like '%CREAMERY%'
          or upper(description) like '%CRIMSON CUP COFFEE%'
          or upper(description) like '%CTLP%'
          or upper(description) like '%DAIRY QUEEN%'
          or upper(description) like '%DIBELLAS%'
          or upper(description) like '%DOMINO%'
          or upper(description) like '%DONATOS%'
          or upper(description) like '%DOORDASH%'
          or upper(description) like '%EVERYDAY%'
          or upper(description) like '%FACTOR75%'
          or upper(description) like '%FIESTA JALISCO%'
          or upper(description) like '%FIREBIRDS%'
          or upper(description) like '%GABBY%'
          or upper(description) like '%GRAND VIEW YARD%'
          or upper(description) like '%HANEY%'
          or upper(description) like '%HUEY MAGOOS%'
          or upper(description) like '%HUCK%'
          or upper(description) like '%JETS%'
          or upper(description) like '%JOEY OS%'
          or upper(description) like '%LIMELIGHT SPORTS BAR%'
          or upper(description) like '%MARGARITAVILLE%'
          or upper(description) like '%MAVERICK%'
          or upper(description) like '%MCDONALD%'
          or upper(description) like '%NELSONS%'
          or upper(description) like '%NIELSENS%'
          or upper(description) like '%NORTHSTAR CAFE%'
          or upper(description) like '%ON THE BORDER%'
          or upper(description) like '%PIZZA%'
          or upper(description) like '%RENOS ROADHOUSE%'          
          or upper(description) like '%RESTAURANT%'          
          or upper(description) like '%SCRAMBLERS%'
          or upper(description) like '%SIAM EXPRESS%'
          or upper(description) like '%SALT + VINE%'
          or upper(description) like '%SUNSET SLUSH%'
          or upper(description) like '%STARBUCKS%'
          or upper(description) like '%TACO BELL%'
          or upper(description) like '%TEDS%'
          or upper(description) like '%THE GOAT%'
          or upper(description) like '%THE ISLAND MARKET%'
          or upper(description) like '%TST*%'
          or upper(description) like '%VOLTOLINIS%'
          or upper(description) like '%WARIOS%'
          or upper(description) like '%WENDY%'
          or upper(description) like '%WINERY%'
          or upper(description) like '%YE OLDE DURTY BIRD%'
             then 'Dining Out'
        when upper(description) like '%SECURITY CREDIT%'
          OR upper(description) like '%SHEIN.COM%'
          or upper(description) like '%TELEGRAM PREM%'
             then 'Fraud'
        when upper(description) like '%BP%'
          or upper(description) like '%CIRCLE K%'
          or upper(description) like '%COSTCO GAS%'
          or upper(description) like '%EXXON%'
          or upper(description) like '%LOVE%'
          or upper(description) like '%MARATHON%'
          or upper(description) like '%PHILLIPS 66%'
          or upper(description) like '%PILOT%'
          or upper(description) like '%SHEETZ%'
          or upper(description) like '%SHELL OIL%'
          or upper(description) like '%SPEEDWAY%'
          or upper(description) like '%SUNOCO%'
          or upper(description) like '%THORNTONS%'
          or upper(description) like '%TURKEY HILL%'
          or upper(description) like '%UNITED DAIRY%' 
             then 'Gas Stations'
        when upper(description) like '%ALDI%'
          or upper(description) like '%COSTCO BY%'
          or upper(description) like '%COSTCO WHSE%'
          or upper(description) like '%FOOD LION%'
          or upper(description) like '%FOODLION%'
          or upper(description) like '% IGA %'
          or upper(description) like '%INSTACART%'
          or upper(description) like '%KROGER%'
          or upper(description) like '%MEIJER%'
          or upper(description) like '%PUBLIX%'
          or upper(description) like '%WAL-MART%'
          or upper(description) like '%WALMART%'
          or upper(description) like '%WM SUPERCENTER%'
          or upper(description) like '%WMT PLUS%'
          or upper(description) like '%WWW COSTCO%'
             then 'Grocery Items'
        when upper(description) like '%CLARKSON%'
          or upper(description) like '%ESPORTA%'
          or upper(description) like '%GREAT CLIPS%'
          or upper(description) like '%LA CHATELAINE%'
          or upper(description) like '%LA FITNESS%'
          or upper(description) like '%NORTHSTAR FAMILY%'
          or upper(description) like '%SPENGA%'
          or upper(description) like '%SALLY BEAUTY%'
          or upper(description) like '%TELADO%'
          or upper(description) like '%TWELVE 7%'
          or upper(description) like '%WALGREENS%'
          or upper(description) like '%YMCA%' 
             then 'Gym Memberships & Personal Care'             
        when upper(description) like '%ALLPHASE%'
          or upper(description) like '%APEX PLUMBERS%'
          or upper(description) like '%CARE HEATING%'
          or upper(description) like '%CHECK%'
          or upper(description) like '%GOT MAIDS%'
          or upper(description) like '%HOME DEPOT%'
          or upper(description) like '%LOWES%'
          or upper(description) like '%MENARDS%' 
             then 'Home Maintenance Expenses'             
        when upper(description) like '%CARDINAL LAWNS%' 
          or upper(description) like '%WORTHINGTON MOWER%'
             then 'Lawncare Expenses'
        when upper(description) like '%LIFEPOINT%' 
             then 'Lifepoint - Tithes'
        when upper(description) like '%MIDWEST LOAN SER%'
             then 'Mortgage'
        when (upper(description) like '%AMAZON%' and amount < 0)
          OR upper(description) like '%EBAY%'
          OR upper(description) like '%TARGET%'
          or upper(description) like '%TBE*BRADFORD ONLINE%'
          or upper(description) like 'SP%' 
             then 'Online Shopping'             
        when (upper(description) like '%AMAZON%' and amount > 0) 
          or (upper(description) like '%COMPONE%' and amount > 0)
          or (upper(description) like '%NATIONWIDE%') 
             then 'Payroll'
        when upper(description) like '%CHEWY%'
          or upper(description) like '%COVETRUS%'
          or upper(description) like '%DOGGIE DAY SPA%'
          or upper(description) like '%FIGO%'
          or upper(description) like '%PET BUTLER%'
          or upper(description) like '%PETSMART%'
          or upper(description) like '%PETSUITES%'
          or upper(description) like '%WORTHINGTON WOODS%'
             then 'Pet Expenses'
        when upper(description) like '%BLACK WING%' 
          or upper(description) like '%CAB STORE COLUMBUS%'  
          or upper(description) like '%LEPD FIREARMS%'  
          or upper(description) like '%CARDINAL CENTER%'  
             then 'Sporting Clays & Ranges'
        when upper(description) like '%ABC LIQUOR%'
          or upper(description) like '%ACTIVATE POLARIS%'
          or upper(description) like '%AUDIBLE%'
          or upper(description) like '%BARNES&NOBLE%'
          or upper(description) like '%CHARLES MILL%'
          or upper(description) like '%COLUMBUS ZOO%'
          or upper(description) like '%DIE HARD DICE%'
          or upper(description) like '%DISNEY PLUS%'          
          or upper(description) like '%DISNEYPLUS%'
          OR upper(description) like '%HERO FORGE%'
          or upper(description) like '%HULU%'
          or upper(description) like '%JOHNS LIQUOR%'
          or upper(description) like '%LACOMEDIA%'
          or upper(description) like '%NETFLIX%'
          or upper(description) like '%PARAMOUNT%'
          or upper(description) like '%ROLL20.NET%'
          or upper(description) like '%SLING.COM%'
          or upper(description) like '%SPOTIFY%'
          or upper(description) like '%SQ *COLUMBUS ASSOCIATI Columbus      OH%'
          or upper(description) like '%SQ *WALKER FARM%'
          or upper(description) like '%STATEHOUSE PARKING%'
          or upper(description) like '%STUDIO 35%'
          or upper(description) like '%THE NEW PAPER%'
          or upper(description) like '%THRIFT BOOKS%'
          or upper(description) like '%TM *HARRY POTTER AND T 800-653-8000  CA%' 
             then 'Streaming and Entertainment'
        when upper(description) like '%GOOGLE%'
          or upper(description) like '%MICROSOFT%'
          or upper(description) like '%RING%' 
             then 'Tech Subscriptions'
        when upper(description) like '%TO LOAN%'
          or upper(description) like '%TO SHARE%'
          or upper(description) like '%TRANSFER TO%'
          or upper(description) like '%XFER TO%'
          or (upper(description) like '%VENMO%' and amount < 0 )
             then 'Transfer out'
        when upper(description) like '%FROM%SHARE%'
          or upper(description) like '%TRANSFER FROM%'
          or upper(description) like '%XFER FROM%'
          or (upper(description) like '%VENMO%' and amount > 0 )
            then 'Transfer in'
        when upper(description) like '%CMH PARKING%'
          or upper(description) like '%CLEARME%'
          or upper(description) like '%COSTCO TRAVEL%'
          or upper(description) like '%COURTYARD BY MARRIOTT%'
          or upper(description) like '%ENTERPRISE RENT-A-CAR%'
          or upper(description) like '%EZPASS%'
          or upper(description) like '%FINDLAY PARKING GARAGE%'
          or upper(description) like '%GCCC OHIO CENTER PARKI%'
          or upper(description) like '%PREMIER GUEST SERVICES%'
          or upper(description) like '%RIGHTWAY PARKING%'
          or upper(description) like '%SOUTHWES%'
          or upper(description) like '%SUNSET PROPERTIES%'
          or upper(description) like '%THEPARKINGSPOT%'
          or upper(description) like '%TRAVELERS%'
          or upper(description) like '%UTOPIA OF THE SEAS%'
          or upper(description) like '%WVTP BECKLEY%'
             then 'Travel'
        when upper(description) like '%AEP  - ONLINE PMT%'
          or upper(description) like '%BREEZELINE%'
          or upper(description) like '%COLS UTILITIES%'
          or upper(description) like '%COLUMBIA GAS OH  - ONLINE PMT%'
          or upper(description) like '%MINT MOBILE%'
          or upper(description) like '%SPECTRUM%'
          or upper(description) like '%TMOBILE%' 
             then 'Utlities'
        else null
    END;

    vacuum full;

END;
$$;
