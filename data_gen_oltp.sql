--data_gen_oltp.sql
DROP TABLE IF EXISTS transactions;
DROP TABLE IF EXISTS accounts;

CREATE TABLE accounts (
    id SERIAL PRIMARY KEY,
    balance DECIMAL(15,2) NOT NULL DEFAULT 1000.00,
    last_update TIMESTAMP DEFAULT NOW()
);

CREATE SEQUENCE accounts_id_seq START 1;
ALTER TABLE accounts ALTER COLUMN id SET DEFAULT nextval('accounts_id_seq');

INSERT INTO accounts (balance)
SELECT 
    1000.00 + (random() * 5000)
FROM generate_series(1, 1000000);

SELECT setval('accounts_id_seq', (SELECT MAX(id) FROM accounts));

CREATE TABLE transactions (
    id SERIAL PRIMARY KEY,
    from_account_id INT NOT NULL REFERENCES accounts(id),
    to_account_id INT NOT NULL REFERENCES accounts(id),
    amount DECIMAL(15,2) NOT NULL,
    type VARCHAR(20) NOT NULL,
    created_at TIMESTAMP DEFAULT NOW()
);

INSERT INTO transactions (from_account_id, to_account_id, amount, type, created_at)
SELECT
    (random() * (SELECT MAX(id)-1 FROM accounts) + 1)::int,
    (random() * (SELECT MAX(id)-1 FROM accounts) + 1)::int,
    (random() * 1000 + 10)::numeric(10,2),
    CASE 
        WHEN random() < 0.8 THEN 'transfer'
        WHEN random() < 0.9 THEN 'deposit'
        ELSE 'withdrawal'
    END,
    NOW() - (random() * interval '365 days')
FROM generate_series(1, 10000000);