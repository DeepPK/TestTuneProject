--oltp_bench.sql
\set account_id_from random(1, 1000000)
\set account_id_to random(1, 1000000)
\set amount random(1, 1000)

BEGIN;

UPDATE accounts 
SET 
    balance = balance - :amount,
    last_update = NOW()
WHERE id = :account_id_from;

UPDATE accounts 
SET 
    balance = balance + :amount,
    last_update = NOW()
WHERE id = :account_id_to;

INSERT INTO transactions (
    from_account_id,
    to_account_id,
    amount,
    type,
    created_at
) VALUES (
    :account_id_from,
    :account_id_to,
    :amount,
    'transfer',
    NOW()
);

SELECT balance 
FROM accounts 
WHERE id = :account_id_from;

COMMIT;