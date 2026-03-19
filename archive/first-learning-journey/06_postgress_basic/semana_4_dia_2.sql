-- Objetivo: transferir saldo entre contas com commit

BEGIN;

UPDATE contas SET saldo = saldo - 200 WHERE id = 1;
UPDATE contas SET saldo = saldo + 200 WHERE id = 2;

COMMIT;