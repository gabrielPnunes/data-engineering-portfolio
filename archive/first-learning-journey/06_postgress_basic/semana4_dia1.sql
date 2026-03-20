-- Exercício 1
-- Objetivo: consultar pedidos por cliente SEM índice

EXPLAIN ANALYZE
SELECT *
FROM pedidos
WHERE cliente_id = 100;
-- Criando índice
CREATE INDEX idx_pedidos_cliente_id
ON pedidos(cliente_id);
-- Exercício 2
-- Objetivo: repetir a mesma consulta COM índice

EXPLAIN ANALYZE
SELECT *
FROM pedidos
WHERE cliente_id = 100;

-- Criando índice
CREATE INDEX idx_pedidos_cliente_id
ON pedidos(cliente_id);

-- Exercício 2
-- Objetivo: repetir a mesma consulta COM índice

EXPLAIN ANALYZE
SELECT *
FROM pedidos
WHERE cliente_id = 100;

-- Exercício 2
-- Objetivo: repetir a mesma consulta COM índice

EXPLAIN ANALYZE
SELECT *
FROM pedidos
WHERE cliente_id = 100;
