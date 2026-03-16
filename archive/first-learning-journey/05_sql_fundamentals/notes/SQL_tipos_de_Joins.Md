# JOINS

--

## SEMANA 3 DIA 3

--

## O que é um Join

- O join serve para juntar duas tabelas usando uma coluna em comum, uma PK(Primary Key)
- Ele conecta apenas dados que tem relações entre si
- Exemplo:

clientes
pedidos

a ligação entre ambos seria uma ID, que idetifica quem pediu algo

---

## Tipos de Joins

### Inner Join

- Ele retorna apenas os registro que existem em ambas as tabelas
- Uma forma de decorar seria comparando com um intersecção
- Ou um Diagrama de ven, a parte do meio seria a certa
- Sintaxe:
  
SELECT
    *
FROM clientes c
INNER JOIN pedidos p
    ON c.id = p.cliente_id

### Left Join ou Right Join (X Outer Join)

- Retorna Todos os VALORES DE UM LADO da tabela e apenas os correspontes do outros lado
- Usando o exemplo do diagrama de venn, ficaria colorido apenas a bola de um lado + a intersecção
- Por comodida, sempre vi ser o Left Join ser mnais usada
- Sintaxe:

SELECT
    *
FROM clientes c
LEFT JOIN pedidos p
    ON c.id = p.cliente_id;

### Full Join (Full Outer Join)

- Retorna todos os VALORES DE UM LADO E DO OUTRO e combina
- No Diagrama de Venn é literalmente colorir tudo
- Onde não tem nada em ambos os lados, vem null
- Sintaxe:

SELECT *
FROM clientes c
FULL JOIN pedidos p
    ON c.id = p.cliente_id;

### Cross Join

- Ele faz Todas as combninações possíveis
- Por ele funcionar como um ""FOR"" ele pode explodir linhas
- Sintaxe:

SELECT *
FROM cores
CROSS JOIN tamanhos;

### Self Join

- Bem autoexplicativo, faz join consigo mesmo
