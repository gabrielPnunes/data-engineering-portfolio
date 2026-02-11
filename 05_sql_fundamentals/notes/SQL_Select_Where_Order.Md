# SQL básico, SELECT, WHERE, ORDER BY

--

## SEMANA 3 DIA 2

--

## Select

- Seleciona o que eu irei ver de uma tabela
- Sintaxe:

SELECT
    * serve como um select todos
FROM
    casa;

- Apenas Colunas Especificas

SELECT
    nome, idade
FROM
    casa;

--

## Where

- Define uma condição ou filtro das linhas, geralmente usando comparadores e funções
- Sintaxe:
  
SELECT
    *
FROM
    CASA
WHERE
    IDADE > 25;

--

## Order By

- Basicamente ordena os dados tragos
- Pode ser de forma Ascendente(ASC) ou Descendente(DESC)
- ASC ja é o default
- Sintaxe:

SELECT
    *
FROM
    CASA
ORDER BY
    IDADE DESC;
