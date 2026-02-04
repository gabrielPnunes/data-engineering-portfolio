-- 1 Seleciona todas as colunas da tabela world_population
SELECT *
FROM world_population;


-- 2 Seleciona apenas a coluna capital
SELECT capital
FROM world_population;


-- 3 Seleciona país, capital e população de 2022
SELECT 
    country,
    capital,
    populacao_2022
FROM world_population;


-- 4 Filtra países com população acima de 50 milhões
SELECT *
FROM world_population
WHERE populacao_2022 > 50000000;


-- 5 Filtra países cujo continente é Europa
SELECT *
FROM world_population
WHERE continent = 'Europe';


-- 6 Exclui países do continente europeu
SELECT *
FROM world_population
WHERE continent != 'Europe';


-- 7 Filtra países com área maior que 1.000.000 km²
SELECT *
FROM world_population
WHERE area_km2 > 1000000;


-- 8 Filtra países com população entre 20M e 80M
SELECT *
FROM world_population
WHERE populacao_2022 >= 20000000
  AND populacao_2022 <= 80000000;


-- 9 Combina filtro por continente e população
SELECT 
    continent,
    country,
    populacao_2022,
    area_km2,
    porcentagem_mundial
FROM world_population
WHERE populacao_2022 > 10000000
  AND continent = 'Africa';


-- 10 Filtra países com 1% ou mais da população mundial
SELECT 
    country,
    populacao_2022,
    area_km2,
    density_km2,
    porcentagem_mundial
FROM world_population
WHERE porcentagem_mundial >= 1;


-- 11 Ordena países em ordem alfabética
SELECT country
FROM world_population
ORDER BY country;


-- 12 Ordena por população crescente
SELECT country, populacao_2022
FROM world_population
ORDER BY populacao_2022;


-- 13 Ordena por população decrescente
SELECT country, populacao_2022
FROM world_population
ORDER BY populacao_2022 DESC;


-- 14 Filtra Europa e ordena por área
SELECT country, area_km2
FROM world_population
WHERE continent = 'Europe'
ORDER BY area_km2;


-- 15 Filtra África e ordena por população decrescente
SELECT country, populacao_2022
FROM world_population
WHERE continent = 'Africa'
ORDER BY populacao_2022 DESC;


-- 16 Filtra países com população acima de 10M e ordena por porcentagem mundial
SELECT 
    country,
    populacao_2022,
    porcentagem_mundial
FROM world_population
WHERE populacao_2022 > 10000000
ORDER BY porcentagem_mundial DESC;


-- 17 Filtra Ásia e ordena alfabeticamente
SELECT country
FROM world_population
WHERE continent = 'Asia'
ORDER BY country;


-- 18 Filtra países pequenos (área < 500 mil km²) e ordena por população
SELECT country, area_km2, populacao_2022
FROM world_population
WHERE area_km2 < 500000
ORDER BY populacao_2022 DESC;


-- 19 Filtra países por faixa de porcentagem mundial (0.1% a 1%)
SELECT country, porcentagem_mundial
FROM world_population
WHERE porcentagem_mundial >= 0.1
  AND porcentagem_mundial <= 1;


-- 20 Ordena primeiro por continente e depois por nome do país
SELECT country, continent
FROM world_population
ORDER BY continent, country;
