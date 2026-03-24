# Day 02 - Treat null and outliers

---

## O que foi feito 
Funções de limpeza para tratar valores nulos e detectar/remover
outliers usando o método IQR.

---

## Funções Criadas
`drop_null_rows`: Dropa os valores nulos do DF
`fill_null_with_value`: Preenche valores nulos com a média
`fill_null_with_median` Preenche valores nulos com a mediana
`fill_null_with_mode`: Preenche valores nulos com a moda
`detect_outliers_iqr`: Detecta outliers via IQR  sem removelos
`remove_outlier`: Retira os valores baseados no IQR
