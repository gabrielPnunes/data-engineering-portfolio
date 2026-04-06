# Day 03 - WINDOW FUNCTIONS

## Objetivos

- Explorar funções de janela para criação de rankings e ordenação dentro de grupos
- Entender diferenças entre ROW_NUMBER, RANK e DENSE_RANK
- Comparar valores entre linhas e criar métricas
- Trabalhar com acumulados e médias móveis

## Queries e funções criadas

- `row_number_by_partition` - Enumera as linhas para cada categoria
  - parâmetros: `(con, table: str, partition_col: str, order_col: str)`
  - exemplo: pegar o maior valor por grupo → `WHERE row_num = 1`

---

- `rank_by_partition` - Cria um ranking, semelhante ao ROW_NUMBER, porém considera empates e pula posições (ex: dois 2°, próximo é 4°)
  - parâmetros: `(con, table: str, partition_col: str, order_col: str)`
  - exemplo: ranking de vendas com empate → posições repetidas

---

- `dense_rank_by_partition` - Cria um ranking semelhante ao RANK, porém não pula posições após empate
  - parâmetros: `(con, table: str, partition_col: str, order_col: str)`
  - exemplo: ranking contínuo sem buracos → 1,2,2,3

---

- `lag_column` - Pega a linha anterior dentro da partição
  - parâmetros: `(con, table: str, partition_col: str, order_col: str, value_col: str, offset: int = 1)`
  - exemplo: calcular variação → `valor - prev_valor`

---

- `lead_column` - Pega a próxima linha dentro da partição
  - parâmetros: `(con, table: str, partition_col: str, order_col: str, value_col: str, offset: int = 1)`
  - exemplo: prever comportamento → `next_valor - valor`

---

- `running_total` - Calcula o acumulado progressivo (soma acumulada)
  - parâmetros: `(con, table: str, partition_col: str, order_col: str, value_col: str)`
  - exemplo: total acumulado de vendas ao longo do tempo

---

- `moving_average` - Calcula média móvel baseada em uma janela
  - parâmetros: `(con, table: str, partition_col: str, order_col: str, value_col: str, window: int = 3)`
  - exemplo: média dos últimos 3 períodos → suavizar variações
