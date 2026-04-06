# Day 05 - SQL PROJECT

## Objetivo

- Criar uma camada analítica reutilizável sobre o modelo dimensional
- Aplicar Window Functions em cenários reais (ranking, tendência, acumulado)
- Gerar relatórios prontos para análise

---

## Queries e funções criadas

- `top_by_partition` - Retorna Top N registros dentro de cada partição (ex: top produtos por cidade)
  - parâmetros: `(con, fact_table, dim_partition, dim_metric, partition_col, metric_col, group_cols, rank=1)`
  - exemplo: top 1 produto por cidade → maior receita

---

- `revenue_ranking` - Cria ranking de receita por grupo
  - parâmetros: `(con, fact_table, dim_table, join_col, group_cols, value_col='total_price', extra_metrics=None)`
  - exemplo: ranking de categorias por receita total

---

- `running_total` - Calcula total acumulado dentro de cada partição
  - parâmetros: `(con, fact_table, dim_table, join_col, partition_col, order_col, value_col, label_col=None)`
  - exemplo: evolução acumulada de vendas ao longo do tempo

---

- `lag_analysis` - Calcula variação entre valor atual e anterior
  - parâmetros: `(con, fact_table, dim_table, join_col, partition_col, order_col, value_col, label_col=None)`
  - exemplo: crescimento ou queda de vendas período a período

---

- `moving_average` - Calcula média móvel para suavizar séries temporais
  - parâmetros: `(con, fact_table, dim_table, join_col, partition_col, order_col, value_col, window=3, label_col=None)`
  - exemplo: média dos últimos 3 períodos → tendência de vendas

---

- `save_report` - Salva DataFrame como CSV
  - parâmetros: `(df, path, report_name)`
  - exemplo: exportar relatório para consumo externo
