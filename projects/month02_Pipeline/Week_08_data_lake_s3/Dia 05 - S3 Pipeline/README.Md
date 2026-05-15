# Week 08 - Day 05 S3 Pipeline

---

## Objetivo

- Criar pipeline completo para Data Lake
- Automatizar camadas Bronze, Silver e Gold
- Integrar processamento local com Amazon S3
- Organizar pipeline em funções modulares
- Implementar monitoramento básico de execução

---

## Conceitos

### Pipeline End-to-End

- Fluxo completo:
  - ingestão
  - transformação
  - persistência
  - upload cloud

---
## Arquivo `day05-s3-pipeline.py`

### Objetivo

Ponto principal de execução do pipeline.

```python
from s3_pipeline import run

if __name__ == "__main__":
    run()
```

---

## Arquivo `s3_pipeline.py`

### Objetivo

Centralizar toda lógica do pipeline Data Lake + S3.

---

# Etapas do Pipeline

```txt
Extract
   ↓
Bronze
   ↓
Silver
   ↓
Gold
   ↓
Upload S3
```

---

## Configurações

### FILES

```python
FILES = {
    ...
}
```

### Objetivo

- Definir datasets de entrada
- Definir schemas esperados

---

### TRANSFORM_CONFIG

```python
TRANSFORM_CONFIG = {
    ...
}
```

### Objetivo

Centralizar:

- tipos
- filtros
- colunas derivadas
- preenchimento de nulos

---

# Funções criadas

---

## `extract`

```python
extract() -> dict
```

### Objetivo

- Ler datasets brutos

### Resultado

```python
{
    "deliveries": df,
    "drivers": df,
    "hubs": df
}
```

---

## `bronze`

```python
bronze(dfs)
```

### Objetivo

Salvar dados brutos no Data Lake.

### Camada Bronze

- sem transformação
- persistência original

---

## `silver`

```python
silver(dfs) -> pd.DataFrame
```

### Objetivo

Aplicar limpeza e padronização.

---

### Transformações aplicadas

#### Cast de tipos

```python
cast_types()
```

---

#### Filtros

```python
filter_rows()
```

### Exemplo

```python
status == delivered
```

---

#### Preenchimento de nulos

```python
fill_nulls()
```

---

#### Colunas derivadas

```python
delivery_days
cost_per_km
```

---

#### Joins

```python
joins()
```

### Enriquecimento

- drivers
- hubs

---

### Resultado

Dataset analítico limpo.

---

## `gold`

```python
gold(df)
```

### Objetivo

Criar agregações analíticas.

---

## Agregações criadas

### `deliveries_by_hub`

```python
groupby(["city", "state"])
```

### Métricas

- total deliveries
- revenue
- rating médio
- média de dias

---

### `deliveries_by_driver`

```python
groupby(["driver_id", "driver_name"])
```

### Métricas

- entregas
- receita
- rating

---

### `deliveries_by_status`

```python
groupby("status")
```

### Métricas

- custo médio
- distância média

---

## `upload`

```python
upload(s3)
```

### Objetivo

Enviar todas as camadas para o Amazon S3.

### Camadas enviadas

```txt
bronze/
silver/
gold/
```

---

# Função principal `run`

```python
run()
```

### Responsabilidades

- controlar pipeline
- medir tempo
- tratar erros
- gerar logs

---

## Controle de Tempo

```python
time.time()
```

### Objetivo

Monitorar performance.

---

## Exemplo de métricas

```txt
EXTRACT → OK (0.12s)
BRONZE → OK (0.08s)
SILVER → OK (0.42s)
GOLD   → OK (0.10s)
UPLOAD → OK (1.23s)
```

---

## Logging do Pipeline

```python
print("=" * 50)
```

### Informações exibidas

- bucket
- lake path
- horário
- duração
- status

---

## Tratamento de Erros

```python
traceback.print_exc()
```

## Fluxo Completo

```txt
Leitura CSV
      ↓
Bronze Layer
      ↓
Silver Layer
      ↓
Gold Layer
      ↓
Upload S3
      ↓
Resumo do Lake
```