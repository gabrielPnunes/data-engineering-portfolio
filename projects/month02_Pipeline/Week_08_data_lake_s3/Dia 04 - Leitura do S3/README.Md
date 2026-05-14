# Week 08 - Day 04 Leitura do S3

---

## Objetivo

- Ler arquivos diretamente do Amazon S3
- Trabalhar com datasets parquet armazenados na cloud
- Fazer download de objetos S3
- Validar consistência entre cloud e ambiente local
- Simular consumo de Data Lake em produção

---

## Conceitos

### Leitura Remota

- Arquivos são carregados diretamente do S3
- Sem necessidade de armazenamento local prévio

---

### Streaming de Objetos

- Dados são lidos em memória usando buffer
- Muito utilizado em pipelines modernos

---

### BytesIO

```python
BytesIO(resp["Body"].read())
```

### Objetivo

- Transformar bytes do S3 em buffer compatível com pandas

---

### Consistência de Dados

- Comparação entre datasets locais e cloud
- Garante integridade do pipeline

---

## Cliente S3

```python
get_client(region="us-east-1")
```

### O que faz

- Cria cliente autenticado AWS
- Usa variáveis de ambiente

---

## Pipeline (`day04-s3-read.py`)

### O que o pipeline faz

- Lista objetos das camadas
- Lê parquet diretamente do S3
- Carrega datasets Gold
- Faz download de arquivos Bronze
- Valida consistência entre local e cloud

---

## Listar Camadas

```python
for layer in ["bronze", "silver", "gold"]:
    list_layer(s3, BUCKET, layer)
```

### Resultado esperado

```txt
bronze/
silver/
gold/
```

---

## Ler Silver diretamente do S3

```python
df_silver = read_parquet(
    s3,
    BUCKET,
    "silver/deliveries_clean.parquet"
)
```

### Informações exibidas

- linhas
- colunas
- dtypes

---

## Ler camada Gold completa

```python
gold_dfs = read_layer(s3, BUCKET, "gold")
```

### Objetivo

- Carregar múltiplos datasets automaticamente

### Exemplos

- deliveries_by_hub
- deliveries_by_driver
- deliveries_by_status

---

## Download de arquivos Bronze

```python
download_file(
    s3,
    BUCKET,
    "bronze/deliveries.parquet",
    local_path
)
```

### Objetivo

- Recuperar arquivos do Data Lake cloud

---

## Validação de consistência

```python
match = len(df_s3) == len(df_local)
```

### Objetivo

- Comparar dataset local vs S3

### Resultado esperado

```txt
OK
```

---

## Fluxo de execução

```txt
Autenticação AWS
        ↓
Listagem de camadas
        ↓
Leitura parquet do S3
        ↓
Leitura completa Gold
        ↓
Download Bronze
        ↓
Validação local vs cloud
```