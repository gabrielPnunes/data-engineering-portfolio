# Week 08 - Day 03 S3 Upload

---

## Objetivo

- Fazer upload de datasets para Amazon S3
- Organizar camadas Bronze, Silver e Gold na nuvem
- Trabalhar com metadata em objetos S3
- Validar uploads automaticamente
- Simular Data Lake cloud real

---

## Conceitos

### Upload para S3

- Processo de envio de arquivos para buckets
- Cada arquivo é armazenado como objeto

---

### Object Key

- Caminho único do objeto no bucket

Exemplo:

```txt
silver/deliveries_clean.parquet
```

---

### Metadata

- Informações adicionais armazenadas junto ao objeto
- Muito usada para:
  - versionamento
  - lineage
  - auditoria
  - governança

---

### Prefix

- Estrutura lógica similar a pastas
- Organiza objetos no bucket

Exemplo:

```txt
bronze/
silver/
gold/
```

#


---

## Pipeline (`day03-s3-upload.py`)

### O que o pipeline faz

- Conecta na AWS
- Faz upload das camadas:
  - Bronze
  - Silver
  - Gold

- Adiciona metadata
- Valida uploads
- Lista objetos do bucket

---

## Upload Bronze

```python
upload_folder(s3, f"{LAKE_PATH}/bronze", BUCKET, "bronze")
```

### Resultado

Objetos enviados:

```txt
bronze/deliveries.parquet
bronze/drivers.parquet
bronze/hubs.parquet
```

---

## Upload Silver

```python
upload_folder(s3, f"{LAKE_PATH}/silver", BUCKET, "silver")
```

---

## Upload Gold

```python
upload_folder(s3, f"{LAKE_PATH}/gold", BUCKET, "gold")
```

---

## Upload com Metadata

```python
upload_file(
    s3,
    silver_file,
    BUCKET,
    "silver/deliveries_clean_v2.parquet",
    metadata={
        "source":  "deliveries.csv",
        "layer":   "silver",
        "rows":    "33951",
        "version": "2",
    }
)
```

---

## Benefícios da Metadata

- rastreabilidade
- auditoria
- lineage
- versionamento
- governança de dados

---

## Validação de Upload

```python
exists = object_exists(s3, BUCKET, key)
```

### Objetivo

- Garantir que upload ocorreu corretamente

---

## Listagem de Objetos

```python
list_objects(s3, BUCKET, prefix=layer)
```

### Resultado esperado

```txt
bronze/
silver/
gold/
```

---

## Fluxo de execução

```txt
Autenticação AWS
        ↓
Upload Bronze
        ↓
Upload Silver
        ↓
Upload Gold
        ↓
Upload com Metadata
        ↓
Validação
        ↓
Listagem de Objetos
```