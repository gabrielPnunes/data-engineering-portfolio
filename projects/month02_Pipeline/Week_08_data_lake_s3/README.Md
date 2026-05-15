# Week 08 - Data Lake + Amazon S3

---

# Objetivo da Semana

- Introduzir conceitos de Data Lake
- Trabalhar com arquitetura Bronze / Silver / Gold
- Integrar pipelines locais com Amazon S3
- Realizar uploads e leituras cloud
- Criar pipeline completo automatizado

---

# Tecnologias Utilizadas

- Python
- Pandas
- Parquet
- AWS S3
- Boto3
- dotenv

---

# Estrutura da Semana

```txt
Week 08
│
├── Day 01 → Data Lake Concepts
├── Day 02 → S3 Bucket
├── Day 03 → Upload para S3
├── Day 04 → Leitura do S3
└── Day 05 → Pipeline completo S3
```

---

# Day 01 - Data Lake Concepts

## Objetivo

- Implementar arquitetura Bronze / Silver / Gold
- Organizar datasets por camadas
- Criar agregações analíticas

---

## Camadas

### Bronze

- Dados brutos
- Sem transformação

```txt
bronze/
```

---

### Silver

- Dados limpos
- Padronização
- Tratamento de nulos
- Tipagem
- Enriquecimento

```txt
silver/
```

---

### Gold

- Dados agregados
- Analytics
- KPIs

```txt
gold/
```

---

## Principais Funções

### `to_bronze`
Salva dados brutos.

### `to_silver`
Salva dados tratados.

### `to_gold`
Salva agregações analíticas.

### `lake_summary`
Gera resumo do Data Lake.

---

# Day 02 - S3 Bucket

## Objetivo

- Criar buckets S3
- Configurar lifecycle
- Validar autenticação AWS

---

## Conceitos

### Bucket

- Container de objetos S3

### Lifecycle

- Expiração automática de arquivos

### Região AWS

```txt
us-east-1
```

---

## Principais Funções

### `get_client`
Cria cliente S3 autenticado.

### `create_bucket`
Cria bucket.

### `bucket_exists`
Valida existência.

### `set_lifecycle`
Configura expiração automática.

### `list_buckets`
Lista buckets AWS.

---

# Day 03 - Upload para S3

## Objetivo

- Enviar datasets para S3
- Organizar Data Lake cloud
- Trabalhar com metadata

---

## Estrutura no S3

```txt
s3://de-portfolio-deliveries/

├── bronze/
├── silver/
└── gold/
```

---

## Principais Funções

### `upload_file`
Upload individual.

### `upload_folder`
Upload de diretórios.

### `object_exists`
Validação de upload.

### `list_objects`
Listagem de objetos.

### `get_object_metadata`
Leitura de metadata.

---

## Metadata

```python
metadata={
    "layer": "silver",
    "version": "2"
}
```

---

# Day 04 - Leitura do S3

## Objetivo

- Ler parquet diretamente do S3
- Fazer download de objetos
- Validar consistência cloud/local

---

## Conceitos

### Streaming de objetos

- Leitura em memória

### BytesIO

```python
BytesIO()
```

---

## Principais Funções

### `read_parquet`
Lê parquet do S3.

### `read_csv`
Lê CSV do S3.

### `download_file`
Baixa arquivos localmente.

### `list_layer`
Lista objetos por camada.

### `read_layer`
Lê camada inteira.

---

# Day 05 - Pipeline Completo S3

## Objetivo

- Automatizar Data Lake completo
- Integrar processamento + upload S3
- Monitorar execução

---

## Fluxo do Pipeline

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

## Etapas

### Extract

- Leitura datasets

### Bronze

- Persistência bruta

### Silver

- Transformação
- limpeza
- joins
- tipagem

### Gold

- agregações analíticas

### Upload

- envio automático para S3

---

## Monitoramento

### Tempo por etapa

```txt
EXTRACT → OK (0.2s)
SILVER → OK (0.5s)
UPLOAD → OK (1.0s)
```

---

## Tratamento de Erros

```python
try:
    ...
except Exception:
```

---

# Principais Aprendizados

- Arquitetura Data Lake
- Bronze / Silver / Gold
- Amazon S3
- Upload e leitura cloud
- Metadata
- Modularização
- Pipelines automatizados
- Organização analítica
- Integração AWS

---