# Day 01 - ETL vs ELT (Comparação de Estratégias de Pipeline)

---

## Objetivo

- Entender a diferença entre ETL e ELT na prática  
- Construir dois fluxos distintos de processamento de dados  
- Comparar resultados entre transformação antes vs depois do carregamento  
- Trabalhar com dados de entregas simulando um cenário real de pipeline  

---

## Funções utilizadas

- `load_all` - Carrega múltiplos datasets da camada RAW  
  - parâmetros: `(path: str)`  
  - exemplo: leitura de arquivos CSV brutos em lote  

- `inspect` - Inspeciona estrutura dos dados  
  - parâmetros: `(df: pd.DataFrame, name: str)`  
  - exemplo: visualizar schema, tipos e tamanho  

- `save_parquet` - Salva DataFrame em formato parquet  
  - parâmetros: `(df: pd.DataFrame, path: str)`  
  - exemplo: persistir dados para camada processed  

- `save_csv` - Salva DataFrame em CSV  
  - parâmetros: `(df: pd.DataFrame, path: str)`  
  - exemplo: exportação para consumo externo  

- `load_parquet` - Carrega arquivos parquet  
  - parâmetros: `(path: str)`  
  - exemplo: leitura de dados já armazenados  

---

## Fluxos comparados

### ELT (Extract, Load, Transform)

- Dados são carregados **brutos primeiro**
- Armazenamento imediato na camada processed
- Transformações aplicadas **após o carregamento**

**Etapas:**
- Extração dos dados  
- Salvamento em parquet (sem tratamento)  
- Leitura posterior  
- Aplicação de filtros e limpeza  

---

### ETL (Extract, Transform, Load)

- Dados são **transformados antes de salvar**
- Apenas dados limpos são persistidos

**Transformações aplicadas:**
- Remoção de pedidos cancelados  
- Remoção de valores nulos em datas críticas  
- Conversão de colunas para datetime  
- Criação da variável `delivery_days`  
- Seleção de colunas relevantes  

---

## Output do Pipeline

- `elt_deliveries_raw.parquet` → dados brutos  
- `etl_deliveries_clean.parquet` → dados tratados  
- `etl_deliveries_clean.csv` → versão para análise  