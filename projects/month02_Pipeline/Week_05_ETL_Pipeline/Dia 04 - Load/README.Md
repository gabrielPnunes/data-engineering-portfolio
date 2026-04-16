# Day 04 - LOAD

---

## Objetivo

- Evoluir a camada de carregamento (LOAD)  
- Suportar múltiplos formatos de saída  
- Garantir organização por camadas (data lake)  
- Adicionar validação e rastreabilidade dos dados  
- Criar mecanismos de auditoria do pipeline  

---

## Queries e funções criadas

- `save_json` - Salva dados brutos em JSON  
  - parâmetros: `(data: dict, output_path: str)`  
  - exemplo: persistir dados originais da ingestão  

- `save_csv` - Salva DataFrame em CSV  
  - parâmetros: `(df: pd.DataFrame, output_path: str)`  
  - exemplo: exportação para consumo externo  

- `save_parquet` - Salva DataFrame em parquet  
  - parâmetros: `(df: pd.DataFrame, output_path: str)`  
  - exemplo: armazenamento otimizado  

- `save_all_formats` - Salva dados em múltiplos formatos  
  - parâmetros: `(df: pd.DataFrame, data_raw: dict, base_path: str, filename: str)`  
  - exemplo: salvar RAW + Processed simultaneamente  

- `load_json` - Carrega JSON  
  - parâmetros: `(filepath: str)`  
  - exemplo: reprocessamento de dados brutos  

- `load_csv` - Carrega CSV  
  - parâmetros: `(filepath: str)`  
  - exemplo: leitura para análise  

- `load_parquet` - Carrega parquet  
  - parâmetros: `(filepath: str)`  
  - exemplo: leitura otimizada  

- `load_layer` - Salva dados por camada  
  - parâmetros: `(df: pd.DataFrame, base_path: str, layer: str, filename: str)`  
  - exemplo: organizar dados em raw / processed  

- `validate_load` - Valida integridade do carregamento  
  - parâmetros: `(path: str, expected_rows: int, expected_cols: int)`  
  - exemplo: garantir consistência pós-escrita  

- `load_summary` - Gera resumo dos arquivos carregados  
  - parâmetros: `(layers: dict)`  
  - exemplo: auditoria de dados por camada  