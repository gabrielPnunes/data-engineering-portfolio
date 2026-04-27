# Week 06 - Day 01 (Project Structure)

---

## Objetivo

- Integrar todas as etapas do pipeline (EXTRACT → TRANSFORM → LOAD)  
- Tornar o pipeline dinâmico e escalável  
- Reduzir hardcodes no código  
- Preparar base para analytics  

---

### Paths

- `RAW_PATH` → origem dos dados brutos  
- `PROCESSED_PATH` → dados tratados  
- `ANALYTICS_PATH` → camada analítica   

---

## Queries e funções utilizadas

### EXTRACT

- `extract_all_files` - Leitura dinâmica baseada no `FILES`  
- `check_duplicates` - Validação de chave  
- `check_date_range` - Validação temporal  

---

### TRANSFORM (100% baseada no config)

- `cast_types` → usa `TRANSFORM_CONFIG["types"]`  
- `filter_rows` → usa `TRANSFORM_CONFIG["filters"]`  
- `fill_nulls` → usa `TRANSFORM_CONFIG["fill_nulls"]`  
- `add_columns` → usa `TRANSFORM_CONFIG["derived_cols"]`  
- `joins` → joins para enriquecimento  

---

### LOAD

- `load_layer` → organização por camada  
- `load_summary` → auditoria dos dados  

---