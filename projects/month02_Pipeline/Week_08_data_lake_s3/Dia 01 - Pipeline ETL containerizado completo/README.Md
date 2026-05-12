# Week 08 - Dia 01 Conceitos de Data Lake

---

## Objetivo

- Introduzir conceitos de Data Lake
- Implementar arquitetura Bronze, Silver e Gold
- Organizar dados em múltiplas camadas
- Separar dados brutos, tratados e analíticos
- Criar estrutura escalável para engenharia de dados

---

## Conceitos

### Data Lake

- Repositório centralizado para armazenamento de dados
- Aceita:
  - dados brutos
  - semi estruturados
  - estruturados

- Escalável e flexível
- Base para pipelines modernos

---

### Bronze Layer

- Camada de dados brutos
- Sem transformações
- Mantém fidelidade ao dado original

✔ Objetivo:
- auditoria
- reprocessamento
- histórico

---

### Silver Layer

- Dados limpos e padronizados
- Aplicação de:
  - filtros
  - casts
  - joins
  - tratamento de nulos

✔ Objetivo:
- preparação para análise

---

### Gold Layer

- Dados agregados
- Prontos para BI e dashboards
- Métricas consolidadas

✔ Objetivo:
- consumo analítico

---