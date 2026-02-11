# Semana 5 Dia 1

---

## Orquestração de Pipelines - Conceitos de DAG

---

### O que é um DAG

- É basicamente uma ordem de execução em formato de grafo aonde tem, dependências, paralelismo e pontos de falhas
- Dpedencias: Sendo o que precisa temrinar antes de algo para continuar
- Paralelismo: O que pode rodar ao mesmo tempo
- Pontos de Falhas: Onde o pipeline pode quebrar e cmo reagir
- Um dag é um (Directed Acyclic Graph)
- Usado para orquestração de dados

---

### Uso Real

- Em uso geral ele geralmente segue o seguiente esquema
- Ingestão, Validação, Transformação, Carga e Monitoramento

- Exemplo de sua estrutura

            ingestao_dados
                   |
            validacao_dados
                   |
        -------------------------
        |                       |
  transformacao_a        transformacao_b
        |                       |
        ----------- carga --------
                   |
               monitoramento

- Ingestao_dados: Busca dados (API, banco, S3, Kafka, etc.)
- validacao_dados: Checa os Schemas, Verifica Nuloes, Tipos e Ranges, Pode abotar o DAG se algo estiver errado
- transformacao_a: Normalização, Limpeza, Padronização de Colunas
- transformacao_b: Agregações, Métricas, Enriquecimento
- carga: Grava no Data Warehouse /  Lakehouse
- Monitoramento: Alerta no Slack / Email sobre as métricas de sucesso ou falha

Válido Resaltar que essas duas Transformações rodam em Paralelo, pois não dependem uma da outra
