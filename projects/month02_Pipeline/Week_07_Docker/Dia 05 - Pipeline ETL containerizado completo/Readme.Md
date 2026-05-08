# Week 07 - Day 05 Pipeline ETL Containerizado Completo

---

## Objetivo

- Consolidar tudo da semana em um pipeline ETL completo
- Integrar extract, transform e load dentro do container
- Persistir dados em Parquet e PostgreSQL simultaneamente
- Adicionar retry automático e monitoramento via Adminer

---

## Comandos

### `restart: on-failure`

```yaml
pipeline:
  restart: on-failure
```

- Reinicia o container automaticamente se o pipeline falhar
- Útil quando o banco ainda não está pronto ao iniciar

---

### Adminer

```yaml
adminer:
  image: adminer
  ports:
    - "8080:8080"
  depends_on:
    - db
```

- Interface web para visualizar e consultar o banco
- Acesse `http://localhost:8080` após subir os serviços