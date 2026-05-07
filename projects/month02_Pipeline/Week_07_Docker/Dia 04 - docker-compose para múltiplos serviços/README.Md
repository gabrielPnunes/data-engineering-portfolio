# Week 07 - Day 04 Docker Compose com Pipeline + PostgreSQL

---

## Objetivo

- Executar múltiplos serviços com Docker Compose
- Integrar pipeline Python com PostgreSQL
- Automatizar dependências entre containers
- Persistir dados transformados no banco
- Simular arquitetura real de engenharia de dados

---

## Conceitos

### Docker Compose

- Ferramenta para orquestrar múltiplos containers
- Usa um único arquivo (`docker-compose.yml`)
- Automatiza:
  - build
  - execução
  - rede
  - volumes
  - variáveis de ambiente

---

### Serviço (`service`)

Cada container definido no compose representa um serviço.

Neste projeto:
- `db` → PostgreSQL
- `pipeline` → aplicação ETL Python

---

## docker-compose.yml

```yaml
services:

  db:
    image: postgres:15-alpine

    environment:
      POSTGRES_USER:     pipeline
      POSTGRES_PASSWORD: pipeline123
      POSTGRES_DB:       deliveries

    ports:
      - "5432:5432"

    volumes:
      - pgdata:/var/lib/postgresql/data

    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U pipeline"]
      interval: 5s
      timeout: 5s
      retries: 5

  pipeline:
    build: .

    env_file: .env

    volumes:
      - ./data/raw:/data/raw
      - ./data/processed:/data/processed

    depends_on:
      db:
        condition: service_healthy

volumes:
  pgdata:
```

---

## Explicação

### Serviço `db`

- Cria container PostgreSQL
- Usa imagem leve (`alpine`)
- Expõe porta `5432`
- Possui volume persistente

---

### Healthcheck

```yaml
healthcheck:
  test: ["CMD-SHELL", "pg_isready -U pipeline"]
```

- Verifica se PostgreSQL está pronto
- Evita erro de conexão do pipeline

---

### Serviço `pipeline`

- Executa aplicação ETL
- Usa variáveis do `.env`
- Compartilha dados via volumes

---



## Comandos Docker Compose

### 1. Subir todos os serviços

```bash
docker-compose up --build
```

- Descrição:

  - `up` → sobe todos os containers
  - `--build` → reconstrói imagens antes da execução
  - Exibe logs em tempo real

  ![Docker Build](/assets/docker/docker_compose_up_1.png)

    ![Docker Build](/assets/docker/docker_compose_up_2.png)

    ![Docker Build](/assets/docker/docker_compose_up_3.png)

    ![Docker Build](/assets/docker/docker_compose_up_4.png)
---

### 2. Subir em background

```bash
docker-compose up --build -d
```

- Descrição:

  - `-d` → executa em modo detached (background)
  - Terminal fica livre após iniciar containers
---

### 3. Ver logs do pipeline

```bash
docker-compose logs -f pipeline
```

- Descrição:

  - `logs` → exibe logs do serviço
  - `-f` → acompanha logs em tempo real
  - `pipeline` → nome do serviço definido no compose

    ![Docker Build](/assets/docker/docker_compose_logs.png)
---

### 5. Derrubar e remover volumes

```bash
docker-compose down -v
```

- Descrição:

  - Remove containers
  - Remove volumes persistentes
  - Apaga dados salvos do PostgreSQL

    ![Docker Build](/assets/docker/docker_down.png)

---