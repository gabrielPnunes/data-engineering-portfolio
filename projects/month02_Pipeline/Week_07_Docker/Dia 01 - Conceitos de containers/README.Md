# Week 07 - Day 01 (Docker - Conceitos de Containers)

---

## Objetivo

- Introduzir conceitos de containerização com Docker
- Criar um ambiente isolado para execução de código Python
- Garantir reprodutibilidade do pipeline
- Executar uma aplicação dentro de um container

---

## Conceitos Fundamentais

### Container

- Ambiente isolado e leve
- Empacota código + dependências + runtime
- Resolve o problema: "na minha máquina funciona"



### Imagem

- Template imutável usado para criar containers


### Dockerfile

- Arquivo que define como a imagem será construída
- Contém instruções passo a passo do ambiente

---

### Explicação

- `FROM` → define imagem base (Python leve)
- `WORKDIR` → diretório de execução
- `COPY` → copia arquivos para o container
- `RUN` → instala dependências
- `CMD` → comando executado ao iniciar


## Dependências

```txt
pandas==2.2.2
requests==2.31.0
```

---

## Script (`main.py`)

### O que ele faz:

- Mostra informações do ambiente do container:

  - Python
  - Sistema operacional
  - Versão do pandas

- Cria um DataFrame simples

- Exibe os dados no terminal

---

## Execução do Projeto

### 1. Build da imagem

```bash
docker build -t delivery-pipeline .
```

- Descrição:

  - `docker build` → constrói a imagem Docker
  - `-t delivery-pipeline` → nome da imagem
  - `.` → usa o diretório atual como contexto

  ![Docker Build](/assets/docker//docker_build.png)

---

### 2. Executar o container

```bash
docker run --rm delivery-pipeline
```

- Descrição:

  - `docker run` → executa um container
  - `--rm` → remove o container após execução
  - `delivery-pipeline` → nome da imagem

  ![Docker Run](/assets/docker/docker_run.png)

---

### 3. Listar imagens

```bash
docker images
```

- Descrição:

  - Lista todas as imagens disponíveis na máquina
  - Mostra tamanho, ID e nome

![Docker Run](/assets/docker/docker_image.png)

---

### 4. Listar containers

```bash
docker ps -a
```

- Descrição:

  - Lista todos os containers (ativos e finalizados)
  - Permite ver histórico de execuções
  
  ![Docker Run](/assets/docker/docker_ps_a.png)
