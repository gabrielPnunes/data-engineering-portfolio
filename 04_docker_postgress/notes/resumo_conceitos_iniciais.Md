# Docker Conceitos Iniciais

## O que é Docker

Ferramenta que tem como proposito rodar aplicações em ambientes isolados, chamados containers

- Vem todas as configurações necessárias para rodar na máquina, precisando apenas ter o docker no PC
- Container é Diferente de Máquina virtual, pois ela usa o conceito de virtuzalição

---

## dockerfiles

- É onde fica todas as informações necessárias para se gerar as imagens

---

## Imagem

- É onde está todas todas as configurações para se rodar o container
- É como se fosse uma print do ambiente onde eu estava rodando
- Por ser uma "print" é estatica

---

## Container

- O container é uma instancia da imagem, é a image sendo rodada
- É como se a print virasse um vídeo
- Pode ser rodado em uma VM ou na maquina fisica msm

---

## Registros

- São onde ficam armazenadas as imagens
- É como se fosse o "Google Fotos"
- Cada empresa tem seu "hub" o armazenamento e etc




📌 Você não instala Postgres localmente
Você roda um container com Postgres dentro.

🔹 Por que isso é essencial para dados

No mundo real:

Dev roda banco local em Docker

Pipeline conecta em container

Time inteiro usa mesma versão

Zero “setup manual”

📌 Engenharia de Dados = ambientes reproduzíveis.

2️⃣ PARTE PRÁTICA DO DIA (ESSENCIAL)

🎯 Objetivo do dia

Subir PostgreSQL via Docker

Conectar usando Python

Executar um comando simples

Nada além disso.

2.1️⃣ Subindo PostgreSQL com Docker
📌 Pré-requisito

Docker instalado

Docker Desktop rodando

🐘 Comando para subir o PostgreSQL
docker run -d \
  --name postgres_dev \
  -e POSTGRES_USER=admin \
  -e POSTGRES_PASSWORD=admin123 \
  -e POSTGRES_DB=meubanco \
  -p 5432:5432 \
  postgres:15

🔍 Explicação linha a linha

docker run → cria e roda container

-d → modo background

--name postgres_dev → nome do container

POSTGRES_USER → usuário do banco

POSTGRES_PASSWORD → senha

POSTGRES_DB → banco inicial

-p 5432:5432

porta do container → porta local

postgres:15 → imagem oficial + versão

✅ Verificando se subiu
docker ps


Você deve ver algo como:

postgres_dev   postgres:15   Up

2.2️⃣ Conectando no PostgreSQL via Python
📦 Instalar dependência
pip install psycopg2-binary

🐍 Código Python de conexão
import psycopg2

conn = psycopg2.connect(
    host="localhost",
    port=5432,
    database="meubanco",
    user="admin",
    password="admin123"
)

cursor = conn.cursor()
cursor.execute("SELECT 1;")

resultado = cursor.fetchone()
print(resultado)

cursor.close()
conn.close()

🔍 Explicação passo a passo
import psycopg2


Driver PostgreSQL para Python.

psycopg2.connect(...)


Cria conexão com o banco no container.

📌 localhost porque a porta foi exposta (-p 5432:5432).

cursor = conn.cursor()


Cursor = ponte para executar SQL.

cursor.execute("SELECT 1;")


Query simples para testar conexão.

cursor.fetchone()


Retorna o resultado da query → (1,)

2.3️⃣ Teste extra (opcional, mas real)
cursor.execute("""
CREATE TABLE IF NOT EXISTS teste (
    id SERIAL PRIMARY KEY,
    nome TEXT
);
""")
conn.commit()