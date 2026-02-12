# ==============================
# BIBLIOTECAS
# ==============================
import sqlite3
import os

# =====================================================
# FUNÇÕES
# =====================================================
def separador():
    print("=" * 40)
separador()

# =====================================================
# EXERCÍCIO 1 — NOT NULL + UNIQUE
# =====================================================
conn = sqlite3.connect("exercicio1.db")
cursor = conn.cursor()

cursor.execute("DROP TABLE IF EXISTS usuarios")

cursor.execute("""
CREATE TABLE usuarios (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    nome TEXT NOT NULL,
    email TEXT NOT NULL UNIQUE
)
""")

usuarios = [
    ("Gabriel", "gabriel@email.com"),
    ("João", "gabriel@email.com"),
    ("Maria", None)
]

sucesso = 0
erro_count = 0

for usuario in usuarios:
    try:
        cursor.execute(
            "INSERT INTO usuarios (nome, email) VALUES (?, ?)",
            usuario
        )
        conn.commit()
        sucesso += 1
        print(f"[OK]    Inserido -> {usuario}")
    except sqlite3.IntegrityError as erro:
        erro_count += 1
        print(f"[ERRO]  {usuario} -> {erro}")

print("\n RESUMO EXERCÍCIO 1")
print(f"Sucesso: {sucesso}")
print(f"Erros:   {erro_count}")

cursor.execute("SELECT * FROM usuarios")
dados = cursor.fetchall()

print("\n DADOS NA TABELA usuarios")
separador()
for linha in dados:
    print(linha)

conn.close()


# =====================================================
# EXERCÍCIO 2 — CHECK Constraint
# =====================================================
conn = sqlite3.connect("exercicio2.db")
cursor = conn.cursor()

cursor.execute("DROP TABLE IF EXISTS supermercado")

cursor.execute("""
CREATE TABLE supermercado (
    nome_produto TEXT NOT NULL,
    id_produto TEXT NOT NULL PRIMARY KEY,
    preco INTEGER NOT NULL CHECK(preco > 0),
    estoque INTEGER NOT NULL CHECK(estoque >= 0)
)
""")

itens = [
    ("Desinfetante", "1", 53, 9),
    ("Água", "2", 59, 10),
    ("Vidro", "3", -10, 5),
    ("Sabão", "4", 20, -3)
]

sucesso = 0
erro_count = 0

for item in itens:
    try:
        cursor.execute(
            "INSERT INTO supermercado (nome_produto, id_produto, preco, estoque) VALUES (?, ?, ?, ?)",
            item
        )
        conn.commit()
        sucesso += 1
        print(f"[OK]    Inserido -> {item}")
    except sqlite3.IntegrityError as erro:
        erro_count += 1
        print(f"[ERRO]  {item} -> {erro}")

print("\nRESUMO EXERCÍCIO 2")
print(f"Sucesso: {sucesso}")
print(f"Erros:   {erro_count}")

cursor.execute("SELECT * FROM supermercado")
dados = cursor.fetchall()

print("\nDADOS NA TABELA supermercado")
separador()
for linha in dados:
    print(linha)

conn.close()

separador()
print("EXECUÇÃO FINALIZADA")
separador()

# =====================================================
# Exercício 3 — FOREIGN KEY
# =====================================================
conn = sqlite3.connect("exercicio3.db")
cursor = conn.cursor()


cursor.execute("PRAGMA foreign_keys = ON")

cursor.execute("DROP TABLE IF EXISTS pedido")
cursor.execute("DROP TABLE IF EXISTS cliente")


cursor.execute("""
CREATE TABLE IF NOT EXISTS cliente (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    nome TEXT NOT NULL
)
""")


cursor.execute("""
CREATE TABLE IF NOT EXISTS pedido (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    id_cliente INTEGER NOT NULL,
    descricao TEXT NOT NULL,
    FOREIGN KEY (id_cliente) REFERENCES cliente(id)
)
""")


cursor.execute("INSERT INTO cliente (nome) VALUES (?)", ("Gabriel",))


try:
    cursor.execute(
        "INSERT INTO pedido (id_cliente, descricao) VALUES (?, ?)",
        (1, "Compra de notebook")
    )
    conn.commit()
    print("[OK] Pedido válido inserido")
except sqlite3.IntegrityError as erro:
    print("[ERRO]", erro)


try:
    cursor.execute(
        "INSERT INTO pedido (id_cliente, descricao) VALUES (?, ?)",
        (99, "Compra inválida")
    )
    conn.commit()
    print("[OK] Pedido inválido inserido")
except sqlite3.IntegrityError as erro:
    print("[ERRO] Violação de FK ->", erro)

conn.close()


