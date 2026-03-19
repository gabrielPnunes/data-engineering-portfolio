# Funções em Python

## Revisão Rápida

Chamada da função → parâmetros → processamento → return → fim

## O que é uma Função

- Uma função é um loco de código reutilizável
- Executa uma tarefa específica
- Pode receber dados (parâmetros)
- Pode devolver um resultado (return)
- Evitar repetição de código

---

## Definição de Funções (`def`)

Sintaxe básica:
def nome_da_funcao(parametros):
    corpo_da_funcao

Exemplo:
def soma(a, b):
    return a + b

### Partes da Função

- `def` → palavra-chave que define uma função
- `nome_da_funcao` → identificador da função
- `(parametros)` → valores de entrada
- `:` → inicia o bloco da função
- corpo da função → código identado
- `return` → valor devolvido (opcional)

### Parâmetros

- Variáveis declaradas na função

Exemplo:
def multiplicar(x, y):
    return x * y

- x e y → parâmetros

### Argumentos

- Valores passados na chamada

Exemplo:
multiplicar(2, 3)

- 2 e 3 → argumentos

### Retorno (`return`)

- Devolve um valor para quem chamou a função
- Encerra a execução da função

Exemplo:
def dobro(n):
    return n * 2

Se não houver `return`:

- A função retorna `None`

---

## Escopo em Funções

- Variáveis criadas dentro da função são locais
- Não existem fora da função

Exemplo:
def teste():
    x = 10
x não existe fora da função por tanto irá ocorrer o erro

---

## ⚙️ Parâmetros Padrão

Permitem valor default.

Exemplo:
def saudacao(nome="Mundo"):
    print(f"Olá, {nome}")

---

## Funções como Objetos

Funções podem:

- Ser atribuídas a variáveis
- Ser passadas como argumento
- Ser retornadas

Exemplo:
def operacao(f, a, b):
    return f(a, b)
