# Week 07 - Day 02 Pipeline Python com Variáveis de Ambiente

---

## Objetivo

- Criar um Dockerfile para execução de um pipeline Python
- Utilizar imagem leve (`alpine`) e comparar com o (`slim`)
- Trabalhar com variáveis de ambiente
- Melhorar portabilidade e reprodutibilidade
- Aplicar boas práticas com `.dockerignore`

---

## Conceitos 

### dockerignore
- Evita arquivos desnecessários na imagem
- Reduz tamanho do container
- Melhora performance do build

---

## Alpine vs slim
#### Alpine

- Imagem extremamente leve (~5MB)
- Mais rápida para download
- Usa `musl libc` (pode gerar incompatibilidades)
- Ideal para produção simples

#### Slim

- Mais pesada (~50MB+)
- Usa `glibc` (mais compatível)
- Menos problemas com bibliotecas
- Melhor para projetos com dependências complexas
