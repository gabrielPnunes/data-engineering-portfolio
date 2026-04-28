# Week 06 - Day 02 (Modularização e Orquestração do Pipeline)

---

## Objetivo

- Modularizar completamente o pipeline  
- Criar um ponto único de execução (`run`)  
- Separar responsabilidades entre módulos  
- Melhorar organização e reusabilidade do código  
- Aproximar o projeto de padrões de produção  

---

## Arquitetura implementada

O pipeline agora é executado a partir de um único gatilho de código:

```Py
from pipeline import run

if __name__ == "__main__":
    run()
```