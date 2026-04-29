# Week 06 - Day 02 (Modularização e Orquestração do Pipeline)

---

## Objetivo

- Modularizar completamente o pipeline  
- Criar um ponto único de execução (`run`)  
- Separar responsabilidades entre módulos  
- Melhorar organização e reusabilidade do código  
- Aproximar o projeto de padrões de produção  

---

O pipeline agora é executado a partir de um único entrypoint:

```python
from pipeline import run

if __name__ == "__main__":
    run()
```