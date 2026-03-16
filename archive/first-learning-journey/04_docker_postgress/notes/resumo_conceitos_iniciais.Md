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
