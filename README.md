# Desafio CI&amp;T

Exercício de programação com container (docker) da CI&T de Campinas para avaliar candidatos.

## Instalação

* clonar o repositório em um diretório
* Baixar o container e os dados da CI&T com o seguinte repositorio: https://github.com/cmiranda-ciandt/magic-cards
* Instalar o python 2.7 e as seguintes bibliotecas python:
__Essas preferencialmente do seu repositório de distribuição, se usar Linux:__
- Flask
- unidecode
- pika (para o RabbitMQ)
- pymysql
- simplejson
- csv
__E instalar o celery para as tarefas em background pelo pip:
```
pip install celery --user
```
------
O celery deve ser preferencialmente instalado pelo pip como usuário para ele respeitar preferências locais. Caso você instale do repositório, ele se acopla ao systemd e ficar com configurações em lugares diferentes. Não esqueça de ter o diretório `~/.local/bin` no seu PATH.
------

## Como executar
Você precisará executar o `desafiocti.py` no contexto do Flask, ou seja:
```
FLASK_APP=./desafiocti.py flask run
```
E além disso precisará também rodar o celery para ele pegar as tarefas em background:
```
celery worker  --loglevel=warn
```
A configuração do celery já consta do aplicativo.
