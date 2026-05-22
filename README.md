# Fila de Mensagens com RabbitMQ

Este projeto é um exemplo simples de como implementar uma fila de mensagens usando RabbitMQ em Node.js. Ele inclui um produtor que envia mensagens para a fila e um consumidor que recebe e processa essas mensagens.

## Como Rodar o Projeto

Com docker, basta rodar o comando:

```bash
docker compose up -d --build
```

- A interface do RabbitMQ estará disponível em `http://localhost:15672` com as credenciais padrão (usuário: `guest`, senha: `guest`).
- A aplicação Node.js estará rodando em `http://localhost:5000`.
