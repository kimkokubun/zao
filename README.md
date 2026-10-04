# flight-fanout

Projeto Go que consome um cadastro de voo (mensagem versionada) do Kafka, faz fan-out de **1 mensagem por passageiro** e publica no tópico de saída com o passageiro como pai e o voo aninhado como filho.

## Fluxo

```
cmd/seed  -->  flight-registrations  -->  cmd/consumer  -->  passenger-flights
```

## Pré-requisitos

- Go 1.22+
- Docker / Docker Compose

## Subir o Kafka

```bash
docker compose up -d
```

Kafka fica em `localhost:9092`.

## Rodar o consumer

```bash
go run ./cmd/consumer
```

## Publicar mensagem de exemplo

Em outro terminal:

```bash
go run ./cmd/seed
```

O seed envia um cadastro do voo `LA3090` com vários passageiros. O consumer gera uma mensagem por passageiro em `passenger-flights`.

## Contratos

Envelope comum:

```json
{ "schema_version": "1.0", "payload": { ... } }
```

Entrada (`flight-registrations`): voo + lista de passageiros.

Saída (`passenger-flights`): um JSON por passageiro, com `flight` aninhado.

Versão desconhecida ou JSON inválido: log + skip (offset avançado para não travar o consumer).

## Variáveis de ambiente (opcional)

| Variável | Default |
|---|---|
| `KAFKA_BROKERS` | `localhost:9092` |
| `KAFKA_INPUT_TOPIC` | `flight-registrations` |
| `KAFKA_OUTPUT_TOPIC` | `passenger-flights` |
| `KAFKA_CONSUMER_GROUP` | `flight-fanout` |

## Testes

```bash
go test ./...
```
