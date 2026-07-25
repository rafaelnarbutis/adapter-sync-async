# 🔄 Adapter - Sincronização Síncrono/Assíncrono

## 📋 Visão Geral

Este projeto é uma **prova de conceito (PoC)** que demonstra a integração de um **fluxo síncrono com um fluxo assíncrono**. A arquitetura utiliza padrões de adaptador para conectar uma aplicação síncrona com operações assíncronas através do Apache Kafka.

### 🎯 Objetivo

Validar e demonstrar como integrar dois paradigmas de processamento:
- ✅ **Síncrono**: API REST (Java/Go) com resposta imediata
- ⏳ **Assíncrono**: Event-driven processing (Kafka) para processamento em background

---

## 🗂️ Estrutura do Projeto

```
adapter/
├── demo/              # 🟢 Aplicação Spring Boot (Fluxo Síncrono)
│   ├── src/
│   │   ├── main/
│   │   │   ├── java/        # Controller, Producer, Listener
│   │   │   └── resources/   # application.yaml
│   │   └── test/            # Testes unitários
│   └── pom.xml              # Dependências Maven
│
├── poc-go/            # 🔵 Aplicação Go (Fluxo Síncrono)
│   ├── main.go              # Entry point
│   ├── adapter/             # Adaptadores (Gin, Kafka)
│   ├── model/               # RequestManager
│   ├── service/             # Processadores de eventos
│   └── go.mod               # Dependências Go
│
├── infra/             # 🐳 Infraestrutura
│   └── compose.yml          # Docker Compose (Kafka, etc)
│ 
├── loadtest.js             # Script de testes de carga
│
└── README.md
```

---

## 🛠️ Ferramentas Necessárias

### Requisitos Obrigatórios

| Ferramenta | Versão | Descrição |
|-----------|--------|-----------|
| ☕ **Java JDK** | 21+ | Runtime para a aplicação Spring Boot |
| 📦 **Maven** | 3.6+ | Gerenciador de dependências e build Java |
| 🐹 **Go** | 1.18+ | Linguagem para a aplicação assíncrona |
| 🐳 **Docker** | 20.10+ | Containerização da infraestrutura |
| 🐋 **Docker Compose** | 1.29+ | Orquestração de containers |
| 📨 **Apache Kafka** | 2.8+ | Message broker (via Docker Compose) |

### Ferramentas Adicionais

| Ferramenta | Descrição |
|-----------|-----------|
| 📊 **k6** | Runtime para executar `loadtest.js` |
| 🔧 **git** | Controle de versão |
| 📝 **cURL/Postman** | Cliente HTTP para testar endpoints |

---

## 🚀 Primeiros Passos

### 1️⃣ Clonar o Repositório
```bash
git clone https://github.com/rafaelnarbutis/adapter-sync-async.git
cd adapter-sync-async
```

### 2️⃣ Configurar a Infraestrutura
```bash
cd infra
docker-compose up -d
# Aguarde o Kafka e dependências iniciarem

# Crie o topico kafka na interface ou via commnado
topic-name = command
```

### 3️⃣ Executar a Aplicação Rest
```bash
cd demo
mvn clean install
mvn spring-boot:run

ou

cd poc-go
go mod download
go run main.go
```


### 4️⃣ Executar Testes de Carga
```bash
## install k6 - choco install k6 (wind) || brew install k6 (mac) || apt-get install k6 (deb)
k6 run loadtest.js
```

---

## 📊 Fluxo de Funcionamento

```
┌─────────────────────────────┐
│   Cliente HTTP              │
│   (POST /api/request)       │
└────────────────┬────────────┘
                 │
                 ▼
         ┌───────────────────┐
         │ Java / Go App     │ ⚡ SÍNCRONO
         │                   │ ✅ Resposta Imediata
         └────────┬──────────┘
                  │
                  ▼ (Publica Evento)
         ┌───────────────────┐
         │  Apache Kafka     │
         │  (Message Broker) │
         └────────┬──────────┘
                  │
                  ▼ (Consome e retorna Evento)
         ┌───────────────────┐
         │ Operacao Assync   │ ⏳ ASSÍNCRONO
         │                   │ 📨 Processamento em Background
         └───────────────────┘

```

---

## 📝 Componentes Principais

### 🟢 Java Spring Boot (demo/)
- **Controller.java**: Expõe endpoints REST
- **Producer.java**: Publica eventos no Kafka
- **Listener.java**: Consome respostas do Kafka
- **DemoApplication.java**: Inicialização da aplicação

### 🔵 Go Application (poc-go/)
- **main.go**: Entry point da aplicação
- **gin_adapter.go**: Servidor HTTP (Gin)
- **kafka_adapter.go**: Cliente Kafka
- **RequestManager.go**: Gerenciamento de requisições
- **processor_service.go**: Lógica de processamento

---

## 🧪 Testando a Integração

### Testar Endpoint Síncrono
```bash
curl -X POST http://localhost:8081/v1/simulate \
  -H "Content-Type: application/json" \
  -H "X-Correlation-ID: 123" \
  -d '{"data": "test"}'
```

## Testes com K6 - loadtest.js

### Java -- Spring + Kafka
```bash
HTTP
    http_req_duration..............: avg=14.8ms min=1.86ms med=14.16ms max=34.01ms p(90)=22.16ms p(95)=23.17ms
      { expected_response:true }...: avg=14.8ms min=1.86ms med=14.16ms max=34.01ms p(90)=22.16ms p(95)=23.17ms
    http_req_failed................: 0.00%  0 out of 8280
    http_reqs......................: 8280   45.79722/s

    EXECUTION
    iteration_duration.............: avg=1.01s  min=1s     med=1.01s   max=1.03s   p(90)=1.02s   p(95)=1.02s  
    iterations.....................: 8280   45.79722/s
    vus............................: 2      min=1         max=99 
    vus_max........................: 100    min=100       max=100

    NETWORK
    data_received..................: 994 kB 5.5 kB/s
    data_sent......................: 2.2 MB 12 kB/s
```

### Go -- Gin + Kafka
```bash
HTTP
    http_req_duration..............: avg=864.9ms min=101.74ms med=883.86ms max=1.74s p(90)=1.6s p(95)=1.66s
      { expected_response:true }...: avg=864.9ms min=101.74ms med=883.86ms max=1.74s p(90)=1.6s p(95)=1.66s
    http_req_failed................: 0.00%  0 out of 4543
    http_reqs......................: 4543   25.141046/s

    EXECUTION
    iteration_duration.............: avg=1.86s   min=1.1s     med=1.88s    max=2.75s p(90)=2.6s p(95)=2.66s
    iterations.....................: 4543   25.141046/s
    vus............................: 7      min=1         max=99 
    vus_max........................: 100    min=100       max=100

    NETWORK
    data_received..................: 759 kB 4.2 kB/s
    data_sent......................: 1.2 MB 6.6 kB/s
```
---

## 🐛 Troubleshooting

| Problema | Solução |
|----------|---------|
| ❌ Porta 8080 já em uso | Alterar em `application.yaml` |
| ❌ Kafka não conecta | Aguardar 30s após `docker-compose up` |
| ❌ Erro Maven build | Executar `mvn clean install -U` |
| ❌ Go mod error | Executar `go mod tidy` |

---

## 📚 Documentação Adicional

- [Spring Boot Docs](https://spring.io/projects/spring-boot)
- [Go Docs](https://golang.org/doc/)
- [Apache Kafka Docs](https://kafka.apache.org/documentation/)
- [Gin Web Framework](https://gin-gonic.com/)
- [k6 - install](https://grafana.com/docs/k6/latest/set-up/install-k6/)
- [k6 - quickstart](https://grafana.com/docs/k6/latest/get-started/write-your-first-test/)

---

## 📄 Licença

Este projeto é uma prova de conceito.

---

**Desenvolvido com ❤️ para demonstrar padrões de integração síncrono/assíncrono**
