# Sistema Distribuído com Sockets TCP em Python

Sistema de consulta de nós ativos composto por um servidor central e múltiplos
clientes que se comunicam exclusivamente via sockets TCP.

---

## Estrutura dos arquivos

```
server.py        Servidor central
client.py        Nó cliente (interativo ou automático)
test_system.py   Suíte de testes (unitários + integração)
README.md        Este arquivo
```

---

## Requisitos

- Python 3.10+
- Apenas bibliotecas padrão (`socket`, `threading`, `time`, `logging`, `unittest`)

---

## Como executar

### 1. Iniciar o servidor

```bash
python server.py
```

O servidor escuta na porta **9000** em todas as interfaces (`0.0.0.0`).
Para alterar host/porta, edite as constantes `HOST` e `PORT` no topo do arquivo.

### 2. Conectar um cliente (modo interativo)

```bash
python client.py node1
```

Exibe um menu com as operações disponíveis:

```
╔══════════════════════════════════════╗
║   Sistema Distribuído — Cliente      ║
╠══════════════════════════════════════╣
║  1. Registrar nó                     ║
║  2. Enviar heartbeat                 ║
║  3. Listar nós ativos                ║
║  4. Desconectar (envia QUIT)         ║
║  0. Sair sem avisar o servidor       ║
╚══════════════════════════════════════╝
```

### 3. Modo demo (heartbeats automáticos)

Útil para testar o sistema com vários terminais simultâneos.
O nó se registra, envia heartbeats a cada 3 s por 30 s e depois desconecta:

```bash
python client.py node2 --demo
python client.py node3 --demo   # outro terminal
python client.py node4 --demo   # outro terminal
```

### 4. Conectar a servidor remoto

```bash
python client.py node1 192.168.1.10 9000
python client.py node1 192.168.1.10 9000 --demo
```

### 5. Rodar os testes

```bash
python test_system.py
```

Saída esperada:

```
Ran 26 tests in ~0.5s
OK
```

---

## Protocolo de comunicação

### Camada de transporte

| Atributo     | Valor                                   |
|--------------|-----------------------------------------|
| Protocolo    | TCP/IP                                  |
| Porta padrão | 9000                                    |
| Encoding     | UTF-8                                   |
| Framing      | Mensagens separadas por `\n`            |
| Serialização | Texto puro no formato `CHAVE:valor`     |

**Sobre a escolha de serialização**: formato texto puro foi preferido a JSON,
pickle ou struct por ser diretamente legível, depurável com `netcat`/`telnet`
sem ferramentas extras, e mais que suficiente para o tipo e volume de dados
trocados (strings simples). Não requer nenhuma biblioteca além da stdlib.

### Comandos e respostas

| Comando cliente       | Resposta do servidor             | Condição                     |
|-----------------------|----------------------------------|------------------------------|
| `REGISTER:<node_id>`  | `OK:REGISTERED`                  | Sucesso                      |
| `REGISTER:<node_id>`  | `ERROR:ALREADY_REGISTERED`       | Nó já existe                 |
| `REGISTER:<node_id>`  | `ERROR:MISSING_NODE_ID`          | ID vazio                     |
| `HEARTBEAT:<node_id>` | `OK:HEARTBEAT`                   | Sucesso                      |
| `HEARTBEAT:<node_id>` | `ERROR:NOT_REGISTERED`           | Nó não registrado            |
| `HEARTBEAT:<node_id>` | `ERROR:MISSING_NODE_ID`          | ID vazio                     |
| `LIST`                | `NODES:<id1>,<id2>,...`          | Lista nós ativos (pode ser vazia: `NODES:`) |
| `QUIT:<node_id>`      | `OK:BYE`                         | Desconexão limpa             |
| *(inválido)*          | `ERROR:UNKNOWN_COMMAND`          | Comando não reconhecido      |
| *(linha vazia)*       | `ERROR:EMPTY_COMMAND`            | Mensagem vazia               |

### Exemplo de sessão (netcat)

```
$ nc localhost 9000
REGISTER:nodeA
OK:REGISTERED
HEARTBEAT:nodeA
OK:HEARTBEAT
LIST
NODES:nodeA
QUIT:nodeA
OK:BYE
```

---

## Regras de negócio

### Registro (`REGISTER:<node_id>`)
- Armazena o nó em memória com o timestamp atual como último heartbeat.
- Rejeita IDs duplicados com `ERROR:ALREADY_REGISTERED`.

### Heartbeat (`HEARTBEAT:<node_id>`)
- Atualiza o timestamp do último heartbeat do nó.
- Nó deve estar registrado; caso contrário retorna `ERROR:NOT_REGISTERED`.

### Consulta (`LIST`)
- Retorna apenas nós cujo último heartbeat ocorreu há **menos de 10 segundos**.
- Nós expirados são silenciosamente excluídos da resposta.

### Desconexão (`QUIT:<node_id>`)
- Remove o nó da memória imediatamente.
- Encerra a conexão TCP com aquele cliente.

### Expiração automática de nós
- Uma thread *cleaner* (daemon) varre os nós a cada **5 segundos** e remove
  aqueles com heartbeat há mais de **10 segundos** (`NODE_TIMEOUT`).
- Um nó expirado não aparece em `LIST` mesmo antes de ser removido pelo cleaner.

### Robustez a falhas
- **Queda abrupta do cliente** (sem `QUIT`): o bloco `finally` do handler
  remove o nó automaticamente, evitando "nós fantasma" na memória.
- **Conexão recusada**: o cliente exibe mensagem de erro descritiva.
- **Comando inválido**: o servidor retorna `ERROR:UNKNOWN_COMMAND` e mantém
  a conexão aberta — o cliente pode continuar enviando comandos.

---

## Arquitetura e decisões de design

### Servidor

```
main()
 ├── Thread cleaner (daemon)          — remove nós expirados a cada 5 s
 └── Loop accept()
      └── Thread handle_client()      — uma thread por cliente conectado
           └── process_message()      — lógica de protocolo pura (sem I/O)
```

- **Concorrência**: modelo *thread-per-connection*. Simples e adequado para
  o cenário de poucos nós distribuídos. Para escala muito alta, select/epoll
  seria preferível.
- **Estado compartilhado**: dicionário `nodes` protegido por `threading.Lock`
  em todas as leituras e escritas.
- **`process_message()` isolada**: sem dependência de socket, permite testes
  unitários determinísticos sem infraestrutura de rede.

### Cliente

```
NodeClient
 ├── connect() / close()              — gerência do socket TCP
 ├── _send()                          — primitiva thread-safe (Lock interno)
 ├── register() / heartbeat()         — comandos do protocolo
 ├── list_nodes()                     — retorna list[str]
 └── quit()                           — envia QUIT e fecha o socket
```

- **`_sock_lock`**: protege o socket contra uso simultâneo da thread de
  heartbeat automático e do loop principal no modo demo.

### Testes (`test_system.py`)

| Classe            | Escopo               | Nº de testes |
|-------------------|----------------------|--------------|
| `TestProtocol`    | Unitário (sem rede)  | 13           |
| `TestIntegration` | Integração (socket)  | 13           |
| **Total**         |                      | **26**       |

Casos cobertos: registro, heartbeat, lista, expiração, queda abrupta,
concorrência (10 threads), thread-safety do cliente, duplicatas, comandos
inválidos e mensagens vazias.
