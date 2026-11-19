# IMTS — Instant Message Trading System

Projeto da disciplina de Sistemas Distribuídos. É um sistema de troca de mensagens no estilo "canais" (publicação/inscrição), com vários servidores replicados, bots clientes, serviço de referência, eleição de coordenador, relógios lógico/físico e replicação de dados.

Tudo roda em containers Docker, orquestrados pelo `docker-compose.yml`.

## Sumário

1. [Estrutura do repositório](#1-estrutura-do-repositório)
2. [Introdução e escolhas (Parte 1)](#2-introdução-e-escolhas)
3. [Troca de mensagens e armazenamento (Parte 2)](#3-troca-de-mensagens-e-armazenamento)
4. [Replicação (Parte 5)](#4-replicação)


---

## 1. Estrutura do repositório

```
.
├── docker-compose.yml
├── IMTS.slnx
├── ClienteCs/          # bot em C# (Cliente.cs, mensagem.proto, Dockerfile)
├── ClientePy/          # bot em Python (cliente.py, mensagem.proto, Dockerfile)
├── ServidorCS/         # servidor em C#
│   ├── Program.cs      #   loop principal (atende clientes)
│   ├── Estado.cs       #   constantes, relógio, estado compartilhado
│   ├── Comunicacao.cs  #   REQ com timeout (referência e outros servidores)
│   ├── Coordenacao.cs  #   eleição, hora do coordenador, replicação, sincronização
│   └── Armazenamento.cs#   SQLite
└── ServidorPy/         # servidor em Python + serviços de infraestrutura
    ├── servidor.py     #   loop principal
    ├── armazenamento.py#   SQLite
    ├── referencia.py   #   serviço de referência
    ├── broker.py       #   broker ROUTER/DEALER
    ├── proxy.py        #   proxy XSUB/XPUB
    └── modulos/        #   estado, relógio, eleição (+ replicação), clientes, referência
```

---
## 2. Introdução e escolhas

### O que o sistema faz

- **Bots** (clientes) fazem login, criam canais, listam canais, se inscrevem em até 3 canais e publicam mensagens (uma por segundo, 10 mensagens por canal escolhido).
- **Servidores** recebem os pedidos dos bots, guardam tudo em disco e publicam cada mensagem no tópico do canal para os inscritos.
- Os servidores se coordenam entre si: pedem **rank** a um serviço de **referência**, elegem um **coordenador** (algoritmo do valentão), sincronizam o **relógio** e **replicam** os dados uns para os outros.

### Escolha da serialização: Protocol Buffers

- Formato binário, compacto, com **esquema único** (`mensagem.proto`) que gera código para Python e C#. Isso evita que cada lado interprete os campos de forma diferente.
- O uso de `oneof` permite um único `Envelope` com vários tipos de corpo (login ou canal), e os tipos de pedido/resposta entre servidores e à referência seguem o mesmo padrão. O receptor descobre o tipo da mensagem pelo `oneof` (`CorpoCase` em C#, `WhichOneof` em Python).
- Os números dos campos são fixos, então servidores e clientes continuam compatíveis entre si.
- O mesmo `mensagem.proto` está copiado em cada projeto (`ClienteCs`, `ClientePy`, `ServidorCS`, `ServidorPy`), pois cada imagem Docker é construída a partir da própria pasta. **As quatro cópias devem ser sempre idênticas**; ao alterar o `.proto`, copie para todas e reconstrua as imagens.
- Toda mensagem carrega `timestamp_ms` (relógio físico) e `relogio` (relógio lógico).

### Escolha da persistência: SQLite

- Cada réplica de servidor tem **seu próprio arquivo SQLite**, em `/dados/{ID_REPLICA}.db`. O `ID_REPLICA` é a variável de ambiente `SERVER_ID` ou, se não existir, o `hostname` do container.
- Motivos: é um banco real (transações, chaves únicas, consultas), não precisa de servidor separado, e o código é pequeno. Arquivos JSON exigiriam reescrever o arquivo inteiro a cada escrita e seriam frágeis com acesso concorrente; guardar só em memória perderia tudo ao reiniciar.

---

## 3. Troca de mensagens e armazenamento 

### Padrões de comunicação ZeroMQ

| Quem fala com quem | Padrão | Endereço |
|---|---|---|
| Cliente → servidor (pedidos) | `REQ` → `ROUTER`/`DEALER` (broker) → `REP` | broker: 5555 (clientes), 5556 (servidores) |
| Servidor → inscritos (publicações) | `PUB` → `XSUB`/`XPUB` (proxy) → `SUB` | proxy: 5557 (publicadores), 5558 (assinantes) |
| Servidor ↔ servidor | `REQ`/`REP` direto | porta 6000 de cada servidor |
| Servidor → referência | `REQ`/`REP` | `referencia:5559` |

**Pedidos dos clientes (REQ/REP com broker).** O cliente se conecta ao broker (`tcp://broker:5555`). O broker (ROUTER ↔ DEALER) distribui os pedidos entre os servidores conectados em **round-robin**. Assim o cliente não precisa saber quantos servidores existem nem onde estão, e dá para aumentar o número de réplicas sem mudar os clientes. Os servidores respondem com `REP` e a resposta volta pelo broker até o cliente certo.

**Publicações (PUB/SUB com proxy).** Quando um bot posta uma mensagem, o servidor a publica no tópico do canal. O proxy (XSUB/XPUB) permite que todos os servidores publiquem em um único endereço e todos os bots assinem em outro, sem conhecerem uns aos outros.

**Tópicos com terminador.** O filtro de tópicos do ZeroMQ funciona por **prefixo**: assinar `Canal1` também receberia `Canal10`. Por isso os tópicos são publicados e assinados como `canal|` (com o separador `|` no final). Consequência: **nomes de canal não podem conter `|`** (o servidor recusa a criação).

Tópicos usados:

- `NomeDoCanal|` — mensagens dos canais (os bots assinam).
- `servers|` — anúncio do coordenador eleito (os servidores assinam).
- `replicacao|` — operações replicadas entre servidores (os servidores assinam).

**Dois sockets por bot.** Cada bot tem um `REQ` (pedidos ao servidor) e, em uma **thread separada**, um `SUB` (recepção). Assim ele consegue enviar a cada ~1 s e, ao mesmo tempo, receber publicações. O tempo gasto esperando a resposta do servidor é descontado do intervalo, para manter ~1 s entre os envios.

### Armazenamento das publicações dos bots

Cada servidor mantém três tabelas SQLite:

```sql
logins    (id, usuario, timestamp_ms,        UNIQUE(usuario, timestamp_ms))
canais    (nome PRIMARY KEY, timestamp_ms)
mensagens (id, canal → canais(nome), texto, timestamp_ms,
           UNIQUE(canal, texto, timestamp_ms))
```

- Cada `post` de um bot gera uma linha em `mensagens` com o **canal**, o **texto** e o **timestamp enviado pelo bot**.
- O servidor só aceita `post` em canal existente (o bot cria o canal antes se precisar).
- `listar_canais` devolve os canais por ordem de criação.
- As chaves `UNIQUE` e o `INSERT OR IGNORE` tornam todas as escritas **idempotentes**: gravar a mesma operação duas vezes não duplica nada. Isso é a base da replicação (Parte 5).
- No servidor C#, todo acesso ao SQLite passa por `lock`, pois mais de uma thread usa a conexão.

---

## 4. Replicação 

### Método escolhido: replicação ativa (de réplica para réplica)

Do conteúdo visto em aula, usamos a **replicação ativa**: todas as réplicas executam as mesmas operações de escrita, em vez de uma delas ser a única que processa e as outras apenas copiarem o resultado. Não há "primário" para as escritas: qualquer servidor aceita a escrita, grava localmente e **propaga a operação** às demais. A consistência obtida é **eventual**: depois que as operações circulam, todas as réplicas convergem para o mesmo conjunto de dados.

### Como foi implementado

**Propagação (push).**

1. O servidor que recebe a operação de um cliente (login, criação de canal ou mensagem) **grava no seu banco** e responde o cliente.
2. Em seguida publica uma `Replicacao` no tópico `replicacao|` (via proxy). A mensagem contém `origem_servidor`, `relogio` e um `oneof` com a operação (`login`, `canal` ou `mensagem`, com o timestamp original do bot).
3. Todos os outros servidores assinam `replicacao|` e **aplicam a mesma operação** no seu SQLite.
4. O servidor ignora as replicações cuja `origem_servidor` é ele mesmo, e quem aplica uma operação replicada **só grava, nunca republica**. Assim não há laço de replicação.

**Idempotência.** A aplicação usa `INSERT OR IGNORE` com chaves `UNIQUE`: se a mesma operação chegar duas vezes (ou já tiver sido recebida pela sincronização inicial), nada é duplicado. A ordem de chegada também não importa para o resultado final, pois as listagens são ordenadas pelo timestamp do bot.

**Sincronização ao entrar (pull).** Ao subir, o servidor pede o estado completo (`SincronizarEstado` → `EstadoCompleto`, com todos os logins, canais e mensagens) ao primeiro servidor ativo que responder, e aplica com `importar_tudo` (também idempotente). Isso acontece **antes** de ele começar a atender clientes.

### Foi necessário alterar o método?

**Sim.** A replicação ativa "pura" por publicação (push) assume que todas as réplicas estão no ar no momento da operação. No nosso projeto, servidores **caem e voltam** (e podem iniciar depois dos outros). Uma réplica que estava fora do ar **perde as publicações** daquele período (o PUB/SUB não guarda histórico para quem não estava inscrito), e ficaria permanentemente diferente das demais.

Por isso o método foi **estendido com uma transferência de estado na entrada** (pull): o servidor que entra copia o estado completo de uma réplica viva. Com isso, a réplica que volta "alcança" o que perdeu, e depois passa a receber as novas operações pelo canal `replicacao|`. O resultado é um método híbrido: replicação ativa para o fluxo normal + cópia de estado para recuperação.

---


