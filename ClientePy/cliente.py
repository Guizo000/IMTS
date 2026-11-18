import os
import random
import threading
import time
from datetime import datetime, timezone

import zmq
import mensagem_pb2

## Configuração (mesmos valores do cliente em C#)
SEP = '|'
MIN_CANAIS = 5
MAX_INSCRICOES = 3
MSGS_POR_CANAL = 10
INTERVALO_MS = 1000

ENDERECO_BROKER = os.environ.get("SERVIDOR_ENDERECO", "tcp://broker:5555")
ENDERECO_SUB = os.environ.get("PROXY_SUB_ENDERECO", "tcp://proxy:5558")

usuario = f"pybot-{random.randint(0, 4999)}"

## Relógio lógico (Lamport)
relogio_logico = 0
trava = threading.Lock()


def proximo_relogio():
    global relogio_logico
    with trava:
        relogio_logico += 1
        return relogio_logico


def atualizar_relogio(recebido):
    global relogio_logico
    with trava:
        relogio_logico = max(relogio_logico, recebido)


def agora():
    return int(time.time() * 1000)


def formatar(timestamp_ms):
    dt = datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone.utc)
    return dt.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3] + " UTC"


## Socket REQ (pedidos ao servidor pelo broker)
context = zmq.Context()
socket = context.socket(zmq.REQ)
socket.connect(ENDERECO_BROKER)


def enviar(envelope, nome):
    envelope.timestamp_ms = agora()
    envelope.relogio = proximo_relogio()
    #print(f"{formatar(envelope.timestamp_ms)} - Enviando requisição: {nome}", flush=True)
    socket.send(envelope.SerializeToString())

    res = mensagem_pb2.Resposta()
    res.ParseFromString(socket.recv())
    atualizar_relogio(res.relogio)

    if not res.sucesso:
        print(f"{formatar(agora())} - Resposta de {nome} recebida - Erro: {res.erro}", flush=True)
    return res


def fazer_login():
    envelope = mensagem_pb2.Envelope()
    envelope.login.usuario = usuario
    return enviar(envelope, "Login")


def listar_canais():
    envelope = mensagem_pb2.Envelope()
    envelope.canal.listar.SetInParent()
    return enviar(envelope, "Listar Canais")


def criar_canal(nome):
    envelope = mensagem_pb2.Envelope()
    envelope.canal.criar = nome
    return enviar(envelope, f"Criar Canal ({nome})")


def postar(canal, texto):
    envelope = mensagem_pb2.Envelope()
    envelope.canal.post.canal = canal
    envelope.canal.post.mensagem = texto
    return enviar(envelope, f"Post em {canal}")


## Thread que recebe as publicações dos canais inscritos
def receber_publicacoes(canais, pronto):
    sub = context.socket(zmq.SUB)
    sub.connect(ENDERECO_SUB)
    for canal in canais:
        sub.setsockopt_string(zmq.SUBSCRIBE, f"{canal}{SEP}")
    pronto.set()

    while True:
        topico, conteudo = sub.recv_multipart()
        recebido = agora()

        envelope = mensagem_pb2.Envelope()
        envelope.ParseFromString(conteudo)
        atualizar_relogio(envelope.relogio)
        post = envelope.canal.post

        #print(f"[{post.canal}] {post.mensagem} | enviada: {formatar(envelope.timestamp_ms)} | recebida: {formatar(recebido)}", flush=True)


print(f"{usuario} entrando", flush=True)

## Login (repete até dar certo)
while not fazer_login().sucesso:
    time.sleep(1)

## Cria canais até ter pelo menos 5
lista = listar_canais()
while len(lista.canais) < MIN_CANAIS:
    criar_canal(f"Canal{random.randint(0, 99)}")
    lista = listar_canais()

## Se inscreve em até 3 canais aleatórios
inscritos = random.sample(list(lista.canais), min(MAX_INSCRICOES, len(lista.canais)))
print(f"{formatar(agora())} - Inscrito em: {', '.join(inscritos)}", flush=True)

pronto = threading.Event()
threading.Thread(target=receber_publicacoes, args=(inscritos, pronto), daemon=True).start()
pronto.wait()
time.sleep(0.5)   # espera a inscrição chegar no proxy

## Loop de envio de mensagens
while True:
    lista = listar_canais()
    while len(lista.canais) == 0:
        time.sleep(1)
        lista = listar_canais()

    canal = random.choice(list(lista.canais))
    print(f"{usuario} irá postar {MSGS_POR_CANAL} mensagens em {canal}", flush=True)

    for _ in range(MSGS_POR_CANAL):
        inicio = time.time()
        postar(canal, f"Mensagem {random.randrange(10000)} de {usuario}")

        # desconta o tempo da resposta pra manter ~1s entre os envios
        resto = INTERVALO_MS / 1000 - (time.time() - inicio)
        if resto > 0:
            time.sleep(resto)
