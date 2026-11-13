import zmq, mensagem_pb2, time, os, socket as sock
from armazenamento import Armazenamento
from datetime import datetime, timezone

ID_REPLICA = os.environ.get("SERVER_ID", sock.gethostname())
dados = Armazenamento(f"/dados/{ID_REPLICA}.db")

context = zmq.Context()

socket = context.socket(zmq.REP)
socket.connect("tcp://broker:5556")

referencia = context.socket(zmq.REQ)
referencia.connect("tcp://referencia:5559")

pub = context.socket(zmq.PUB)
pub.connect("tcp://proxy:5557")

relogio_logico = 0
relogio_fisico_offset = 0
rank = None

MENSAGENS_HEARTBEAT = 10
SEP = '|'

##----------------------##
## FUNÇÕES AUXILIARES   ##
##----------------------##
def formatar(timestamp_ms):
    dt = datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone.utc)
    return dt.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3] + " UTC"

def agora():
    return int(time.time() * 1000) + relogio_fisico_offset

def atualizar_relogio_fisico(timestamp_ms):
    global relogio_fisico_offset
    relogio_fisico_offset = timestamp_ms - int(time.time() * 1000)

def pedir_rank():
    pedido = mensagem_pb2.PedidoReferencia()
    pedido.timestamp_ms = int(time.time() * 1000)
    pedido.pedir_rank = ID_REPLICA
    referencia.send(pedido.SerializeToString())

    res = referencia.recv()
    resReferencia = mensagem_pb2.RespostaReferencia()
    resReferencia.ParseFromString(res)
    return resReferencia

def avancar_relogio_logico():
    global relogio_logico
    relogio_logico += 1
    return relogio_logico

def atualizar_relogio_logico(recebido):
    global relogio_logico
    relogio_logico = max(relogio_logico, recebido)
    print(f"Relógio Lógico {relogio_logico}", flush=True)

def responder(res):
    res.timestamp_ms = agora()
    res.relogio = avancar_relogio_logico()
    socket.send(res.SerializeToString())

def pedido_invalido():
    res = mensagem_pb2.Resposta()
    res.erro = "Pedido inválido"
    res.sucesso = False
    responder(res)

##----------------------##
## FUNÇÕES DE REQ / REP ##
##----------------------##
def login(envelope):
    #print(f"{formatar(agora())} - Pedido de login recebido: {envelope.login.usuario}", flush=True)
    res = mensagem_pb2.Resposta()
    if dados.registrar_login(envelope.login.usuario, envelope.timestamp_ms) == False:
        res.erro = "Usuário já logado"
        res.sucesso = False
    else:
        res.sucesso = True

    #print(f"{formatar(res.timestamp_ms)} - Enviando resposta do login", flush=True)
    responder(res)


def criar_canal(envelope):
    #print(f"{formatar(agora())} - Pedido de criação de canal recebido: {envelope.canal.criar}", flush=True)
    res = mensagem_pb2.Resposta()
    if SEP in envelope.canal.criar:
        res.erro = f"Nome de canal não pode conter '{SEP}'"
        res.sucesso = False
    elif dados.criar_canal(envelope.canal.criar, envelope.timestamp_ms) == False:
        res.erro = "Canal já criado"
        res.sucesso = False
    else:
        res.sucesso = True

    #print(f"{formatar(res.timestamp_ms)} - Enviando resposta da criação do canal", flush=True)
    responder(res)
    


def listar_canais():
    #print(f"{formatar(agora())} - Pedido de listagem de canais recebido", flush=True)

    res = mensagem_pb2.Resposta()
    canais = dados.listar_canais()
    
    res.sucesso = True
    res.canais.extend(canais)

    #print(f"{formatar(res.timestamp_ms)} - Enviando resposta da listagem de canais", flush=True)
    responder(res)


##----------------------##
## FUNÇÕES DE PUB/SUB   ##
##----------------------##
def postar_mensagem(envelope):
    #print(f"{formatar(agora())} - Pedido de postagem de mensagem recebido: {envelope.canal.post.canal}", flush=True)

    res = mensagem_pb2.Resposta()
    post = envelope.canal.post
    if dados.canal_existe(post.canal) == False:
        res.erro = "Canal não existe"
        res.sucesso = False
    else:
        dados.postar(post.canal, post.mensagem, envelope.timestamp_ms)
        envelope.relogio = avancar_relogio_logico()
        pub.send_multipart([f"{post.canal}{SEP}".encode(), envelope.SerializeToString()])
        res.sucesso = True

    #print(f"{formatar(res.timestamp_ms)} - Enviando resposta da postagem de mensagem", flush=True)
    responder(res)


##----------------------##
##  LOOP PRINCIPAL      ##
##----------------------##
resReferencia = pedir_rank()
rank = resReferencia.rank

print(f"{formatar(agora())} Servidor {ID_REPLICA} entrando - Rank: {rank}", flush=True)
print(f"{formatar(agora())} Teste pré sincronização", flush=True)
atualizar_relogio_fisico(resReferencia.timestamp_ms)
print(f"{formatar(agora())} Teste pós sincronização", flush=True)

mensagens_recebidas = 0

while True:
    req = socket.recv()

    envelope = mensagem_pb2.Envelope()
    envelope.ParseFromString(req)

    atualizar_relogio_logico(envelope.relogio)

    tipo = envelope.WhichOneof("corpo")
    if tipo == "login":
        login(envelope)
    elif tipo == "canal":
        tipoCanal = envelope.canal.WhichOneof("corpo")

        if tipoCanal == "criar":
            criar_canal(envelope)
        elif tipoCanal == "listar":
            listar_canais()
        elif tipoCanal == "post":
            postar_mensagem(envelope)
        else:
            pedido_invalido()
    else:
        pedido_invalido()
        
    mensagens_recebidas += 1
    if mensagens_recebidas >= MENSAGENS_HEARTBEAT:
        print(f"{formatar(agora())} Enviando mensagem de Heartbeat", flush=True)
        pedido = mensagem_pb2.PedidoReferencia()
        pedido.timestamp_ms = agora()
        pedido.heartbeat = ID_REPLICA
        referencia.send(pedido.SerializeToString())

        res = referencia.recv()
        resReferencia = mensagem_pb2.RespostaReferencia()
        resReferencia.ParseFromString(res)

        if not resReferencia.sucesso:
            resReferencia = pedir_rank()
            rank = resReferencia.rank
            print(f"{formatar(agora())} Servidor {ID_REPLICA} Pegando Rank por Erro no Heartbeat - Rank: {rank}", flush=True)
        

        atualizar_relogio_fisico(resReferencia.timestamp_ms)
        mensagens_recebidas = 0



