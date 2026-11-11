import zmq, mensagem_pb2, time, os, socket as sock
from armazenamento import Armazenamento
from datetime import datetime, timezone

ID_REPLICA = os.environ.get("SERVER_ID", sock.gethostname())
dados = Armazenamento(f"/dados/{ID_REPLICA}.db")

context = zmq.Context()

socket = context.socket(zmq.REP)
socket.connect("tcp://broker:5556")

pub = context.socket(zmq.PUB)
pub.connect("tcp://proxy:5557")

SEP = '|';

##----------------------##
## FUNÇÕES AUXILIARES   ##
##----------------------##
def formatar(timestamp_ms: int):
    dt = datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone.utc)
    return dt.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3] + " UTC"


##----------------------##
## FUNÇÕES DE REQ / REP ##
##----------------------##
def Login(envelope):
    #print(f"{formatar(int(time.time() * 1000))} - Pedido de login recebido: {envelope.login.usuario}", flush=True)

    res = mensagem_pb2.Resposta()
    if dados.registrar_login(envelope.login.usuario, envelope.timestamp_ms) == False:
        res.erro = "Usuário já logado"
        res.sucesso = False
    else:
        res.sucesso = True
    res.timestamp_ms = int(time.time() * 1000)

    #print(f"{formatar(res.timestamp_ms)} - Enviando resposta do login", flush=True)
    socket.send(res.SerializeToString())


def CriarCanal(envelope):
    #print(f"{formatar(int(time.time() * 1000))} - Pedido de criação de canal recebido: {envelope.canal.criar}", flush=True)

    res = mensagem_pb2.Resposta()
    if SEP in envelope.canal.criar:
        res.erro = f"Nome de canal não pode conter '{SEP}'"
        res.sucesso = False
    elif dados.criar_canal(envelope.canal.criar, envelope.timestamp_ms) == False:
        res.erro = "Canal já criado"
        res.sucesso = False
    else:
        res.sucesso = True
    res.timestamp_ms = int(time.time() * 1000)

    #print(f"{formatar(res.timestamp_ms)} - Enviando resposta da criação do canal", flush=True)
    socket.send(res.SerializeToString())


def ListarCanais():
    #print(f"{formatar(int(time.time() * 1000))} - Pedido de listagem de canais recebido", flush=True)

    res = mensagem_pb2.Resposta()
    canais = dados.listar_canais()
    
    res.sucesso = True
    res.canais.extend(canais)

    res.timestamp_ms = int(time.time() * 1000)

    #print(f"{formatar(res.timestamp_ms)} - Enviando resposta da listagem de canais", flush=True)
    socket.send(res.SerializeToString())


##----------------------##
## FUNÇÕES DE PUB/SUB   ##
##----------------------##
def PostarMensagem(envelope):
    #print(f"{formatar(int(time.time() * 1000))} - Pedido de postagem de mensagem recebido: {envelope.canal.post.canal}", flush=True)

    res = mensagem_pb2.Resposta()
    post = envelope.canal.post
    if dados.canal_existe(post.canal) == False:
        res.erro = "Canal não existe"
        res.sucesso = False
    else:
        dados.postar(post.canal, post.mensagem, envelope.timestamp_ms)
        pub.send_multipart([f"{post.canal}{SEP}".encode(), envelope.SerializeToString()])
        res.sucesso = True
    res.timestamp_ms = int(time.time() * 1000)

    #print(f"{formatar(res.timestamp_ms)} - Enviando resposta da postagem de mensagem", flush=True)
    socket.send(res.SerializeToString())


##----------------------##
##  LOOP PRINCIPAL      ##
##----------------------##
while True:
    req = socket.recv()

    envelope = mensagem_pb2.Envelope()
    envelope.ParseFromString(req)

    tipo = envelope.WhichOneof("corpo")

    if tipo == "login":
        Login(envelope)
    if tipo == "canal":
        tipoCanal = envelope.canal.WhichOneof("corpo")

        if tipoCanal == "criar":
            CriarCanal(envelope)
        elif tipoCanal == "listar":
            ListarCanais()
        elif tipoCanal == "post":
            PostarMensagem(envelope)



