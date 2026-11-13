import zmq, mensagem_pb2, time

context = zmq.Context()
socket = context.socket(zmq.REP)
socket.bind("tcp://*:5559")

#Informações dos servidores
rank = 1
servidores = {}
tempo_maximo_heartbeat = 15 * 1000  # 15 segundos

def remover_inativos():
    global servidores
    agora = int(time.time() * 1000)
    servidores = {n: i for n, i in servidores.items()
                  if agora - i["last_heartbeat"] <= tempo_maximo_heartbeat}

    # Imprime todos os servidores em um único print
    if servidores:
        print("Servidores ativos:\n" + "\n".join(
            f" - {nome}: Rank {dados['rank']} | Heartbeat: {dados['last_heartbeat']}" 
            for nome, dados in servidores.items()
        ), flush=True)
    else:
        print("Nenhum servidor ativo no momento.", flush=True)

    
def pedir_rank(pedido):
    global rank
    global servidores

    remover_inativos()

    nome = pedido.pedir_rank
    agora = int(time.time() * 1000)

    if nome not in servidores:
        servidores[nome] = {"rank": rank, "last_heartbeat": agora}
        rank += 1
    else:
        servidores[nome]["last_heartbeat"] = agora     

    res = mensagem_pb2.RespostaReferencia()
    res.sucesso = True
    res.rank = servidores[nome]["rank"] 

    return res

    
def listar_servidores(pedido):
    global servidores

    remover_inativos()

    res = mensagem_pb2.RespostaReferencia()
    res.sucesso = True
    for nome, info in servidores.items():
        res.servidores.add(nome=nome, rank=info["rank"])

    return res


def heartbeat(pedido):
    global servidores

    remover_inativos()

    nome = pedido.heartbeat
    agora = int(time.time() * 1000)

    res = mensagem_pb2.RespostaReferencia()
    if nome in servidores:
        servidores[nome]["last_heartbeat"] = agora
        res.sucesso = True
    else:
        res.sucesso = False
        res.erro = "Servidor não registrado"

    return res


while True:
    req = socket.recv()

    pedido = mensagem_pb2.PedidoReferencia()
    pedido.ParseFromString(req)

    tipo = pedido.WhichOneof("corpo")

    if tipo == "pedir_rank":
        res = pedir_rank(pedido)
    elif tipo == "listar":
        res = listar_servidores(pedido)
    elif tipo == "heartbeat":
        res = heartbeat(pedido)
        pass

    res.timestamp_ms = int(time.time() * 1000) 
    socket.send(res.SerializeToString()) 
    