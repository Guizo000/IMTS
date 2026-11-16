import mensagem_pb2

from modulos import estado
from modulos.relogio import agora, avancar_relogio_logico, formatar


def _trocar(pedido):
    pedido.timestamp_ms = agora()
    avancar_relogio_logico()
    estado.referencia.send(pedido.SerializeToString())

    res = mensagem_pb2.RespostaReferencia()
    res.ParseFromString(estado.referencia.recv())
    return res


def pedir_rank():
    pedido = mensagem_pb2.PedidoReferencia()
    pedido.pedir_rank = estado.ID_REPLICA
    return _trocar(pedido)


def pedir_servidores():
    pedido = mensagem_pb2.PedidoReferencia()
    pedido.listar.SetInParent()
    return _trocar(pedido)


def enviar_heartbeat():
    print(f"{formatar(agora())} Enviando mensagem de Heartbeat", flush=True)
    pedido = mensagem_pb2.PedidoReferencia()
    pedido.heartbeat = estado.ID_REPLICA
    res = _trocar(pedido)

    if not res.sucesso:
        res = pedir_rank()
        estado.rank = res.rank
        print(f"{formatar(agora())} Servidor {estado.ID_REPLICA} Pegando Rank por Erro no Heartbeat - Rank: {estado.rank}", flush=True)