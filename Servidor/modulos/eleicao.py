import time

import zmq
import mensagem_pb2

from modulos import estado
from modulos.relogio import agora, avancar_relogio_logico, atualizar_relogio_logico, enviar, formatar
from modulos.referencia_cliente import pedir_servidores


def _novo_req(timeout_ms=3000):
    req = estado.context.socket(zmq.REQ)
    req.setsockopt(zmq.LINGER, 0)
    req.setsockopt(zmq.SNDTIMEO, timeout_ms)
    req.setsockopt(zmq.RCVTIMEO, timeout_ms)
    return req


## Lado que pergunta
def pedir_horario_coordenador():
    if estado.coordenador_atual is None:     
        return None

    req = _novo_req()
    pedido = mensagem_pb2.MensagemServidor()
    pedido.pedido.horario.SetInParent()

    try:
        req.connect(f"tcp://{estado.coordenador_atual}:{estado.PORTA_SERVIDORES}")
        enviar(pedido, req)
        res = mensagem_pb2.MensagemServidor()
        res.ParseFromString(req.recv())
        return res
    except zmq.ZMQError:
        return None
    finally:
        req.close()


def anunciar_coordenador():
    publicacao = mensagem_pb2.PublicacaoCoordenador()
    publicacao.relogio = avancar_relogio_logico()
    publicacao.mensagem = estado.ID_REPLICA
    publicacao.timestamp_ms = agora()
    estado.pub.send_multipart([f"{estado.CANAL_SERVERS}{estado.SEP}".encode(), publicacao.SerializeToString()])


def pedir_eleicao():
    servidores = pedir_servidores().servidores

    for servidor in servidores:
        if servidor.nome != estado.ID_REPLICA and servidor.rank > estado.rank:
            req = _novo_req()
            pedido = mensagem_pb2.MensagemServidor()
            pedido.pedido.eleicao.SetInParent()

            try:
                req.connect(f"tcp://{servidor.nome}:{estado.PORTA_SERVIDORES}")
                enviar(pedido, req)

                resposta = mensagem_pb2.MensagemServidor()
                resposta.ParseFromString(req.recv())
                atualizar_relogio_logico(resposta.relogio)
                estado.coordenador_atual = None
                break
            except zmq.ZMQError:
                continue
            finally:
                req.close()
    else:
        estado.coordenador_atual = estado.ID_REPLICA
        time.sleep(0.5)
        print(f"{formatar(agora())} Servidor {estado.ID_REPLICA} eleito coordenador - Publicando aviso - Rank: {estado.rank}", flush=True)
        anunciar_coordenador()


## Lado que responde
def tratar_servidor():
    recv = estado.socket_servidores.recv()

    mensagem = mensagem_pb2.MensagemServidor()
    mensagem.ParseFromString(recv)
    atualizar_relogio_logico(mensagem.relogio)

    tipo_pedido = None
    if mensagem.WhichOneof("corpo") == "pedido":
        tipo_pedido = mensagem.pedido.WhichOneof("corpo")

    res = mensagem_pb2.MensagemServidor()   # REP exige sempre uma resposta
    if tipo_pedido == "horario":
        res.resposta.horario.SetInParent()
    elif tipo_pedido == "eleicao":
        res.resposta.eleicao.SetInParent()
    enviar(res, estado.socket_servidores)

    if tipo_pedido == "eleicao":
        pedir_eleicao()


def tratar_mensagem_publicada():
    topico, mensagem = estado.sub.recv_multipart()

    publicacao = mensagem_pb2.PublicacaoCoordenador()
    publicacao.ParseFromString(mensagem)

    estado.coordenador_atual = publicacao.mensagem
    print(f"{formatar(agora())} Servidor {estado.ID_REPLICA} recebeu publicação de novo coordenador: {estado.coordenador_atual}", flush=True)
    atualizar_relogio_logico(publicacao.relogio)