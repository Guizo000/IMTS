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


def sincronizar_com_cluster(): #
    servidores = pedir_servidores().servidores #

    for servidor in servidores: #
        if servidor.nome == estado.ID_REPLICA: #
            continue #

        req = _novo_req() #
        pedido = mensagem_pb2.MensagemServidor() #
        pedido.pedido.sincronizar.SetInParent() #

        try: #
            req.connect(f"tcp://{servidor.nome}:{estado.PORTA_SERVIDORES}") #
            enviar(pedido, req) #

            resposta = mensagem_pb2.MensagemServidor() #
            resposta.ParseFromString(req.recv()) #
            atualizar_relogio_logico(resposta.relogio) #

            if resposta.WhichOneof("corpo") == "resposta" and resposta.resposta.WhichOneof("corpo") == "estado_completo": #
                ec = resposta.resposta.estado_completo #
                logins = [(l.usuario, l.timestamp_ms) for l in ec.logins] #
                canais = [(c.nome, c.timestamp_ms) for c in ec.canais] #
                mensagens = [(m.canal, m.texto, m.timestamp_ms) for m in ec.mensagens] #
                estado.dados.importar_tudo(logins, canais, mensagens) #
                print( #
                    f"{formatar(agora())} Servidor {estado.ID_REPLICA} sincronizou estado com {servidor.nome} " #
                    f"({len(canais)} canais, {len(mensagens)} msgs)", #
                    flush=True #
                ) #
                return True #
        except zmq.ZMQError: #
            continue #
        finally: #
            req.close() #

    return False #


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
    elif tipo_pedido == "sincronizar": #
        dump = estado.dados.exportar_tudo() #
        ec = res.resposta.estado_completo #
        for usuario, ts in dump["logins"]: #
            ec.logins.add(usuario=usuario, timestamp_ms=ts) #
        for nome, ts in dump["canais"]: #
            ec.canais.add(nome=nome, timestamp_ms=ts) #
        for canal, texto, ts in dump["mensagens"]: #
            ec.mensagens.add(canal=canal, texto=texto, timestamp_ms=ts) #

    enviar(res, estado.socket_servidores)

    if tipo_pedido == "eleicao":
        pedir_eleicao()


def _aplicar_replicacao(payload: bytes): #
    rep = mensagem_pb2.Replicacao() #
    rep.ParseFromString(payload) #
    atualizar_relogio_logico(rep.relogio) #

    if rep.origem_servidor == estado.ID_REPLICA: #
        return #

    op = rep.WhichOneof("operacao") #
    if op == "login": #
        estado.dados.registrar_login(rep.login.usuario, rep.login.timestamp_ms) #
    elif op == "canal": #
        estado.dados.criar_canal(rep.canal.nome, rep.canal.timestamp_ms) #
    elif op == "mensagem": #
        estado.dados.postar(rep.mensagem.canal, rep.mensagem.texto, rep.mensagem.timestamp_ms) #


def tratar_mensagem_publicada():
    topico_bytes, mensagem = estado.sub.recv_multipart() #
    topico = topico_bytes.decode() #

    if topico.startswith(f"{estado.CANAL_SERVERS}{estado.SEP}"): #
        publicacao = mensagem_pb2.PublicacaoCoordenador()
        publicacao.ParseFromString(mensagem)

        estado.coordenador_atual = publicacao.mensagem
        print(f"{formatar(agora())} Servidor {estado.ID_REPLICA} recebeu publicação de novo coordenador: {estado.coordenador_atual}", flush=True)
        atualizar_relogio_logico(publicacao.relogio)

    elif topico.startswith(f"{estado.CANAL_REPLICACAO}{estado.SEP}"): #
        _aplicar_replicacao(mensagem) #