import time

import zmq
import mensagem_pb2

from modulos import estado
from modulos.relogio import agora, avancar_relogio_logico, atualizar_relogio_logico, enviar, formatar
from modulos.referencia_cliente import pedir_servidores

TIMEOUT_MS = 2000
ESPERA_ANUNCIO_MS = 3 * TIMEOUT_MS   # tempo esperando o anuncio depois que um servidor maior respondeu OK


def _novo_req():
    req = estado.context.socket(zmq.REQ)
    req.setsockopt(zmq.LINGER, 0)
    req.setsockopt(zmq.SNDTIMEO, TIMEOUT_MS)
    return req


def _esperar_resposta(req, timeout_ms=TIMEOUT_MS):
    # Espera a resposta de outro servidor SEM parar de atender os pedidos que
    # chegam no socket_servidores.
    poller = zmq.Poller()
    poller.register(req, zmq.POLLIN)
    poller.register(estado.socket_servidores, zmq.POLLIN)

    limite = time.time() + timeout_ms / 1000
    while True:
        restante = int((limite - time.time()) * 1000)
        if restante <= 0:
            return None

        eventos = dict(poller.poll(restante))
        if estado.socket_servidores in eventos:
            tratar_servidor()
        if req in eventos:
            res = mensagem_pb2.MensagemServidor()
            res.ParseFromString(req.recv())
            atualizar_relogio_logico(res.relogio)
            return res


def _pedir(nome_servidor, pedido):
    req = _novo_req()
    try:
        req.connect(f"tcp://{nome_servidor}:{estado.PORTA_SERVIDORES}")
        enviar(pedido, req)
        return _esperar_resposta(req)
    except zmq.ZMQError:
        return None
    finally:
        req.close()


## Lado que pergunta
def pedir_horario_coordenador():
    if estado.coordenador_atual is None:
        return None

    pedido = mensagem_pb2.MensagemServidor()
    pedido.pedido.horario.SetInParent()
    return _pedir(estado.coordenador_atual, pedido)


def sincronizar_com_cluster(): 
    servidores = pedir_servidores().servidores 

    for servidor in servidores: 
        if servidor.nome == estado.ID_REPLICA: 
            continue 

        pedido = mensagem_pb2.MensagemServidor() 
        pedido.pedido.sincronizar.SetInParent() 
        resposta = _pedir(servidor.nome, pedido) 
        if resposta is None: 
            continue #

        if resposta.WhichOneof("corpo") == "resposta" and resposta.resposta.WhichOneof("corpo") == "estado_completo": 
            ec = resposta.resposta.estado_completo 
            logins = [(l.usuario, l.timestamp_ms) for l in ec.logins] 
            canais = [(c.nome, c.timestamp_ms) for c in ec.canais] 
            mensagens = [(m.canal, m.texto, m.timestamp_ms) for m in ec.mensagens] 
            estado.dados.importar_tudo(logins, canais, mensagens) 
            print( 
                f"{formatar(agora())} Servidor {estado.ID_REPLICA} sincronizou estado com {servidor.nome} " 
                f"({len(canais)} canais, {len(mensagens)} msgs)", 
                flush=True 
            ) 
            return True 

    return False 


def anunciar_coordenador():
    publicacao = mensagem_pb2.PublicacaoCoordenador()
    publicacao.relogio = avancar_relogio_logico()
    publicacao.mensagem = estado.ID_REPLICA
    publicacao.rank = estado.rank
    publicacao.timestamp_ms = agora()
    estado.pub.send_multipart([f"{estado.CANAL_SERVERS}{estado.SEP}".encode(), publicacao.SerializeToString()])


def pedir_eleicao():
    estado.eleicao_pendente = False
    servidores = pedir_servidores().servidores
    # pergunta primeiro pro maior rank, que e quem deve ganhar
    maiores = sorted((s for s in servidores if s.nome != estado.ID_REPLICA and s.rank > estado.rank),
                     key=lambda s: s.rank, reverse=True)

    for servidor in maiores:
        pedido = mensagem_pb2.MensagemServidor()
        pedido.pedido.eleicao.SetInParent()

        resposta = _pedir(servidor.nome, pedido)
        if resposta is not None:
            # tem servidor maior vivo: ele assume a eleicao e eu espero o anuncio
            estado.coordenador_atual = None
            estado.aguardando_anuncio_ate = agora() + ESPERA_ANUNCIO_MS
            return

    estado.coordenador_atual = estado.ID_REPLICA
    estado.aguardando_anuncio_ate = None
    print(f"{formatar(agora())} Servidor {estado.ID_REPLICA} eleito coordenador - Publicando aviso - Rank: {estado.rank}", flush=True)
    anunciar_coordenador()
    # quem pediu eleicao enquanto eu fazia a minha ja vai receber esse anuncio
    estado.eleicao_pendente = False


## Lado que responde
def tratar_servidor():
    recv = estado.socket_servidores.recv()

    mensagem = mensagem_pb2.MensagemServidor()
    mensagem.ParseFromString(recv)
    atualizar_relogio_logico(mensagem.relogio)

    tipo_pedido = None
    if mensagem.WhichOneof("corpo") == "pedido":
        tipo_pedido = mensagem.pedido.WhichOneof("corpo")

    res = mensagem_pb2.MensagemServidor()  
    if tipo_pedido == "horario":
        res.resposta.horario.SetInParent()
    elif tipo_pedido == "eleicao":
        res.resposta.eleicao.SetInParent()
    elif tipo_pedido == "sincronizar": 
        dump = estado.dados.exportar_tudo() 
        ec = res.resposta.estado_completo 
        for usuario, ts in dump["logins"]: 
            ec.logins.add(usuario=usuario, timestamp_ms=ts) 
        for nome, ts in dump["canais"]: 
            ec.canais.add(nome=nome, timestamp_ms=ts) 
        for canal, texto, ts in dump["mensagens"]: 
            ec.mensagens.add(canal=canal, texto=texto, timestamp_ms=ts) 

    enviar(res, estado.socket_servidores)

    if tipo_pedido == "eleicao":
        estado.eleicao_pendente = True


def _aplicar_replicacao(payload: bytes): 
    rep = mensagem_pb2.Replicacao() 
    rep.ParseFromString(payload) 
    atualizar_relogio_logico(rep.relogio) 

    if rep.origem_servidor == estado.ID_REPLICA: 
        return 

    op = rep.WhichOneof("operacao") 
    if op == "login": 
        estado.dados.registrar_login(rep.login.usuario, rep.login.timestamp_ms) 
    elif op == "canal": 
        estado.dados.criar_canal(rep.canal.nome, rep.canal.timestamp_ms) 
    elif op == "mensagem": 
        estado.dados.postar(rep.mensagem.canal, rep.mensagem.texto, rep.mensagem.timestamp_ms) 


def tratar_mensagem_publicada():
    topico_bytes, mensagem = estado.sub.recv_multipart() 
    topico = topico_bytes.decode() 

    if topico.startswith(f"{estado.CANAL_SERVERS}{estado.SEP}"): 
        publicacao = mensagem_pb2.PublicacaoCoordenador()
        publicacao.ParseFromString(mensagem)
        atualizar_relogio_logico(publicacao.relogio)

        estado.coordenador_atual = publicacao.mensagem
        estado.aguardando_anuncio_ate = None
        print(f"{formatar(agora())} Servidor {estado.ID_REPLICA} recebeu publicação de novo coordenador: {estado.coordenador_atual}", flush=True)

        # valentao: se quem se anunciou tem rank menor que o meu, eu tomo o lugar
        if publicacao.mensagem != estado.ID_REPLICA and publicacao.rank < estado.rank:
            estado.eleicao_pendente = True

    elif topico.startswith(f"{estado.CANAL_REPLICACAO}{estado.SEP}"): 
        _aplicar_replicacao(mensagem) 
