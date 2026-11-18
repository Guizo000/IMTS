import mensagem_pb2

from modulos import estado
from modulos.relogio import avancar_relogio_logico, atualizar_relogio_logico, enviar


def _publicar_replicacao(rep: mensagem_pb2.Replicacao): #
    rep.origem_servidor = estado.ID_REPLICA #
    rep.relogio = avancar_relogio_logico() #
    estado.pub.send_multipart([ #
        f"{estado.CANAL_REPLICACAO}{estado.SEP}".encode(), #
        rep.SerializeToString() #
    ]) #


def pedido_invalido():
    res = mensagem_pb2.Resposta()
    res.erro = "Pedido inválido"
    res.sucesso = False
    enviar(res, estado.socket_clientes)


def login(envelope):
    res = mensagem_pb2.Resposta()
    usuario = envelope.login.usuario #
    ts = envelope.timestamp_ms #

    if estado.dados.registrar_login(usuario, ts) == False: #
        res.sucesso = False
    else:
        res.sucesso = True
        rep = mensagem_pb2.Replicacao() #
        rep.login.usuario = usuario #
        rep.login.timestamp_ms = ts #
        _publicar_replicacao(rep) #

    enviar(res, estado.socket_clientes)


def criar_canal(envelope):
    res = mensagem_pb2.Resposta()
    nome_canal = envelope.canal.criar #
    ts = envelope.timestamp_ms #

    if estado.SEP in nome_canal: #
        res.erro = f"Nome de canal não pode conter '{estado.SEP}'"
        res.sucesso = False
    elif estado.dados.criar_canal(nome_canal, ts) == False: #
        res.erro = "Canal já criado"
        res.sucesso = False
    else:
        res.sucesso = True
        rep = mensagem_pb2.Replicacao() #
        rep.canal.nome = nome_canal #
        rep.canal.timestamp_ms = ts #
        _publicar_replicacao(rep) #

    enviar(res, estado.socket_clientes)


def listar_canais():
    res = mensagem_pb2.Resposta()
    res.sucesso = True
    res.canais.extend(estado.dados.listar_canais())
    enviar(res, estado.socket_clientes)


def postar_mensagem(envelope):
    res = mensagem_pb2.Resposta()
    post = envelope.canal.post
    ts = envelope.timestamp_ms #

    if estado.dados.canal_existe(post.canal) == False:
        res.erro = "Canal não existe"
        res.sucesso = False
    else:
        estado.dados.postar(post.canal, post.mensagem, ts) #
        envelope.relogio = avancar_relogio_logico()
        estado.pub.send_multipart([
            f"{post.canal}{estado.SEP}".encode(),
            envelope.SerializeToString()
        ])

        rep = mensagem_pb2.Replicacao() #
        rep.mensagem.canal = post.canal #
        rep.mensagem.texto = post.mensagem #
        rep.mensagem.timestamp_ms = ts #
        _publicar_replicacao(rep) #

        res.sucesso = True

    enviar(res, estado.socket_clientes)


def tratar_cliente():
    req = estado.socket_clientes.recv()

    envelope = mensagem_pb2.Envelope()
    envelope.ParseFromString(req)
    atualizar_relogio_logico(envelope.relogio)

    tipo = envelope.WhichOneof("corpo")
    if tipo == "login":
        login(envelope)
    elif tipo == "canal":
        tipo_canal = envelope.canal.WhichOneof("corpo")
        if tipo_canal == "criar":
            criar_canal(envelope)
        elif tipo_canal == "listar":
            listar_canais()
        elif tipo_canal == "post":
            postar_mensagem(envelope)
        else:
            pedido_invalido()
    else:
        pedido_invalido()