import mensagem_pb2

from modulos import estado
from modulos.relogio import avancar_relogio_logico, atualizar_relogio_logico, enviar


def pedido_invalido():
    res = mensagem_pb2.Resposta()
    res.erro = "Pedido inválido"
    res.sucesso = False
    enviar(res, estado.socket_clientes)


def login(envelope):
    res = mensagem_pb2.Resposta()
    if estado.dados.registrar_login(envelope.login.usuario, envelope.timestamp_ms) == False:
        res.erro = "Usuário já logado"
        res.sucesso = False
    else:
        res.sucesso = True
    enviar(res, estado.socket_clientes)


def criar_canal(envelope):
    res = mensagem_pb2.Resposta()
    if estado.SEP in envelope.canal.criar:
        res.erro = f"Nome de canal não pode conter '{estado.SEP}'"
        res.sucesso = False
    elif estado.dados.criar_canal(envelope.canal.criar, envelope.timestamp_ms) == False:
        res.erro = "Canal já criado"
        res.sucesso = False
    else:
        res.sucesso = True
    enviar(res, estado.socket_clientes)


def listar_canais():
    res = mensagem_pb2.Resposta()
    res.sucesso = True
    res.canais.extend(estado.dados.listar_canais())
    enviar(res, estado.socket_clientes)


def postar_mensagem(envelope):
    res = mensagem_pb2.Resposta()
    post = envelope.canal.post
    if estado.dados.canal_existe(post.canal) == False:
        res.erro = "Canal não existe"
        res.sucesso = False
    else:
        estado.dados.postar(post.canal, post.mensagem, envelope.timestamp_ms)
        envelope.relogio = avancar_relogio_logico()
        estado.pub.send_multipart([f"{post.canal}{estado.SEP}".encode(), envelope.SerializeToString()])
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