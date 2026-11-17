from modulos import estado
from modulos.relogio import agora, formatar, atualizar_relogio_logico, atualizar_relogio_fisico
from modulos.referencia_cliente import pedir_rank, enviar_heartbeat
from modulos.eleicao import ( #
    pedir_eleicao, #
    pedir_horario_coordenador, #
    sincronizar_com_cluster, #
    tratar_servidor, #
    tratar_mensagem_publicada, #
) #
from modulos.clientes import tratar_cliente

contador_heartbeat = 0
contador_relogio_fisico = 0

## PEDINDO AUTO RANK
contador_relogio_fisico += 1
estado.rank = pedir_rank().rank

## SINCRONIZANDO ESTADO COM RÉPLICAS ATIVAS #
sincronizar_com_cluster() #

## ELEIÇÃO AO ENTRAR NO SISTEMA
contador_relogio_fisico += 1
print(f"{formatar(agora())} Servidor: {estado.ID_REPLICA} entrando e pedindo eleição - Rank: {estado.rank}", flush=True)
pedir_eleicao()

while True:
    eventos = dict(estado.poller.poll())

    if estado.socket_clientes in eventos:
        tratar_cliente()
        contador_heartbeat += 1
        contador_relogio_fisico += 1

        if contador_heartbeat >= estado.MENSAGENS_HEARTBEAT:
            enviar_heartbeat()
            contador_heartbeat = 0

    if estado.socket_servidores in eventos:
        contador_relogio_fisico += 1
        tratar_servidor()

    if estado.sub in eventos:
        tratar_mensagem_publicada()

    if contador_relogio_fisico >= estado.MENSAGENS_RELOGIO_FISICO and estado.ID_REPLICA != estado.coordenador_atual:
        res_horario = pedir_horario_coordenador()

        if res_horario is None:
            print(f"{formatar(agora())} Servidor {estado.ID_REPLICA} pedindo eleição por ausência do coordenador - Rank: {estado.rank}", flush=True)
            pedir_eleicao()
        else:
            atualizar_relogio_logico(res_horario.relogio)
            atualizar_relogio_fisico(res_horario.timestamp_ms)

        contador_relogio_fisico = 0