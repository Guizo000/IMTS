import time
from datetime import datetime, timezone

from modulos import estado


def formatar(timestamp_ms):
    dt = datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone.utc)
    return dt.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3] + " UTC"


def agora():
    return int(time.time() * 1000) + estado.atraso_relogio_fisico + estado.relogio_fisico_offset 


def atualizar_relogio_fisico(timestamp_ms):
    estado.relogio_fisico_offset = timestamp_ms - (int(time.time() * 1000) + estado.atraso_relogio_fisico)
    print(f"{formatar(agora())} Servidor {estado.ID_REPLICA} atualizando seu relógio físico", flush=True)


def avancar_relogio_logico():
    estado.relogio_logico += 1
    return estado.relogio_logico


def atualizar_relogio_logico(recebido):
    estado.relogio_logico = max(estado.relogio_logico, recebido)
    #print(f"Relógio Lógico {estado.relogio_logico}", flush=True)


def enviar(envio, socket):
    envio.timestamp_ms = agora()
    envio.relogio = avancar_relogio_logico()
    socket.send(envio.SerializeToString())