import os
import socket as sock

import zmq

from armazenamento import Armazenamento

## Configuração
ID_REPLICA = os.environ.get("SERVER_ID", sock.gethostname())
PORTA_SERVIDORES = 6000
SEP = '|'
CANAL_SERVERS = "servers"
CANAL_REPLICACAO = "replicacao" #
MENSAGENS_HEARTBEAT = 10
MENSAGENS_RELOGIO_FISICO = 15

## Armazenamento
dados = Armazenamento(f"/dados/{ID_REPLICA}.db")

## Sockets
context = zmq.Context()

socket_clientes = context.socket(zmq.REP)
socket_clientes.connect("tcp://broker:5556")

socket_servidores = context.socket(zmq.REP)
socket_servidores.bind(f"tcp://*:{PORTA_SERVIDORES}")

referencia = context.socket(zmq.REQ)
referencia.connect("tcp://referencia:5559")

pub = context.socket(zmq.PUB)
pub.connect("tcp://proxy:5557")

sub = context.socket(zmq.SUB)
sub.connect("tcp://proxy:5558")
sub.setsockopt_string(zmq.SUBSCRIBE, f"{CANAL_SERVERS}{SEP}")
sub.setsockopt_string(zmq.SUBSCRIBE, f"{CANAL_REPLICACAO}{SEP}") #

poller = zmq.Poller()
poller.register(socket_clientes, zmq.POLLIN)
poller.register(socket_servidores, zmq.POLLIN)
poller.register(sub, zmq.POLLIN)

## Estado que muda durante a execução
relogio_logico = 0
relogio_fisico_offset = 0
rank = None
coordenador_atual = None
eleicao_pendente = False        # alguem pediu eleicao (ou anunciou coordenador com rank menor)
aguardando_anuncio_ate = None   # recebi OK de um maior, espero o anuncio ate esse horario
