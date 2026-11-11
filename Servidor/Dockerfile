FROM python:3.13-slim

WORKDIR /app

RUN pip install pyzmq protobuf grpcio-tools

COPY Servidor/mensagem.proto .

RUN python -m grpc_tools.protoc -I. --python_out=. mensagem.proto

CMD ["python", "main.py"]