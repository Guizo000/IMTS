using Google.Protobuf;
using NetMQ;
using NetMQ.Sockets;

// -------------------------------------------------------------------------//
// Pedidos REQ com timeout. Cada pedido abre um socket novo, assim se o      //
// outro lado nao responder o socket e descartado e nada fica travado.       //
// -------------------------------------------------------------------------//
static class Comunicacao
{
    public static readonly TimeSpan TIMEOUT = TimeSpan.FromSeconds(2);

    public static byte[]? Pedir(string endereco, byte[] pedido, TimeSpan? timeout = null)
    {
        try
        {
            using var req = new RequestSocket();
            req.Options.Linger = TimeSpan.Zero;
            req.Connect(endereco);

            if (!req.TrySendFrame(timeout ?? TIMEOUT, pedido))
                return null;
            if (!req.TryReceiveFrameBytes(timeout ?? TIMEOUT, out var resposta))
                return null;
            return resposta;
        }
        catch (Exception)
        {
            // nome do servidor que caiu nao resolve mais no DNS do docker, por exemplo
            return null;
        }
    }

    // ---- referencia ----
    static RespostaReferencia? PedirReferencia(PedidoReferencia pedido)
    {
        pedido.TimestampMs = Relogio.Agora();
        Relogio.Avancar();
        var bytes = Pedir(Estado.REFERENCIA, pedido.ToByteArray(), TimeSpan.FromSeconds(3));
        return bytes == null ? null : RespostaReferencia.Parser.ParseFrom(bytes);
    }

    public static int PedirRank()
    {
        while (true)
        {
            var res = PedirReferencia(new PedidoReferencia { PedirRank = Estado.ID_REPLICA });
            if (res != null && res.Sucesso)
                return res.Rank;

            Thread.Sleep(1000);
        }
    }

    public static List<Servidor> PedirServidores()
    {
        var res = PedirReferencia(new PedidoReferencia { Listar = new ListarServidores() });
        return res == null ? new List<Servidor>() : res.Servidores.ToList();
    }

    public static void EnviarHeartbeat()
    {
        Log.Escrever($"Enviando mensagem de Heartbeat - Rank: {Estado.Rank}", doisPontos: true);
        Log.Escrever($"- Coordenador Atual: {Estado.CoordenadorAtual}", doisPontos: true);

        var res = PedirReferencia(new PedidoReferencia { Heartbeat = Estado.ID_REPLICA });
        if (res != null && !res.Sucesso)
        {
            int rank = PedirRank();
            lock (Estado.Trava) Estado.Rank = rank;
            Log.Escrever($"Pegando Rank por Erro no Heartbeat - Rank: {rank}", doisPontos: true);
        }
    }

    // ---- outros servidores ----
    public static MensagemServidor? PedirServidor(string nome, MensagemServidor pedido)
    {
        pedido.TimestampMs = Relogio.Agora();
        pedido.Relogio = Relogio.Avancar();

        var bytes = Pedir($"tcp://{nome}:{Estado.PORTA_SERVIDORES}", pedido.ToByteArray());
        if (bytes == null)
            return null;

        var res = MensagemServidor.Parser.ParseFrom(bytes);
        Relogio.Atualizar(res.Relogio);
        return res;
    }
}
