using Google.Protobuf;
using NetMQ;
using NetMQ.Sockets;

// -------------------------------------------------------------------------//
//   thread principal  -> atende os clientes (REP conectado no broker)      //
//   thread servidores -> atende os outros servidores (REP porta 6000)      //
//   thread publicacoes-> SUB no proxy (coordenador e replicacao)           //
//   thread eleicao    -> faz a eleicao quando precisa                      //
// -------------------------------------------------------------------------//


// ---------------- Rank ----------------
int rank = Comunicacao.PedirRank();
lock (Estado.Trava) Estado.Rank = rank;

// ---------------- Threads ----------------
Iniciar(Coordenacao.AtenderServidores, "servidores");
Iniciar(Coordenacao.OuvirPublicacoes, "publicacoes");
Thread.Sleep(500);

// ---------------- Sincroniza com quem ja esta rodando ----------------
Coordenacao.SincronizarComCluster();

// ---------------- Eleicao ao entrar ----------------
Log.Escrever($"entrando e pedindo eleição - Rank: {rank}", doisPontos: true);
lock (Estado.Trava) Estado.EleicaoPendente = true;
Iniciar(Coordenacao.LoopEleicao, "eleicao");

// ---------------- Atendimento dos clientes ----------------
using var socketClientes = new ResponseSocket();
socketClientes.Connect(Estado.BROKER);

using var pub = new PublisherSocket();
pub.Connect(Estado.PROXY_PUB);

int contadorHeartbeat = 0;
int contadorRelogioFisico = 0;

while (true)
{
    var bytes = socketClientes.ReceiveFrameBytes();
    var envelope = Envelope.Parser.ParseFrom(bytes);
    Relogio.Atualizar(envelope.Relogio);

    Resposta res = TratarCliente(envelope);
    res.TimestampMs = Relogio.Agora();
    res.Relogio = Relogio.Avancar();
    socketClientes.SendFrame(res.ToByteArray());


    contadorHeartbeat++;
    contadorRelogioFisico++;

    if (contadorHeartbeat >= Estado.MENSAGENS_HEARTBEAT)
    {
        Comunicacao.EnviarHeartbeat();
        contadorHeartbeat = 0;
    }

    if (contadorRelogioFisico >= Estado.MENSAGENS_RELOGIO_FISICO)
    {
        Coordenacao.SincronizarRelogio();
        contadorRelogioFisico = 0;
    }
}


// ---------------------------------------------------------------------//
// Funcoes                                                              //
// ---------------------------------------------------------------------//
static void Iniciar(Action acao, string nome)
{
    new Thread(() => acao()) { IsBackground = true, Name = nome }.Start();
}

Resposta TratarCliente(Envelope envelope)
{

    switch (envelope.CorpoCase)
    {
        case Envelope.CorpoOneofCase.Login:
            return Login(envelope);

        case Envelope.CorpoOneofCase.Canal:
            switch (envelope.Canal.CorpoCase)
            {
                case Canal.CorpoOneofCase.Criar: return CriarCanal(envelope);
                case Canal.CorpoOneofCase.Listar: return ListarCanais();
                case Canal.CorpoOneofCase.Post: return Postar(envelope);
            }
            break;
    }

    return new Resposta { Sucesso = false, Erro = "Pedido inválido" };
}

Resposta Login(Envelope envelope)
{
    string usuario = envelope.Login.Usuario;
    long ts = envelope.TimestampMs;

    if (!Estado.Dados.RegistrarLogin(usuario, ts))
        return new Resposta { Sucesso = false };

    Replicar(new Replicacao { Login = new RegistroLogin { Usuario = usuario, TimestampMs = ts } });
    return new Resposta { Sucesso = true };
}

Resposta CriarCanal(Envelope envelope)
{
    string nome = envelope.Canal.Criar;
    long ts = envelope.TimestampMs;

    if (nome.Contains(Estado.SEP))
        return new Resposta { Sucesso = false, Erro = $"Nome de canal não pode conter '{Estado.SEP}'" };
    if (!Estado.Dados.CriarCanal(nome, ts))
        return new Resposta { Sucesso = false, Erro = "Canal já criado" };

    Replicar(new Replicacao { Canal = new RegistroCanal { Nome = nome, TimestampMs = ts } });
    return new Resposta { Sucesso = true };
}

Resposta ListarCanais()
{
    var res = new Resposta { Sucesso = true };
    res.Canais.AddRange(Estado.Dados.ListarCanais());
    return res;
}

Resposta Postar(Envelope envelope)
{
    var post = envelope.Canal.Post;
    long ts = envelope.TimestampMs;

    if (!Estado.Dados.CanalExiste(post.Canal))
        return new Resposta { Sucesso = false, Erro = "Canal não existe" };

    Estado.Dados.Postar(post.Canal, post.Mensagem, ts);

    // publica a mensagem no canal (topico = nome do canal + separador)
    envelope.Relogio = Relogio.Avancar();
    pub.SendMoreFrame(post.Canal + Estado.SEP).SendFrame(envelope.ToByteArray());

    Replicar(new Replicacao { Mensagem = new RegistroMensagem { Canal = post.Canal, Texto = post.Mensagem, TimestampMs = ts } });
    return new Resposta { Sucesso = true };
}

void Replicar(Replicacao rep)
{
    rep.OrigemServidor = Estado.ID_REPLICA;
    rep.Relogio = Relogio.Avancar();
    pub.SendMoreFrame(Estado.CANAL_REPLICACAO + Estado.SEP).SendFrame(rep.ToByteArray());
}
