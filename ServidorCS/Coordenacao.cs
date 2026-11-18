using Google.Protobuf;
using NetMQ;
using NetMQ.Sockets;

// -------------------------------------------------------------------------//
// Tudo que e conversa entre servidores: eleicao (valentao), horario do      //
// coordenador, sincronizacao do estado e o topico de publicacoes.           //
// -------------------------------------------------------------------------//
static class Coordenacao
{
    const int ESPERA_ANUNCIO_MS = 5000;

    // ---------------------------------------------------------------- //
    // Thread que atende os outros servidores (REP na porta 6000).       //
    // Fica numa thread so pra isso, entao o servidor sempre responde    //
    // o pedido de eleicao, mesmo se estiver ocupado com outra coisa.   //
    // ---------------------------------------------------------------- //
    public static void AtenderServidores()
    {
        using var rep = new ResponseSocket();
        rep.Bind($"tcp://*:{Estado.PORTA_SERVIDORES}");

        while (true)
        {
            var msg = MensagemServidor.Parser.ParseFrom(rep.ReceiveFrameBytes());
            Relogio.Atualizar(msg.Relogio);

            var tipo = msg.CorpoCase == MensagemServidor.CorpoOneofCase.Pedido
                ? msg.Pedido.CorpoCase
                : PedidoServidor.CorpoOneofCase.None;

            var res = new MensagemServidor { Resposta = new RespostaServidor() };
            switch (tipo)
            {
                case PedidoServidor.CorpoOneofCase.Horario:
                    res.Resposta.Horario = new RespostaHorario();
                    break;

                case PedidoServidor.CorpoOneofCase.Eleicao:
                    res.Resposta.Eleicao = new Eleicao();
                    lock (Estado.Trava) Estado.EleicaoPendente = true;
                    break;

                case PedidoServidor.CorpoOneofCase.Sincronizar:
                    var (logins, canais, mensagens) = Estado.Dados.ExportarTudo();
                    var ec = new EstadoCompleto();
                    ec.Logins.AddRange(logins.Select(l => new RegistroLogin { Usuario = l.Usuario, TimestampMs = l.TimestampMs }));
                    ec.Canais.AddRange(canais.Select(c => new RegistroCanal { Nome = c.Nome, TimestampMs = c.TimestampMs }));
                    ec.Mensagens.AddRange(mensagens.Select(m => new RegistroMensagem { Canal = m.Canal, Texto = m.Texto, TimestampMs = m.TimestampMs }));
                    res.Resposta.EstadoCompleto = ec;
                    break;
            }

            res.TimestampMs = Relogio.Agora();
            res.Relogio = Relogio.Avancar();
            rep.SendFrame(res.ToByteArray());
        }
    }

    // ---------------------------------------------------------------- //
    // Thread que escuta o proxy: anuncio de coordenador e replicacao    //
    // ---------------------------------------------------------------- //
    public static void OuvirPublicacoes()
    {
        using var sub = new SubscriberSocket();
        sub.Connect(Estado.PROXY_SUB);
        sub.Subscribe(Estado.CANAL_SERVERS + Estado.SEP);
        sub.Subscribe(Estado.CANAL_REPLICACAO + Estado.SEP);

        while (true)
        {
            var frames = sub.ReceiveMultipartBytes();
            var topico = System.Text.Encoding.UTF8.GetString(frames[0]);

            if (topico == Estado.CANAL_SERVERS + Estado.SEP)
            {
                var pub = PublicacaoCoordenador.Parser.ParseFrom(frames[1]);
                Relogio.Atualizar(pub.Relogio);
                Log.Escrever($"recebeu publicação de novo coordenador: {pub.Mensagem}");

                lock (Estado.Trava)
                {
                    Estado.CoordenadorAtual = pub.Mensagem;
                    Estado.AguardandoAnuncioAte = null;

                    // valentao: se eu tenho rank maior que o anunciado, eu tomo o lugar
                    if (pub.Mensagem != Estado.ID_REPLICA && pub.Rank < Estado.Rank)
                        Estado.EleicaoPendente = true;
                }
            }
            else if (topico == Estado.CANAL_REPLICACAO + Estado.SEP)
            {
                var rep = Replicacao.Parser.ParseFrom(frames[1]);
                Relogio.Atualizar(rep.Relogio);
                if (rep.OrigemServidor == Estado.ID_REPLICA)
                    continue;

                switch (rep.OperacaoCase)
                {
                    case Replicacao.OperacaoOneofCase.Login:
                        Estado.Dados.RegistrarLogin(rep.Login.Usuario, rep.Login.TimestampMs);
                        break;
                    case Replicacao.OperacaoOneofCase.Canal:
                        Estado.Dados.CriarCanal(rep.Canal.Nome, rep.Canal.TimestampMs);
                        break;
                    case Replicacao.OperacaoOneofCase.Mensagem:
                        Estado.Dados.Postar(rep.Mensagem.Canal, rep.Mensagem.Texto, rep.Mensagem.TimestampMs);
                        break;
                }
            }
        }
    }

    // ---------------------------------------------------------------- //
    // Thread da eleicao: fica olhando se precisa fazer eleicao          //
    // ---------------------------------------------------------------- //
    public static void LoopEleicao()
    {
        using var pub = new PublisherSocket();
        pub.Connect(Estado.PROXY_PUB);
        Thread.Sleep(500);  // da tempo do PUB conectar no proxy

        while (true)
        {
            bool fazer;
            lock (Estado.Trava)
            {
                fazer = Estado.EleicaoPendente;
                if (!fazer && Estado.AguardandoAnuncioAte != null && Relogio.Agora() > Estado.AguardandoAnuncioAte)
                    fazer = true;
            }

            if (fazer)
                PedirEleicao(pub);

            Thread.Sleep(200);
        }
    }

    static void PedirEleicao(PublisherSocket pub)
    {
        int meuRank;
        lock (Estado.Trava)
        {
            Estado.EleicaoPendente = false;
            meuRank = Estado.Rank;
        }

        var maiores = Comunicacao.PedirServidores()
            .Where(s => s.Nome != Estado.ID_REPLICA && s.Rank > meuRank)
            .OrderByDescending(s => s.Rank);

        foreach (var servidor in maiores)
        {
            var pedido = new MensagemServidor { Pedido = new PedidoServidor { Eleicao = new Eleicao() } };

            if (Comunicacao.PedirServidor(servidor.Nome, pedido) != null)
            {
                lock (Estado.Trava)
                {
                    Estado.CoordenadorAtual = null;
                    Estado.AguardandoAnuncioAte = Relogio.Agora() + ESPERA_ANUNCIO_MS;
                }
                return;
            }
        }

        // ninguem maior respondeu: eu sou o coordenador
        lock (Estado.Trava)
        {
            Estado.CoordenadorAtual = Estado.ID_REPLICA;
            Estado.AguardandoAnuncioAte = null;
            Estado.EleicaoPendente = false;
        }
        Log.Escrever($"eleito coordenador - Publicando aviso - Rank: {meuRank}");

        var anuncio = new PublicacaoCoordenador
        {
            Mensagem = Estado.ID_REPLICA,
            Rank = meuRank,
            Relogio = Relogio.Avancar(),
            TimestampMs = Relogio.Agora()
        };
        pub.SendMoreFrame(Estado.CANAL_SERVERS + Estado.SEP).SendFrame(anuncio.ToByteArray());
    }

    // ---------------------------------------------------------------- //
    // Pedido do horario ao coordenador (chamado pela thread principal)  //
    // ---------------------------------------------------------------- //
    public static void SincronizarRelogio()
    {
        string? coordenador;
        lock (Estado.Trava)
        {
            if (Estado.CoordenadorAtual == Estado.ID_REPLICA || Estado.AguardandoAnuncioAte != null)
                return;
            coordenador = Estado.CoordenadorAtual;
        }

        MensagemServidor? res = null;
        if (coordenador != null)
        {
            var pedido = new MensagemServidor { Pedido = new PedidoServidor { Horario = new PedidoHorario() } };
            res = Comunicacao.PedirServidor(coordenador, pedido);
        }

        if (res == null)
        {
            Log.Escrever($"pedindo eleição por ausência do coordenador - Rank: {Estado.Rank}");
            lock (Estado.Trava) Estado.EleicaoPendente = true;
            return;
        }

        Relogio.AtualizarFisico(res.TimestampMs);
    }

    // ---------------------------------------------------------------- //
    // Ao entrar, pede o estado completo pra algum servidor ativo         //
    // ---------------------------------------------------------------- //
    public static void SincronizarComCluster()
    {
        foreach (var servidor in Comunicacao.PedirServidores())
        {
            if (servidor.Nome == Estado.ID_REPLICA)
                continue;

            var pedido = new MensagemServidor { Pedido = new PedidoServidor { Sincronizar = new SincronizarEstado() } };
            var res = Comunicacao.PedirServidor(servidor.Nome, pedido);
            if (res?.Resposta?.EstadoCompleto == null)
                continue;

            var ec = res.Resposta.EstadoCompleto;
            Estado.Dados.ImportarTudo(
                ec.Logins.Select(l => new LoginSalvo(l.Usuario, l.TimestampMs)),
                ec.Canais.Select(c => new CanalSalvo(c.Nome, c.TimestampMs)),
                ec.Mensagens.Select(m => new MensagemSalva(m.Canal, m.Texto, m.TimestampMs)));

            Log.Escrever($"sincronizou estado com {servidor.Nome} ({ec.Canais.Count} canais, {ec.Mensagens.Count} msgs)");
            return;
        }
    }
}
