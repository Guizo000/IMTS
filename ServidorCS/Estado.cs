using System.Net;

// -------------------------------------------------------------------------//
// Estado compartilhado do servidor em C#.                                   //
// Diferente do servidor em Python (que faz tudo num loop so com poller),    //
// aqui cada socket fica numa thread propria, entao o estado que e mexido    //
// por mais de uma thread fica protegido por lock.                           //
// -------------------------------------------------------------------------//
static class Estado
{
    public static readonly string ID_REPLICA = Environment.GetEnvironmentVariable("SERVER_ID") ?? Dns.GetHostName();
    public const int PORTA_SERVIDORES = 6000;
    public const string SEP = "|";
    public const string CANAL_SERVERS = "servers";
    public const string CANAL_REPLICACAO = "replicacao";
    public const int MENSAGENS_HEARTBEAT = 10;
    public const int MENSAGENS_RELOGIO_FISICO = 15;

    public static readonly string BROKER = "tcp://broker:5556";
    public static readonly string REFERENCIA = "tcp://referencia:5559";
    public static readonly string PROXY_PUB = "tcp://proxy:5557";
    public static readonly string PROXY_SUB = "tcp://proxy:5558";

    public static readonly Armazenamento Dados = new($"/dados/{ID_REPLICA}.db");

    public static readonly object Trava = new();

    // muda durante a execucao (sempre acessar com lock (Trava))
    public static int Rank;
    public static string? CoordenadorAtual;
    public static bool EleicaoPendente;
    public static long? AguardandoAnuncioAte;
}


static class Relogio
{
    static long relogioLogico = 0;
    static long offsetFisico = 0;
    static readonly object trava = new();

    public static long Agora()
    {
        lock (trava)
            return DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() + offsetFisico;
    }

    public static string Formatar(long timestampMs) =>
        DateTimeOffset.FromUnixTimeMilliseconds(timestampMs).ToString("yyyy-MM-dd HH:mm:ss.fff") + " UTC";

    public static void AtualizarFisico(long timestampMs)
    {
        lock (trava)
            offsetFisico = timestampMs - DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
            Console.WriteLine($"{Relogio.Formatar(Relogio.Agora())} Servidor: {Estado.ID_REPLICA} atualizando seu relógio físico");
    }

    public static long Avancar()
    {
        lock (trava)
            return ++relogioLogico;
    }

    public static void Atualizar(long recebido)
    {
        lock (trava)
            relogioLogico = Math.Max(relogioLogico, recebido);
    }

    public static long Valor
    {
        get { lock (trava) return relogioLogico; }
    }
}


static class Log
{
    static readonly object trava = new();

    // mesmo formato dos prints do servidor em Python
    public static void Escrever(string texto, bool doisPontos = false)
    {
        lock (trava)
            Console.WriteLine($"{Relogio.Formatar(Relogio.Agora())} Servidor{(doisPontos ? ":" : "")} {Estado.ID_REPLICA} {texto}");
    }
}
