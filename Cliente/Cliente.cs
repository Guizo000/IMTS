using System.Diagnostics;
using Google.Protobuf;
using NetMQ;
using NetMQ.Sockets;

// -------------//
// Configuração //
// -------------//
var rnd = new Random();

const string SEP = "|";            
const int MIN_CANAIS = 5;          
const int MAX_INSCRICOES = 3;     
const int MSGS_POR_CANAL = 10;     
const int INTERVALO_MS = 1000;

string endpoint = Environment.GetEnvironmentVariable("SERVIDOR_ENDERECO") ?? "tcp://broker:5555";
string endpointSub = Environment.GetEnvironmentVariable("PROXY_SUB_ENDERECO") ?? "tcp://proxy:5558";
string usuario = $"bot-{rnd.Next(5000)}";
Console.WriteLine($"{usuario} entrando");

using var socket = new RequestSocket();
socket.Connect(endpoint);

// --------//
// Relógio //
// --------//
long relogio_logico = 0;
object trava = new();

long ProximoRelogio()
{
    lock (trava)
    {
        relogio_logico++;
        return relogio_logico;
    }
}

void AtualizarRelogio(long recebido)
{
    lock (trava)
    {
        relogio_logico = Math.Max(relogio_logico, recebido);
        Console.WriteLine($"Relógio Lógico: {relogio_logico}");
    }
}

// ------//
// Login //
// ------//
while (true)
{
    var reqLogin = new Envelope
    {
        TimestampMs = Agora(),
        Login = new Login { Usuario = usuario }
    };

    if (TratarResposta(Enviar(reqLogin, "Login"), "Login"))
        break;

    Thread.Sleep(1000);
}

// -------------//
// Criar Canais //
// -------------//
Resposta lista = ListarCanais();
while (lista.Canais.Count < MIN_CANAIS)
{
    CriarCanal($"Canal{rnd.Next(100)}");
    lista = ListarCanais();
}

// -----------------------//
// Inscrição Em Canais    //
// -----------------------//
var inscritos = new List<string>();
while (inscritos.Count < MAX_INSCRICOES)
{
    var candidatos = lista.Canais.Except(inscritos).ToList();
    if (candidatos.Count == 0)
        break;

    inscritos.Add(candidatos[rnd.Next(candidatos.Count)]);
}
Console.WriteLine($"{Formatar(Agora())} - Inscrito em: {string.Join(", ", inscritos)}");

// -------------------------------//
// Thread para imprimir mensagens //
// -------------------------------//
var assinados = inscritos.ToList();              
using var inscricoesProntas = new ManualResetEventSlim(false);

var receptor = new Thread(() =>
{
    using var sub = new SubscriberSocket();
    sub.Connect(endpointSub);

    foreach (var canal in assinados)
        sub.Subscribe(canal + SEP);              

    inscricoesProntas.Set();

    while (true)
    {
        var frames = sub.ReceiveMultipartBytes();   
        long recebidoMs = Agora();

        var env = Envelope.Parser.ParseFrom(frames[1]);
        AtualizarRelogio(env.Relogio);
        var post = env.Canal.Post;

        //Console.WriteLine($"[{post.Canal}] {post.Mensagem} | enviada: {Formatar(env.TimestampMs)} | recebida: {Formatar(recebidoMs)}");
    }
})
{ IsBackground = true };
receptor.Start();

// Espera a assinatura existir e propagar pelo proxy antes de começar a postar
inscricoesProntas.Wait();
Thread.Sleep(500);


// ------------------------//
// Loop Envio de Mensagens //
// ------------------------//
while (true)
{
    lista = ListarCanais();
    while(lista.Canais.Count == 0)
    {
        Thread.Sleep(1000);
        lista = ListarCanais();    
    }

    string canal = lista.Canais[rnd.Next(lista.Canais.Count)];
    Console.WriteLine($"{usuario} irá postar 10 mensagens em {canal}");

    for (int i = 0; i < MSGS_POR_CANAL; i++)
    {
        var sw = Stopwatch.StartNew();

        Postar(canal, $"Mensagem {rnd.Next(10000)} de {usuario}");

        // O servidor demora para responder; descontamos esse tempo para manter ~1 s entre envios
        int resto = INTERVALO_MS - (int)sw.ElapsedMilliseconds;
        if (resto > 0)
            Thread.Sleep(resto);
    }
}


// ----------//
// Funções   //
// ----------//
static long Agora() => DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

static string Formatar(long timestampMs)
{
    DateTimeOffset dt = DateTimeOffset.FromUnixTimeMilliseconds(timestampMs);
    return dt.ToString("yyyy-MM-dd HH:mm:ss.fff") + " UTC";
}

Resposta Enviar(Envelope req, string nome = "Sem Nome")
{
    //Console.WriteLine($"{Formatar(req.TimestampMs)} - Enviando requisição: {nome}");
    req.Relogio = ProximoRelogio();
    socket.SendFrame(req.ToByteArray());

    byte[] bytes = socket.ReceiveFrameBytes();
    Resposta res = Resposta.Parser.ParseFrom(bytes);

    AtualizarRelogio(res.Relogio);
    return res;
}

bool TratarResposta(Resposta res, string nome = "Sem Nome")
{
    if (res.Sucesso)
    {
        //Console.WriteLine($"{Formatar(Agora())} - Resposta de {nome} recebida - Sucesso");
        return true;
    }

    //Console.WriteLine($"{Formatar(Agora())} - Resposta de {nome} recebida - Erro: {res.Erro}");
    return false;
}

Resposta ListarCanais()
{
    var req = new Envelope
    {
        TimestampMs = Agora(),
        Canal = new Canal { Listar = new Canal.Types.Listar() }
    };

    Resposta res = Enviar(req, "Listar Canais");
    TratarResposta(res, "Listar Canais");
    return res;
}

void CriarCanal(string nome)
{
    var req = new Envelope
    {
        TimestampMs = Agora(),
        Canal = new Canal { Criar = nome }
    };

    TratarResposta(Enviar(req, $"Criar Canal ({nome})"), "Criar Canal");
}

void Postar(string canal, string texto)
{
    var req = new Envelope
    {
        TimestampMs = Agora(),  
        Canal = new Canal
        {
            Post = new Canal.Types.Post { Canal = canal, Mensagem = texto }
        }
    };

    TratarResposta(Enviar(req, $"Post em {canal}"), "Post");
}