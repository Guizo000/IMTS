using Microsoft.Data.Sqlite;

// -------------------------------------------------------------------------//
// Armazenamento do servidor em SQLite. A conexão é usada por mais de uma   //                                                   
// então todo acesso passa por lock                                         //
// -------------------------------------------------------------------------//
record LoginSalvo(string Usuario, long TimestampMs);
record CanalSalvo(string Nome, long TimestampMs);
record MensagemSalva(string Canal, string Texto, long TimestampMs);

class Armazenamento
{
    readonly SqliteConnection db;
    readonly object trava = new();

    public Armazenamento(string caminho)
    {
        Directory.CreateDirectory(Path.GetDirectoryName(caminho)!);
        db = new SqliteConnection($"Data Source={caminho}");
        db.Open();

        Executar(@"
            CREATE TABLE IF NOT EXISTS logins (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                usuario TEXT NOT NULL,
                timestamp_ms INTEGER NOT NULL,
                UNIQUE(usuario, timestamp_ms)
            );
            CREATE TABLE IF NOT EXISTS canais (
                nome TEXT PRIMARY KEY,
                timestamp_ms INTEGER NOT NULL
            );
            CREATE TABLE IF NOT EXISTS mensagens (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                canal TEXT NOT NULL REFERENCES canais(nome),
                texto TEXT NOT NULL,
                timestamp_ms INTEGER NOT NULL,
                UNIQUE(canal, texto, timestamp_ms)
            );");
    }

    // executa o comando e devolve quantas linhas foram alteradas
    int Executar(string sql, params (string nome, object valor)[] parametros)
    {
        using var cmd = db.CreateCommand();
        cmd.CommandText = sql;
        foreach (var (nome, valor) in parametros)
            cmd.Parameters.AddWithValue(nome, valor);
        return cmd.ExecuteNonQuery();
    }

    List<T> Consultar<T>(string sql, Func<SqliteDataReader, T> ler, params (string nome, object valor)[] parametros)
    {
        using var cmd = db.CreateCommand();
        cmd.CommandText = sql;
        foreach (var (nome, valor) in parametros)
            cmd.Parameters.AddWithValue(nome, valor);

        var lista = new List<T>();
        using var reader = cmd.ExecuteReader();
        while (reader.Read())
            lista.Add(ler(reader));
        return lista;
    }

    // ---- as funcoes devolvem false quando o registro ja existia (INSERT OR IGNORE) ----
    bool InserirLogin(string usuario, long ts) =>
        Executar("INSERT OR IGNORE INTO logins (usuario, timestamp_ms) VALUES ($u, $ts)", ("$u", usuario), ("$ts", ts)) == 1;

    bool InserirCanal(string nome, long ts) =>
        Executar("INSERT OR IGNORE INTO canais (nome, timestamp_ms) VALUES ($n, $ts)", ("$n", nome), ("$ts", ts)) == 1;

    bool InserirMensagem(string canal, string texto, long ts) =>
        Executar("INSERT OR IGNORE INTO mensagens (canal, texto, timestamp_ms) VALUES ($c, $t, $ts)",
                 ("$c", canal), ("$t", texto), ("$ts", ts)) == 1;

    public bool RegistrarLogin(string usuario, long ts)
    {
        lock (trava) return InserirLogin(usuario, ts);
    }

    public bool CriarCanal(string nome, long ts)
    {
        lock (trava) return InserirCanal(nome, ts);
    }

    public bool CanalExiste(string nome)
    {
        lock (trava)
            return Consultar("SELECT 1 FROM canais WHERE nome = $n", r => 1, ("$n", nome)).Count > 0;
    }

    public List<string> ListarCanais()
    {
        lock (trava)
            return Consultar("SELECT nome FROM canais ORDER BY timestamp_ms ASC, nome ASC", r => r.GetString(0));
    }

    public bool Postar(string canal, string texto, long ts)
    {
        lock (trava)
        {
            Executar("BEGIN");
            InserirCanal(canal, ts);   
            bool nova = InserirMensagem(canal, texto, ts);
            Executar("COMMIT");
            return nova;
        }
    }

    public (List<LoginSalvo>, List<CanalSalvo>, List<MensagemSalva>) ExportarTudo()
    {
        lock (trava)
            return (
                Consultar("SELECT usuario, timestamp_ms FROM logins ORDER BY timestamp_ms ASC",
                          r => new LoginSalvo(r.GetString(0), r.GetInt64(1))),
                Consultar("SELECT nome, timestamp_ms FROM canais ORDER BY timestamp_ms ASC",
                          r => new CanalSalvo(r.GetString(0), r.GetInt64(1))),
                Consultar("SELECT canal, texto, timestamp_ms FROM mensagens ORDER BY timestamp_ms ASC",
                          r => new MensagemSalva(r.GetString(0), r.GetString(1), r.GetInt64(2))));
    }

    public void ImportarTudo(IEnumerable<LoginSalvo> ls, IEnumerable<CanalSalvo> cs, IEnumerable<MensagemSalva> ms)
    {
        lock (trava)
        {
            Executar("BEGIN");
            foreach (var l in ls) InserirLogin(l.Usuario, l.TimestampMs);
            foreach (var c in cs) InserirCanal(c.Nome, c.TimestampMs);
            foreach (var m in ms) InserirMensagem(m.Canal, m.Texto, m.TimestampMs);
            Executar("COMMIT");
        }
    }
}
