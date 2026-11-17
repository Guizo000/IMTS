import os
import sqlite3

class Armazenamento:
    def __init__(self, caminho: str):
        os.makedirs(os.path.dirname(caminho), exist_ok=True)
        self.db = sqlite3.connect(caminho)
        self.db.executescript("""
            CREATE TABLE IF NOT EXISTS logins (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                usuario TEXT NOT NULL,
                timestamp_ms INTEGER NOT NULL,
                UNIQUE(usuario, timestamp_ms) -- #
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
                UNIQUE(canal, texto, timestamp_ms) -- #
            );
        """)
        self.db.commit()

    def registrar_login(self, usuario: str, ts: int) -> bool:
        with self.db:
            cur = self.db.execute(
                "INSERT OR IGNORE INTO logins (usuario, timestamp_ms) VALUES (?, ?)", #
                (usuario, ts)
            )
        return cur.rowcount == 1

    def criar_canal(self, nome: str, ts: int) -> bool:
        with self.db:
            cur = self.db.execute(
                "INSERT OR IGNORE INTO canais (nome, timestamp_ms) VALUES (?, ?)", #
                (nome, ts)
            )
        return cur.rowcount == 1

    def listar_canais(self) -> list[str]:
        rows = self.db.execute("SELECT nome FROM canais ORDER BY timestamp_ms ASC, nome ASC") #
        return [r[0] for r in rows]
    
    def canal_existe(self, nome: str) -> bool:
        return self.db.execute(
            "SELECT 1 FROM canais WHERE nome = ?", (nome,)
        ).fetchone() is not None

    def postar(self, canal: str, texto: str, ts: int) -> bool: #
        with self.db:
            self.db.execute( #
                "INSERT OR IGNORE INTO canais (nome, timestamp_ms) VALUES (?, ?)", #
                (canal, ts) #
            ) #
            cur = self.db.execute( #
                "INSERT OR IGNORE INTO mensagens (canal, texto, timestamp_ms) VALUES (?, ?, ?)", #
                (canal, texto, ts) #
            ) #
        return cur.rowcount == 1 #

    def listar_mensagens(self, canal: str) -> list[tuple[str, int]]: #
        rows = self.db.execute( #
            "SELECT texto, timestamp_ms FROM mensagens WHERE canal = ? ORDER BY timestamp_ms ASC, id ASC", #
            (canal,) #
        ) #
        return [(r[0], r[1]) for r in rows] #

    def exportar_tudo(self) -> dict: #
        logins = self.db.execute( #
            "SELECT usuario, timestamp_ms FROM logins ORDER BY timestamp_ms ASC" #
        ).fetchall() #
        canais = self.db.execute( #
            "SELECT nome, timestamp_ms FROM canais ORDER BY timestamp_ms ASC" #
        ).fetchall() #
        mensagens = self.db.execute( #
            "SELECT canal, texto, timestamp_ms FROM mensagens ORDER BY timestamp_ms ASC" #
        ).fetchall() #
        return { #
            "logins": logins, #
            "canais": canais, #
            "mensagens": mensagens #
        } #

    def importar_tudo(self, logins: list, canais: list, mensagens: list): #
        with self.db: #
            self.db.executemany( #
                "INSERT OR IGNORE INTO logins (usuario, timestamp_ms) VALUES (?, ?)", #
                logins #
            ) #
            self.db.executemany( #
                "INSERT OR IGNORE INTO canais (nome, timestamp_ms) VALUES (?, ?)", #
                canais #
            ) #
            self.db.executemany( #
                "INSERT OR IGNORE INTO mensagens (canal, texto, timestamp_ms) VALUES (?, ?, ?)", #
                mensagens #
            ) #