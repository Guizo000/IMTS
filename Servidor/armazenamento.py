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
                timestamp_ms INTEGER NOT NULL
            );
            CREATE TABLE IF NOT EXISTS canais (
                nome TEXT PRIMARY KEY,
                timestamp_ms INTEGER NOT NULL
            );
            CREATE TABLE IF NOT EXISTS mensagens (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                canal TEXT NOT NULL REFERENCES canais(nome),
                texto TEXT NOT NULL,
                timestamp_ms INTEGER NOT NULL
            );
        """)
        self.db.commit()

    def registrar_login(self, usuario: str, ts: int) -> bool:
        with self.db:
            cur = self.db.execute(
                "INSERT INTO logins VALUES (?, ?)", (usuario, ts))
        return cur.rowcount == 1

    def criar_canal(self, nome: str, ts: int) -> bool:
        with self.db:
            cur = self.db.execute(
                "INSERT OR IGNORE INTO canais VALUES (?, ?)", (nome, ts))
        return cur.rowcount == 1

    def listar_canais(self) -> list[str]:
        rows = self.db.execute("SELECT nome FROM canais ORDER BY nome")
        return [r[0] for r in rows]
    
    def canal_existe(self, nome: str) -> bool:
        return self.db.execute(
            "SELECT 1 FROM canais WHERE nome = ?", (nome,)).fetchone() is not None

    def postar(self, canal: str, texto: str, ts: int):
        with self.db:
            self.db.execute(
                "INSERT INTO mensagens (canal, texto, timestamp_ms) "
                "VALUES (?, ?, ?)", (canal, texto, ts)) 