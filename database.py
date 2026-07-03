import sqlite3
import os
import threading

# Na Vercel o filesystem é read-only exceto /tmp
# Detecta se está rodando na Vercel pelo env var
_IS_VERCEL = os.getenv("VERCEL") == "1" or os.getenv("VERCEL_ENV") is not None

if _IS_VERCEL:
    DB_PATH = "/tmp/shop.db"
    # Copia os uploads pra /tmp também
    UPLOAD_DIR = "/tmp/uploads"
else:
    DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "shop.db")
    UPLOAD_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "static", "uploads")

os.makedirs(UPLOAD_DIR, exist_ok=True)

# Uma conexão por thread — evita "database is locked" com requisições simultâneas
_local = threading.local()

def get_conn() -> sqlite3.Connection:
    if not hasattr(_local, "conn") or _local.conn is None:
        con = sqlite3.connect(DB_PATH, timeout=30, check_same_thread=False)
        con.execute("PRAGMA journal_mode = WAL")
        con.execute("PRAGMA busy_timeout = 10000")
        con.execute("PRAGMA foreign_keys = OFF")
        con.row_factory = sqlite3.Row
        _local.conn = con
    else:
        # Garante que foreign_keys está OFF mesmo em conexões reaproveitadas
        _local.conn.execute("PRAGMA foreign_keys = OFF")
    return _local.conn

def close_conn():
    if hasattr(_local, "conn") and _local.conn:
        _local.conn.close()
        _local.conn = None

def init_db():
    con = get_conn()
    cur = con.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS config (
            chave TEXT PRIMARY KEY,
            valor TEXT
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS produtos (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            titulo          TEXT NOT NULL,
            descricao       TEXT,
            imagem          TEXT,
            preco_base      REAL DEFAULT 0,
            discloud_app_id TEXT,
            tipo            TEXT DEFAULT 'bot',
            destaque        INTEGER DEFAULT 0,
            criado_em       TEXT DEFAULT (datetime('now'))
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS planos (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            produto_id INTEGER NOT NULL,
            nome       TEXT NOT NULL,
            preco      REAL NOT NULL,
            dias       INTEGER DEFAULT 30,
            FOREIGN KEY (produto_id) REFERENCES produtos(id)
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS pedidos (
            id               INTEGER PRIMARY KEY AUTOINCREMENT,
            produto_id       INTEGER,
            plano_id         INTEGER,
            discord_id       TEXT,
            valor            REAL,
            status           TEXT DEFAULT 'pendente',
            payment_id       TEXT,
            payment_provider TEXT,
            pix_copia_cola   TEXT,
            pix_qr_base64    TEXT,
            notificado       INTEGER DEFAULT 0,
            criado_em        TEXT DEFAULT (datetime('now')),
            entregue_em      TEXT,
            FOREIGN KEY (produto_id) REFERENCES produtos(id),
            FOREIGN KEY (plano_id)   REFERENCES planos(id)
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS carrinho (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            session_id TEXT NOT NULL,
            produto_id INTEGER NOT NULL,
            plano_id   INTEGER NOT NULL,
            discord_id TEXT,
            criado_em  TEXT DEFAULT (datetime('now'))
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS discord_users (
            discord_id   TEXT PRIMARY KEY,
            username     TEXT,
            discriminator TEXT,
            avatar       TEXT,
            email        TEXT,
            access_token TEXT,
            refresh_token TEXT,
            ultimo_login TEXT DEFAULT (datetime('now'))
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS cupons (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            codigo      TEXT NOT NULL UNIQUE COLLATE NOCASE,
            tipo        TEXT NOT NULL DEFAULT 'porcentagem',
            valor       REAL NOT NULL,
            ativo       INTEGER DEFAULT 1,
            usos        INTEGER DEFAULT 0,
            limite_usos INTEGER DEFAULT 0,
            criado_em   TEXT DEFAULT (datetime('now'))
        )
    """)

    # Migrações
    for migration in [
        "ALTER TABLE pedidos ADD COLUMN notificado INTEGER DEFAULT 0",
        "ALTER TABLE pedidos ADD COLUMN cupom_codigo TEXT",
        "ALTER TABLE pedidos ADD COLUMN desconto REAL DEFAULT 0",
        "ALTER TABLE produtos ADD COLUMN destaque INTEGER DEFAULT 0",
        "ALTER TABLE produtos ADD COLUMN tipo TEXT DEFAULT 'bot'",
        "ALTER TABLE produtos ADD COLUMN pre_venda INTEGER DEFAULT 0",
        "ALTER TABLE produtos ADD COLUMN pre_venda_desconto REAL DEFAULT 0",
        "ALTER TABLE produtos ADD COLUMN pre_venda_data TEXT",
        "ALTER TABLE produtos ADD COLUMN tag_novo INTEGER DEFAULT 0",
        "ALTER TABLE produtos ADD COLUMN tag_novo_ate TEXT",
    ]:
        try:
            cur.execute(migration)
        except Exception:
            pass

    defaults = {
        "nome_loja":          "BotStore",
        "descricao_loja":     "Os melhores bots e sites para o seu servidor Discord",
        "admin_password":     "admin123",
        "provedor_pix_ativo": "",
        "mp_token":           "",
        "inter_client_id":    "",
        "inter_client_secret":"",
        "pagbank_token":      "",
        "pagbank_env":        "sandbox",
        "efi_client_id":      "",
        "efi_client_secret":  "",
        "efi_env":            "sandbox",
        "discloud_token":     "",
        "discord_webhook":    "",
        "cor_primaria":       "#7c3aed",
        "bot_api_url":        os.getenv("BOT_API_URL", ""),
        "bot_api_key":        os.getenv("BOT_API_KEY", ""),
        "banner_ativo":       "0",
        "banner_texto":       "",
        "banner_cor":         "primary",
        "dm_mensagem_extra":  "",
        "purincash_token":    "",
        "discord_client_id":  os.getenv("DISCORD_CLIENT_ID", ""),
        "discord_client_secret": os.getenv("DISCORD_CLIENT_SECRET", ""),
    }
    for chave, valor in defaults.items():
        cur.execute(
            "INSERT OR IGNORE INTO config (chave, valor) VALUES (?, ?)",
            (chave, valor)
        )

    con.commit()
