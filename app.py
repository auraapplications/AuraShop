import os
import json
import base64
import secrets
import requests
import threading
import time
import queue
from datetime import datetime
from functools import wraps

from flask import (Flask, render_template, request, redirect, url_for,
                   session, flash, jsonify, abort, Response, stream_with_context)
from werkzeug.utils import secure_filename
from dotenv import load_dotenv

from database import get_conn, close_conn, init_db, UPLOAD_DIR

load_dotenv()

app = Flask(__name__)
app.secret_key = os.getenv("SECRET_KEY", secrets.token_hex(32))

# Usa UPLOAD_DIR do database (funciona tanto local quanto Vercel /tmp)
os.makedirs(UPLOAD_DIR, exist_ok=True)
app.config["UPLOAD_FOLDER"] = UPLOAD_DIR
ALLOWED_EXTENSIONS = {"png", "jpg", "jpeg", "gif", "webp"}

init_db()

# ── SSE — broadcast de eventos em tempo real ─────────────────
# Cada cliente conectado ao /events recebe uma fila individual.
# Quando algo muda (produto editado, pedido confirmado), chamamos
# sse_broadcast() e todos os clientes recebem o evento.

_sse_clients: list[queue.Queue] = []
_sse_lock = threading.Lock()

def sse_broadcast(event: str, data: dict):
    msg = f"event: {event}\ndata: {json.dumps(data)}\n\n"
    with _sse_lock:
        dead = []
        for q in _sse_clients:
            try:
                q.put_nowait(msg)
            except queue.Full:
                dead.append(q)
        for q in dead:
            _sse_clients.remove(q)

@app.route("/events")
def sse_stream():
    q: queue.Queue = queue.Queue(maxsize=20)
    with _sse_lock:
        _sse_clients.append(q)
    def generate():
        try:
            # keepalive inicial
            yield ": keepalive\n\n"
            while True:
                try:
                    msg = q.get(timeout=25)
                    yield msg
                except queue.Empty:
                    yield ": keepalive\n\n"
        finally:
            with _sse_lock:
                if q in _sse_clients:
                    _sse_clients.remove(q)
    return Response(stream_with_context(generate()),
                    mimetype="text/event-stream",
                    headers={"Cache-Control": "no-cache",
                             "X-Accel-Buffering": "no"})

# ── Polling de pagamentos (roda em background) ────────────────
# Consulta a API do MP a cada 8s pra checar pagamentos pendentes.
# Funciona mesmo sem webhook (dev local, sem domínio público).

def _verificar_mp_payment(payment_id: str, token: str) -> bool:
    """Retorna True se o pagamento foi aprovado no MP."""
    try:
        r = requests.get(
            f"https://api.mercadopago.com/v1/payments/{payment_id}",
            headers={"Authorization": f"Bearer {token}"},
            timeout=10
        )
        status = r.json().get("status", "")
        return status == "approved"
    except Exception:
        return False

def _polling_loop():
    """Thread de background que verifica pagamentos pendentes."""
    # Espera o servidor iniciar
    time.sleep(5)
    while True:
        try:
            # Cria conexão própria pra essa thread
            import sqlite3 as _sq
            from database import DB_PATH
            con = _sq.connect(DB_PATH, timeout=15)
            con.execute("PRAGMA journal_mode = WAL")
            con.execute("PRAGMA foreign_keys = OFF")
            con.row_factory = _sq.Row

            # Busca configurações
            row = con.execute("SELECT valor FROM config WHERE chave='mp_token'").fetchone()
            mp_token = row["valor"] if row else ""
            row2 = con.execute("SELECT valor FROM config WHERE chave='purincash_token'").fetchone()
            purincash_token = row2["valor"] if row2 else ""

            if mp_token:
                pendentes = con.execute(
                    """SELECT * FROM pedidos
                       WHERE status='pendente' AND notificado=0
                         AND payment_id != '' AND payment_provider='mercadopago'"""
                ).fetchall()

                for pedido in pendentes:
                    if _verificar_mp_payment(str(pedido["payment_id"]), mp_token):
                        con.execute("UPDATE pedidos SET status='pago', notificado=1 WHERE id=? AND status='pendente'", (pedido["id"],))
                        con.commit()
                        sse_broadcast("payment_confirmed", {"pedido_id": pedido["id"], "discord_id": pedido["discord_id"]})
                        try:
                            produto = con.execute("SELECT * FROM produtos WHERE id=?", (pedido["produto_id"],)).fetchone()
                            plano   = con.execute("SELECT * FROM planos WHERE id=?",   (pedido["plano_id"],)).fetchone()
                            if produto and plano:
                                notificar_bot(dict(pedido), dict(produto), dict(plano))
                        except Exception:
                            pass

            if purincash_token:
                pendentes_pc = con.execute(
                    """SELECT * FROM pedidos
                       WHERE status='pendente' AND notificado=0
                         AND payment_id != '' AND payment_provider='purincash'"""
                ).fetchall()
                for pedido in pendentes_pc:
                    try:
                        r = requests.get(
                            f"https://api.purincash.com/v1/charges/{pedido['payment_id']}",
                            headers={"Authorization": f"Bearer {purincash_token}"},
                            timeout=10
                        )
                        if r.json().get("status") == "paid":
                            con.execute("UPDATE pedidos SET status='pago', notificado=1 WHERE id=? AND status='pendente'", (pedido["id"],))
                            con.commit()
                            sse_broadcast("payment_confirmed", {"pedido_id": pedido["id"], "discord_id": pedido["discord_id"]})
                            try:
                                produto = con.execute("SELECT * FROM produtos WHERE id=?", (pedido["produto_id"],)).fetchone()
                                plano   = con.execute("SELECT * FROM planos WHERE id=?",   (pedido["plano_id"],)).fetchone()
                                if produto and plano:
                                    notificar_bot(dict(pedido), dict(produto), dict(plano))
                            except Exception:
                                pass
                    except Exception:
                        pass
            con.close()
        except Exception:
            pass
        time.sleep(8)

# Inicia o polling em background (daemon=True morre junto com o processo)
_poll_thread = threading.Thread(target=_polling_loop, daemon=True)
_poll_thread.start()

# Fecha a conexão SQLite ao final de cada request
@app.teardown_appcontext
def teardown_db(exc):
    close_conn()

# ── Helpers ──────────────────────────────────────────────────

def allowed_file(filename):
    return "." in filename and filename.rsplit(".", 1)[1].lower() in ALLOWED_EXTENSIONS

def get_config():
    rows = get_conn().execute("SELECT chave, valor FROM config").fetchall()
    return {r["chave"]: r["valor"] for r in rows}

def set_config(chave, valor):
    con = get_conn()
    con.execute("INSERT OR REPLACE INTO config (chave, valor) VALUES (?, ?)", (chave, valor))
    con.commit()

def set_configs(dados: dict):
    con = get_conn()
    for chave, valor in dados.items():
        con.execute("INSERT OR REPLACE INTO config (chave, valor) VALUES (?, ?)", (chave, valor))
    con.commit()

def login_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if not session.get("admin"):
            return redirect(url_for("admin_login"))
        return f(*args, **kwargs)
    return decorated

def notificar_discord(pedido, produto, plano):
    """Mantido por compatibilidade — chama notificar_bot que já faz tudo."""
    notificar_bot(pedido, produto, plano)
def notificar_bot(pedido, produto, plano):
    """Chama a API interna do bot para mandar DM ao comprador,
    registrar no banco do bot e logar no canal de vendas."""
    cfg = get_config()
    bot_url = cfg.get("bot_api_url", "").rstrip("/")
    bot_key = cfg.get("bot_api_key", "")
    if not bot_url or not bot_key:
        return
    try:
        requests.post(
            f"{bot_url}/api/notify-sale",
            headers={"X-API-Key": bot_key, "Content-Type": "application/json"},
            json={
                "discord_id":   str(pedido.get("discord_id", "")),
                "produto_nome": produto.get("titulo", ""),
                "plano_nome":   plano.get("nome", ""),
                "valor":        float(pedido.get("valor", 0)),
                "pedido_id":    str(pedido.get("id", "")),
                "provedor":     str(pedido.get("payment_provider", "")),
                "produto_id":   produto.get("id"),
                "plano_id":     plano.get("id"),
                "dias":         int(plano.get("dias", 30)),
            },
            timeout=10
        )
    except Exception as e:
        import logging
        logging.getLogger("botstore").warning(f"[notificar_bot] falhou: {e}")

# ── Rotas públicas ───────────────────────────────────────────

@app.route("/")
def index():
    cfg = get_config()
    con = get_conn()
    produtos = con.execute("SELECT * FROM produtos ORDER BY id DESC").fetchall()
    resultado = []
    for p in produtos:
        row = con.execute("SELECT MIN(preco) as mp FROM planos WHERE produto_id=?", (p["id"],)).fetchone()
        resultado.append({**dict(p), "min_preco": row["mp"] or p["preco_base"]})
    return render_template("index.html", cfg=cfg, produtos=resultado)

@app.route("/produto/<int:pid>")
def produto(pid):
    cfg = get_config()
    con = get_conn()
    p = con.execute("SELECT * FROM produtos WHERE id=?", (pid,)).fetchone()
    if not p:
        abort(404)
    planos = con.execute("SELECT * FROM planos WHERE produto_id=? ORDER BY preco", (pid,)).fetchall()
    return render_template("produto.html", cfg=cfg, produto=dict(p),
                           planos=[dict(pl) for pl in planos])

@app.route("/checkout", methods=["POST"])
def checkout():
    cfg = get_config()
    produto_id = request.form.get("produto_id")
    plano_id   = request.form.get("plano_id")
    discord_id = request.form.get("discord_id", "").strip()

    if not all([produto_id, plano_id, discord_id]):
        flash("Preencha todos os campos.", "error")
        return redirect(request.referrer or url_for("index"))

    con = get_conn()
    produto_row = con.execute("SELECT * FROM produtos WHERE id=?", (produto_id,)).fetchone()
    plano_row   = con.execute("SELECT * FROM planos WHERE id=? AND produto_id=?",
                              (plano_id, produto_id)).fetchone()
    if not produto_row or not plano_row:
        abort(404)

    provedor       = cfg.get("provedor_pix_ativo", "")
    pix_copia_cola = ""
    pix_qr_base64  = ""
    payment_id     = ""

    # Aplica cupom se informado
    cupom_codigo = request.form.get("cupom_codigo", "").strip().upper()
    desconto = 0.0
    valor_final = float(plano_row["preco"])
    if cupom_codigo:
        con2 = get_conn()
        cupom = con2.execute(
            "SELECT * FROM cupons WHERE codigo=? COLLATE NOCASE AND ativo=1", (cupom_codigo,)
        ).fetchone()
        if cupom and (cupom["limite_usos"] == 0 or cupom["usos"] < cupom["limite_usos"]):
            if cupom["tipo"] == "porcentagem":
                desconto = round(valor_final * cupom["valor"] / 100, 2)
            else:
                desconto = min(float(cupom["valor"]), valor_final)
            valor_final = max(0.01, round(valor_final - desconto, 2))
            con2.execute("UPDATE cupons SET usos=usos+1 WHERE id=?", (cupom["id"],))
            con2.commit()
        else:
            cupom_codigo = ""

    if provedor == "mercadopago":
        pix_copia_cola, pix_qr_base64, payment_id = _gerar_pix_mp(cfg, valor_final, produto_row["titulo"])
    elif provedor == "pagbank":
        pix_copia_cola, pix_qr_base64, payment_id = _gerar_pix_pagbank(cfg, valor_final, produto_row["titulo"])
    elif provedor == "efi":
        pix_copia_cola, pix_qr_base64, payment_id = _gerar_pix_efi(cfg, valor_final, produto_row["titulo"])
    elif provedor == "purincash":
        cb = request.url_root.rstrip("/") + "/webhook/purincash"
        pix_copia_cola, pix_qr_url_tmp, payment_id = _gerar_pix_purincash(
            cfg, valor_final, produto_row["titulo"], "tmp", cb)
        pix_qr_base64 = pix_qr_url_tmp  # guarda URL no campo base64, tratado no template
    else:
        pix_copia_cola = "CONFIGURE_UM_PROVEDOR_PIX_NO_ADMIN"
        pix_qr_base64  = ""

    cur = con.execute(
        """INSERT INTO pedidos (produto_id, plano_id, discord_id, valor, payment_id,
           payment_provider, pix_copia_cola, pix_qr_base64, cupom_codigo, desconto)
           VALUES (?,?,?,?,?,?,?,?,?,?)""",
        (produto_id, plano_id, discord_id, valor_final,
         payment_id, provedor, pix_copia_cola, pix_qr_base64, cupom_codigo, desconto)
    )
    pedido_id = cur.lastrowid
    con.commit()

    return render_template("checkout.html", cfg=cfg,
                           produto=dict(produto_row), plano=dict(plano_row),
                           discord_id=discord_id, pedido_id=pedido_id,
                           pix_copia_cola=pix_copia_cola, pix_qr_base64=pix_qr_base64,
                           valor_final=valor_final, desconto=desconto, cupom_codigo=cupom_codigo)

@app.route("/pedido/<int:pid>/status")
def pedido_status(pid):
    row = get_conn().execute("SELECT status FROM pedidos WHERE id=?", (pid,)).fetchone()
    if not row:
        return jsonify({"status": "not_found"})
    return jsonify({"status": row["status"]})

# ── Helpers Pix ───────────────────────────────────────────────

def _gerar_pix_mp(cfg, valor, descricao):
    token = cfg.get("mp_token", "")
    if not token:
        return "", "", ""
    try:
        resp = requests.post(
            "https://api.mercadopago.com/v1/payments",
            headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json",
                     "X-Idempotency-Key": secrets.token_hex(16)},
            json={"transaction_amount": float(valor), "description": descricao[:50],
                  "payment_method_id": "pix", "payer": {"email": "cliente@botstore.com"}},
            timeout=15)
        data = resp.json()
        td = data.get("point_of_interaction", {}).get("transaction_data", {})
        return td.get("qr_code", ""), td.get("qr_code_base64", ""), str(data.get("id", ""))
    except Exception:
        return "", "", ""

def _gerar_pix_pagbank(cfg, valor, descricao):
    token = cfg.get("pagbank_token", "")
    env   = cfg.get("pagbank_env", "sandbox")
    if not token:
        return "", "", ""
    base = "https://sandbox.api.pagseguro.com" if env == "sandbox" else "https://api.pagseguro.com"
    try:
        resp = requests.post(f"{base}/orders",
            headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
            json={"reference_id": secrets.token_hex(8),
                  "customer": {"name": "Cliente", "email": "cliente@botstore.com", "tax_id": "00000000000"},
                  "items": [{"name": descricao[:50], "quantity": 1, "unit_amount": int(float(valor)*100)}],
                  "qr_codes": [{"amount": {"value": int(float(valor)*100)}}]},
            timeout=15)
        data = resp.json()
        qr = data.get("qr_codes", [{}])[0]
        return qr.get("text", ""), "", str(data.get("id", ""))
    except Exception:
        return "", "", ""

def _gerar_pix_efi(cfg, valor, descricao):
    cid = cfg.get("efi_client_id", "")
    cs  = cfg.get("efi_client_secret", "")
    env = cfg.get("efi_env", "sandbox")
    if not cid or not cs:
        return "", "", ""
    base = "https://pix-h.api.efipay.com.br" if env == "sandbox" else "https://pix.api.efipay.com.br"
    try:
        creds = base64.b64encode(f"{cid}:{cs}".encode()).decode()
        auth  = requests.post(f"{base}/oauth/token",
                              headers={"Authorization": f"Basic {creds}"},
                              json={"grant_type": "client_credentials"}, timeout=10)
        at = auth.json().get("access_token", "")
        resp = requests.post(f"{base}/v2/cob",
            headers={"Authorization": f"Bearer {at}", "Content-Type": "application/json"},
            json={"calendario": {"expiracao": 3600}, "valor": {"original": f"{float(valor):.2f}"},
                  "chave": cid, "infoAdicionais": [{"nome": "Produto", "valor": descricao[:50]}]},
            timeout=15)
        data = resp.json()
        loc_id = data.get("loc", {}).get("id", "")
        qr = requests.get(f"{base}/v2/loc/{loc_id}/qrcode",
                          headers={"Authorization": f"Bearer {at}"}, timeout=10).json()
        return qr.get("qrcode", ""), qr.get("imagemQrcode", "").replace("data:image/png;base64,",""), data.get("txid","")
    except Exception:
        return "", "", ""

def _gerar_pix_purincash(cfg, valor, descricao, pedido_id, callback_url=""):
    token = cfg.get("purincash_token", "")
    if not token:
        return "", "", ""
    try:
        valor_cents = int(round(float(valor) * 100))
        payload = {
            "valueCents":   valor_cents,
            "description":  descricao[:200],
            "customer":     {"externalId": str(pedido_id)},
        }
        if callback_url:
            payload["callbackUrl"] = callback_url
        resp = requests.post(
            "https://api.purincash.com/v1/charges",
            headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
            json=payload,
            timeout=15
        )
        data = resp.json()
        if resp.status_code not in (200, 201) or "pix" not in data:
            return "", "", ""
        br_code   = data["pix"].get("brCode", "")
        qr_img    = data["pix"].get("qrCodeImage", "")  # URL da imagem, não base64
        payment_id = data.get("paymentId", "")
        return br_code, qr_img, payment_id
    except Exception:
        return "", "", ""

# ── Webhooks ──────────────────────────────────────────────────

def _confirmar_pedido(payment_id):
    con = get_conn()
    pedido = con.execute("SELECT * FROM pedidos WHERE payment_id=?", (payment_id,)).fetchone()
    if pedido and pedido["status"] == "pendente":
        con.execute("UPDATE pedidos SET status='pago', notificado=1 WHERE payment_id=?", (payment_id,))
        con.commit()
        produto = con.execute("SELECT * FROM produtos WHERE id=?", (pedido["produto_id"],)).fetchone()
        plano   = con.execute("SELECT * FROM planos WHERE id=?",   (pedido["plano_id"],)).fetchone()
        if produto and plano:
            notificar_bot(dict(pedido), dict(produto), dict(plano))

@app.route("/webhook/mp",      methods=["POST"])
def webhook_mp():
    data = request.json or {}
    if data.get("type") == "payment":
        _confirmar_pedido(str(data.get("data", {}).get("id", "")))
    return jsonify({"ok": True})

@app.route("/webhook/pagbank", methods=["POST"])
def webhook_pagbank():
    data = request.json or {}
    if data.get("charges"):
        for c in data["charges"]:
            if c.get("status") == "PAID":
                _confirmar_pedido(str(data.get("id", "")))
    return jsonify({"ok": True})

@app.route("/webhook/efi",     methods=["POST"])
def webhook_efi():
    for pix in (request.json or {}).get("pix", []):
        _confirmar_pedido(pix.get("txid", ""))
    return jsonify({"ok": True})

@app.route("/webhook/purincash", methods=["POST"])
def webhook_purincash():
    data = request.json or {}
    event = data.get("event", "")
    if event in ("charge.paid", "payment.paid"):
        pid = data.get("paymentId", "")
        if pid:
            _confirmar_pedido(pid)
    return jsonify({"ok": True})

# ── Validar cupom (público, chamado via JS no checkout) ──────

@app.route("/cupom/validar", methods=["POST"])
def cupom_validar():
    codigo = (request.json or {}).get("codigo", "").strip()
    plano_id = (request.json or {}).get("plano_id")
    if not codigo:
        return jsonify({"ok": False, "erro": "Informe o código do cupom."})
    con = get_conn()
    cupom = con.execute(
        "SELECT * FROM cupons WHERE codigo=? COLLATE NOCASE AND ativo=1", (codigo,)
    ).fetchone()
    if not cupom:
        return jsonify({"ok": False, "erro": "Cupom inválido ou inativo."})
    if cupom["limite_usos"] > 0 and cupom["usos"] >= cupom["limite_usos"]:
        return jsonify({"ok": False, "erro": "Cupom esgotado."})
    # Calcula desconto com base no plano
    desconto = 0.0
    if plano_id:
        plano = con.execute("SELECT preco FROM planos WHERE id=?", (plano_id,)).fetchone()
        if plano:
            if cupom["tipo"] == "porcentagem":
                desconto = round(plano["preco"] * cupom["valor"] / 100, 2)
            else:
                desconto = min(float(cupom["valor"]), plano["preco"])
    return jsonify({
        "ok": True,
        "tipo": cupom["tipo"],
        "valor": cupom["valor"],
        "desconto": desconto,
        "codigo": cupom["codigo"],
    })

@app.route("/admin/login", methods=["GET", "POST"])
def admin_login():
    cfg = get_config()
    if request.method == "POST":
        if request.form.get("senha", "") == cfg.get("admin_password", "admin123"):
            session["admin"] = True
            return redirect(url_for("admin_dashboard"))
        flash("Senha incorreta.", "error")
    return render_template("admin/login.html", cfg=cfg)

@app.route("/admin/logout")
def admin_logout():
    session.clear()
    return redirect(url_for("admin_login"))

# ── Admin: dashboard ──────────────────────────────────────────

@app.route("/admin/")
@app.route("/admin")
@login_required
def admin_dashboard():
    cfg = get_config()
    con = get_conn()
    total_produtos = con.execute("SELECT COUNT(*) as c FROM produtos").fetchone()["c"]
    total_pedidos  = con.execute("SELECT COUNT(*) as c FROM pedidos").fetchone()["c"]
    receita        = con.execute("SELECT SUM(valor) as s FROM pedidos WHERE status != 'pendente'").fetchone()["s"] or 0
    ultimos        = con.execute(
        """SELECT p.*, pr.titulo as produto_nome, pl.nome as plano_nome
           FROM pedidos p
           LEFT JOIN produtos pr ON p.produto_id=pr.id
           LEFT JOIN planos   pl ON p.plano_id=pl.id
           ORDER BY p.id DESC LIMIT 8"""
    ).fetchall()
    return render_template("admin/dashboard.html", cfg=cfg,
                           total_produtos=total_produtos, total_pedidos=total_pedidos,
                           receita=receita, ultimos=[dict(u) for u in ultimos])

# ── Admin: produtos ───────────────────────────────────────────

@app.route("/admin/produtos")
@login_required
def admin_produtos():
    cfg = get_config()
    con = get_conn()
    produtos = con.execute("SELECT * FROM produtos ORDER BY id DESC").fetchall()
    resultado = []
    for p in produtos:
        planos = con.execute("SELECT * FROM planos WHERE produto_id=?", (p["id"],)).fetchall()
        resultado.append({**dict(p), "planos": [dict(pl) for pl in planos]})
    return render_template("admin/produtos.html", cfg=cfg, produtos=resultado)

@app.route("/admin/produtos/novo", methods=["GET", "POST"])
@login_required
def admin_produto_novo():
    cfg = get_config()
    if request.method == "POST":
        titulo    = request.form.get("titulo", "").strip()
        descricao = request.form.get("descricao", "").strip()
        app_id    = request.form.get("discloud_app_id", "").strip()
        imagem_path = ""

        f = request.files.get("imagem")
        if f and f.filename and allowed_file(f.filename):
            fname = secrets.token_hex(8) + "_" + secure_filename(f.filename)
            f.save(os.path.join(app.config["UPLOAD_FOLDER"], fname))
            imagem_path = f"/static/uploads/{fname}"

        nomes  = request.form.getlist("plano_nome[]")
        precos = request.form.getlist("plano_preco[]")
        dias   = request.form.getlist("plano_dias[]")
        preco_base = float(precos[0]) if precos else 0

        con = get_conn()
        cur = con.execute(
            "INSERT INTO produtos (titulo, descricao, imagem, preco_base, discloud_app_id) VALUES (?,?,?,?,?)",
            (titulo, descricao, imagem_path, preco_base, app_id)
        )
        pid = cur.lastrowid
        for i, nome in enumerate(nomes):
            if nome.strip():
                con.execute(
                    "INSERT INTO planos (produto_id, nome, preco, dias) VALUES (?,?,?,?)",
                    (pid, nome.strip(), float(precos[i] or 0), int(dias[i] or 30))
                )
        con.commit()
        flash("Produto criado com sucesso!", "success")
        sse_broadcast("produto_atualizado", {"action": "created", "id": pid})
        return redirect(url_for("admin_produtos"))

    return render_template("admin/produto_form.html", cfg=cfg, produto=None, planos=[])

@app.route("/admin/produtos/<int:pid>/editar", methods=["GET", "POST"])
@login_required
def admin_produto_editar(pid):
    cfg = get_config()
    con = get_conn()
    produto = con.execute("SELECT * FROM produtos WHERE id=?", (pid,)).fetchone()
    if not produto:
        abort(404)

    if request.method == "POST":
        titulo    = request.form.get("titulo", "").strip()
        descricao = request.form.get("descricao", "").strip()
        app_id    = request.form.get("discloud_app_id", "").strip()
        imagem_path = produto["imagem"]

        f = request.files.get("imagem")
        if f and f.filename and allowed_file(f.filename):
            fname = secrets.token_hex(8) + "_" + secure_filename(f.filename)
            f.save(os.path.join(app.config["UPLOAD_FOLDER"], fname))
            imagem_path = f"/static/uploads/{fname}"

        nomes  = request.form.getlist("plano_nome[]")
        precos = request.form.getlist("plano_preco[]")
        dias   = request.form.getlist("plano_dias[]")
        preco_base = float(precos[0]) if precos else 0

        con.execute(
            "UPDATE produtos SET titulo=?, descricao=?, imagem=?, preco_base=?, discloud_app_id=? WHERE id=?",
            (titulo, descricao, imagem_path, preco_base, app_id, pid)
        )
        con.execute("DELETE FROM planos WHERE produto_id=?", (pid,))
        for i, nome in enumerate(nomes):
            if nome.strip():
                con.execute(
                    "INSERT INTO planos (produto_id, nome, preco, dias) VALUES (?,?,?,?)",
                    (pid, nome.strip(), float(precos[i] or 0), int(dias[i] or 30))
                )
        con.commit()
        flash("Produto atualizado!", "success")
        sse_broadcast("produto_atualizado", {"action": "updated", "id": pid})
        return redirect(url_for("admin_produtos"))

    planos = con.execute("SELECT * FROM planos WHERE produto_id=?", (pid,)).fetchall()
    return render_template("admin/produto_form.html", cfg=cfg,
                           produto=dict(produto), planos=[dict(pl) for pl in planos])

@app.route("/admin/produtos/<int:pid>/deletar", methods=["POST"])
@login_required
def admin_produto_deletar(pid):
    con = get_conn()
    con.execute("DELETE FROM planos WHERE produto_id=?", (pid,))
    con.execute("DELETE FROM produtos WHERE id=?", (pid,))
    con.commit()
    flash("Produto deletado.", "success")
    return redirect(url_for("admin_produtos"))

# ── Admin: pedidos ────────────────────────────────────────────

@app.route("/admin/pedidos")
@login_required
def admin_pedidos():
    cfg = get_config()
    pedidos = get_conn().execute(
        """SELECT p.*, pr.titulo as produto_nome, pl.nome as plano_nome
           FROM pedidos p
           LEFT JOIN produtos pr ON p.produto_id=pr.id
           LEFT JOIN planos   pl ON p.plano_id=pl.id
           ORDER BY p.id DESC"""
    ).fetchall()
    return render_template("admin/pedidos.html", cfg=cfg, pedidos=[dict(p) for p in pedidos])

@app.route("/admin/pedidos/<int:pid>")
@login_required
def admin_pedido_detalhe(pid):
    cfg = get_config()
    pedido = get_conn().execute(
        """SELECT p.*, pr.titulo as produto_nome, pl.nome as plano_nome
           FROM pedidos p
           LEFT JOIN produtos pr ON p.produto_id=pr.id
           LEFT JOIN planos   pl ON p.plano_id=pl.id
           WHERE p.id=?""", (pid,)
    ).fetchone()
    if not pedido:
        abort(404)
    return render_template("admin/pedido_detalhe.html", cfg=cfg, pedido=dict(pedido))

@app.route("/admin/pedidos/<int:pid>/entregar", methods=["POST"])
@login_required
def admin_pedido_entregar(pid):
    con = get_conn()
    con.execute("UPDATE pedidos SET status='entregue', entregue_em=datetime('now') WHERE id=?", (pid,))
    con.commit()
    flash("Pedido marcado como entregue.", "success")
    return redirect(url_for("admin_pedidos"))

@app.route("/admin/pedidos/<int:pid>/confirmar", methods=["POST"])
@login_required
def admin_pedido_confirmar(pid):
    con = get_conn()
    pedido = con.execute("SELECT * FROM pedidos WHERE id=?", (pid,)).fetchone()
    if pedido:
        con.execute("UPDATE pedidos SET status='pago' WHERE id=?", (pid,))
        con.commit()
        produto = con.execute("SELECT * FROM produtos WHERE id=?", (pedido["produto_id"],)).fetchone()
        plano   = con.execute("SELECT * FROM planos WHERE id=?",   (pedido["plano_id"],)).fetchone()
        if produto and plano:
            notificar_bot(dict(pedido), dict(produto), dict(plano))
    flash("Pedido confirmado.", "success")
    return redirect(url_for("admin_pedidos"))

# ── Admin: configurações ──────────────────────────────────────

@app.route("/admin/configuracoes", methods=["GET", "POST"])
@login_required
def admin_configuracoes():
    cfg = get_config()
    if request.method == "POST":
        nova_senha = request.form.get("admin_password", "").strip()
        bot_api_key_novo = request.form.get("bot_api_key", "").strip()
        updates = {
            "nome_loja":       request.form.get("nome_loja", "BotStore"),
            "descricao_loja":  request.form.get("descricao_loja", ""),
            "admin_password":  nova_senha if nova_senha else cfg.get("admin_password", "admin123"),
            "discord_webhook": request.form.get("discord_webhook", ""),
            "bot_api_url":     request.form.get("bot_api_url", "").rstrip("/"),
        }
        if bot_api_key_novo:
            updates["bot_api_key"] = bot_api_key_novo
        set_configs(updates)
        flash("Configurações salvas!", "success")
        return redirect(url_for("admin_configuracoes"))
    return render_template("admin/configuracoes.html", cfg=cfg)

@app.route("/admin/test-webhook", methods=["POST"])
@login_required
def admin_test_webhook():
    cfg = get_config()
    webhook = cfg.get("discord_webhook", "")
    if not webhook:
        return jsonify({"ok": False})
    try:
        r = requests.post(webhook, json={"embeds": [{
            "title": "✅ Teste de Webhook",
            "description": "Integração com Discord funcionando!",
            "color": 0x7c3aed,
            "footer": {"text": cfg.get("nome_loja", "BotStore")}
        }]}, timeout=5)
        return jsonify({"ok": r.status_code in (200, 204)})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})

@app.route("/admin/test-bot-api", methods=["POST"])
@login_required
def admin_test_bot_api():
    cfg = get_config()
    # Aceita tanto do body JSON (direto do JS) quanto do banco
    body = request.get_json(silent=True) or {}
    url = (body.get("url") or cfg.get("bot_api_url", "")).rstrip("/")
    key = body.get("key") or cfg.get("bot_api_key", "")
    if not url or not key:
        return jsonify({"ok": False, "error": "URL ou API Key não configurados."})
    try:
        r = requests.get(f"{url}/health",
                         headers={"X-API-Key": key}, timeout=6)
        data = r.json()
        if r.status_code == 200 and data.get("ok"):
            return jsonify({"ok": True, "bot_user": data.get("bot_user", "?")})
        return jsonify({"ok": False, "error": f"HTTP {r.status_code}: {data.get('erro', 'resposta inesperada')}"})
    except requests.exceptions.ConnectionError:
        return jsonify({"ok": False, "error": "Não conseguiu conectar. Verifique a URL."})
    except requests.exceptions.Timeout:
        return jsonify({"ok": False, "error": "Timeout — bot demorou demais pra responder."})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})

# ── Admin: Pix ────────────────────────────────────────────────

@app.route("/admin/pix", methods=["GET", "POST"])
@login_required
def admin_pix():
    cfg = get_config()
    if request.method == "POST":
        # Campos de seleção (ambiente e provedor) sempre atualizam
        updates = {
            "provedor_pix_ativo":  request.form.get("provedor_pix_ativo", ""),
            "pagbank_env":         request.form.get("pagbank_env", "sandbox"),
            "efi_env":             request.form.get("efi_env", "sandbox"),
        }
        # Tokens e credenciais só atualizam se o campo veio preenchido
        for campo in ("mp_token", "inter_client_id", "inter_client_secret",
                      "pagbank_token", "efi_client_id", "efi_client_secret",
                      "purincash_token"):
            val = request.form.get(campo, "").strip()
            if val:
                updates[campo] = val
        set_configs(updates)
        flash("Configurações de Pix salvas!", "success")
        return redirect(url_for("admin_pix"))
    return render_template("admin/pix_config.html", cfg=cfg)

# ── Admin: DisCloud ───────────────────────────────────────────

DISCLOUD_API = "https://api.discloud.app/v2"

def dc_headers(token):
    return {"api-token": token}

@app.route("/admin/discloud", methods=["GET", "POST"])
@login_required
def admin_discloud():
    cfg = get_config()
    if request.method == "POST":
        novo_token = request.form.get("discloud_token", "").strip()
        if novo_token:
            set_config("discloud_token", novo_token)
            flash("Token DisCloud atualizado!", "success")
        else:
            flash("Nenhum token informado. O token atual foi mantido.", "error")
        return redirect(url_for("admin_discloud"))
    return render_template("admin/discloud.html", cfg=cfg)

@app.route("/admin/discloud/apps")
@login_required
def admin_discloud_apps():
    token = get_config().get("discloud_token", "")
    if not token:
        return jsonify({"error": "Token não configurado"}), 400
    try:
        resp = requests.get(f"{DISCLOUD_API}/app/all", headers=dc_headers(token), timeout=15)
        return jsonify(resp.json())
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/admin/discloud/status/<app_id>")
@login_required
def admin_discloud_status(app_id):
    token = get_config().get("discloud_token", "")
    if not token:
        return jsonify({"error": "Token não configurado"}), 400
    try:
        resp = requests.get(f"{DISCLOUD_API}/app/{app_id}/status", headers=dc_headers(token), timeout=15)
        return jsonify(resp.json())
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/admin/discloud/restart/<app_id>", methods=["POST"])
@login_required
def admin_discloud_restart(app_id):
    token = get_config().get("discloud_token", "")
    if not token:
        return jsonify({"error": "Token não configurado"}), 400
    try:
        resp = requests.put(f"{DISCLOUD_API}/app/{app_id}/restart", headers=dc_headers(token), timeout=15)
        return jsonify(resp.json())
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/admin/discloud/logs/<app_id>")
@login_required
def admin_discloud_logs(app_id):
    token = get_config().get("discloud_token", "")
    if not token:
        return jsonify({"error": "Token não configurado"}), 400
    try:
        resp = requests.get(f"{DISCLOUD_API}/app/{app_id}/logs", headers=dc_headers(token), timeout=15)
        return jsonify(resp.json())
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/admin/discloud/delete/<app_id>", methods=["POST"])
@login_required
def admin_discloud_delete(app_id):
    token = get_config().get("discloud_token", "")
    if not token:
        return jsonify({"error": "Token não configurado"}), 400
    try:
        resp = requests.delete(f"{DISCLOUD_API}/app/{app_id}/delete", headers=dc_headers(token), timeout=15)
        return jsonify(resp.json())
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# ── Admin: cupons ─────────────────────────────────────────────

@app.route("/admin/cupons")
@login_required
def admin_cupons():
    cfg = get_config()
    cupons = get_conn().execute("SELECT * FROM cupons ORDER BY id DESC").fetchall()
    return render_template("admin/cupons.html", cfg=cfg, cupons=[dict(c) for c in cupons])

@app.route("/admin/cupons/novo", methods=["POST"])
@login_required
def admin_cupom_novo():
    codigo      = request.form.get("codigo", "").strip().upper()
    tipo        = request.form.get("tipo", "porcentagem")
    valor       = float(request.form.get("valor", 0))
    limite_usos = int(request.form.get("limite_usos", 0))
    if not codigo or valor <= 0:
        flash("Preencha código e valor.", "error")
        return redirect(url_for("admin_cupons"))
    try:
        con = get_conn()
        con.execute(
            "INSERT INTO cupons (codigo, tipo, valor, limite_usos) VALUES (?,?,?,?)",
            (codigo, tipo, valor, limite_usos)
        )
        con.commit()
        flash(f"Cupom {codigo} criado!", "success")
    except Exception:
        flash("Código já existe.", "error")
    return redirect(url_for("admin_cupons"))

@app.route("/admin/cupons/<int:cid>/toggle", methods=["POST"])
@login_required
def admin_cupom_toggle(cid):
    con = get_conn()
    cupom = con.execute("SELECT ativo FROM cupons WHERE id=?", (cid,)).fetchone()
    if cupom:
        con.execute("UPDATE cupons SET ativo=? WHERE id=?", (0 if cupom["ativo"] else 1, cid))
        con.commit()
    return redirect(url_for("admin_cupons"))

@app.route("/admin/cupons/<int:cid>/deletar", methods=["POST"])
@login_required
def admin_cupom_deletar(cid):
    con = get_conn()
    con.execute("DELETE FROM cupons WHERE id=?", (cid,))
    con.commit()
    flash("Cupom deletado.", "success")
    return redirect(url_for("admin_cupons"))

# ── Admin: destaque ───────────────────────────────────────────

@app.route("/admin/produtos/<int:pid>/destaque", methods=["POST"])
@login_required
def admin_produto_destaque(pid):
    con = get_conn()
    p = con.execute("SELECT destaque FROM produtos WHERE id=?", (pid,)).fetchone()
    if p:
        con.execute("UPDATE produtos SET destaque=? WHERE id=?", (0 if p["destaque"] else 1, pid))
        con.commit()
    return redirect(url_for("admin_produtos"))

# ── Admin: banner e configs visuais ───────────────────────────

@app.route("/admin/visual", methods=["GET", "POST"])
@login_required
def admin_visual():
    cfg = get_config()
    if request.method == "POST":
        updates = {
            "banner_ativo":      "1" if request.form.get("banner_ativo") else "0",
            "banner_texto":      request.form.get("banner_texto", ""),
            "banner_cor":        request.form.get("banner_cor", "primary"),
            "dm_mensagem_extra": request.form.get("dm_mensagem_extra", ""),
        }
        set_configs(updates)
        flash("Configurações visuais salvas!", "success")
        return redirect(url_for("admin_visual"))
    return render_template("admin/visual.html", cfg=cfg)

# ── Serve uploads ─────────────────────────────────────────────

@app.route("/static/uploads/<filename>")
def uploaded_file(filename):
    from flask import send_from_directory
    return send_from_directory(app.config["UPLOAD_FOLDER"], filename)

# ── Main ──────────────────────────────────────────────────────

if __name__ == "__main__":
    app.run(debug=True, port=5000)
