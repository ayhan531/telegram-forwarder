import os
import io
import base64
import uuid
import asyncio
import glob
from fastapi import FastAPI, Request, Form, Depends, Query
from fastapi.responses import HTMLResponse, RedirectResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from sqlalchemy.orm import Session
from starlette.middleware.sessions import SessionMiddleware
from telethon import TelegramClient, events, functions, types
from dotenv import load_dotenv
import qrcode

import database
from database import SessionLocal, Rule, Account, MessageMapping, WordFilter, User, hash_password

load_dotenv()

app = FastAPI()
_SECRET = os.environ.get("SESSION_SECRET", "tg-forwarder-secret-2026")
_DATA_DIR = os.environ.get("DATA_DIR", ".")
app.add_middleware(SessionMiddleware, secret_key=_SECRET, max_age=86400 * 7)
app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")

# Global clients dict: account_id -> TelegramClient
clients = {}
# Temporary storage for login flows: phone -> (client, phone_code_hash)
login_sessions = {}
# QR login sessions: temp_id -> dict
qr_sessions = {}

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

def get_current_user(request: Request, db: Session = Depends(get_db)) -> User:
    """Session'dan mevcut kullanıcıyı döner. Giriş yapılmamışsa login'e yönlendirir."""
    user_id = request.session.get("user_id")
    if not user_id:
        # Exception yerine redirect — FastAPI route'larında HTTPException ile yakalanır
        return None
    return db.query(User).filter(User.id == user_id).first()

def require_user(request: Request, db: Session = Depends(get_db)) -> User:
    """Zorunlu auth — None ise login'e redirect eder."""
    user = get_current_user(request, db)
    if not user:
        raise _redirect_to_login()
    return user

class _redirect_to_login(Exception):
    pass

from fastapi.exception_handlers import http_exception_handler
from starlette.exceptions import HTTPException

@app.exception_handler(_redirect_to_login)
async def redirect_login_handler(request: Request, exc: _redirect_to_login):
    return RedirectResponse(url="/login", status_code=303)

def _resolve_chat_id(chat) -> str:
    """Telethon chat entity'sinden standart Telegram chat_id string'i üretir."""
    from telethon import utils as tg_utils
    raw_id = tg_utils.get_peer_id(chat)
    return str(raw_id)


def _reactions_to_str(reactions_results) -> str:
    """Reaksiyon listesini karşılaştırılabilir string'e çevirir."""
    if not reactions_results:
        return ""
    parts = []
    for r in reactions_results:
        reaction = r.reaction
        if hasattr(reaction, 'emoticon'):
            parts.append(f"{reaction.emoticon}:{r.count}")
        elif hasattr(reaction, 'document_id'):
            parts.append(f"custom_{reaction.document_id}:{r.count}")
    return "|".join(sorted(parts))


async def reaction_poll_loop(client: TelegramClient, account: Account, interval: int = 15):
    """
    Her `interval` saniyede bir kaynak mesajların reaksiyonlarını çek,
    değişen reaksiyonları hedefe ilet.
    Tek bir DB session kullanır — concurrent lock hatasını önler.
    """
    print(f"[{account.name}] 🔄 Reaksiyon polling başladı ({interval}s aralık)")
    await asyncio.sleep(12)  # Backfill tamamlansın

    while True:
        try:
            db = SessionLocal()
            try:
                rules = db.query(Rule).filter(
                    Rule.account_id == account.id,
                    Rule.is_active == True
                ).all()

                for rule in rules:
                    try:
                        src_msgs = await client.get_messages(int(rule.source_chat_id), limit=50)

                        for msg in src_msgs:
                            if not msg:
                                continue
                            if rule.sender_id and str(getattr(msg, 'sender_id', '')) != rule.sender_id:
                                continue
                            if not msg.reactions or not msg.reactions.results:
                                continue

                            current_str = _reactions_to_str(msg.reactions.results)
                            if not current_str:
                                continue

                            mapping = db.query(MessageMapping).filter(
                                MessageMapping.original_chat_id    == rule.source_chat_id,
                                MessageMapping.original_msg_id     == msg.id,
                                MessageMapping.destination_chat_id == rule.destination_id
                            ).first()

                            if not mapping:
                                # Backfill için geçici olarak bu session'ı kapat
                                db.close()
                                db = SessionLocal()
                                await backfill_mappings(client, account, limit=100)
                                mapping = db.query(MessageMapping).filter(
                                    MessageMapping.original_chat_id    == rule.source_chat_id,
                                    MessageMapping.original_msg_id     == msg.id,
                                    MessageMapping.destination_chat_id == rule.destination_id
                                ).first()

                            if not mapping:
                                continue

                            if (mapping.last_reactions or "") == current_str:
                                continue

                            new_reactions = [r.reaction for r in msg.reactions.results]
                            try:
                                await client(functions.messages.SendReactionRequest(
                                    peer=int(rule.destination_id),
                                    msg_id=mapping.forwarded_msg_id,
                                    reaction=new_reactions,
                                    add_to_recent=True
                                ))
                                mapping.last_reactions = current_str
                                db.commit()
                                print(f"[{account.name}] ✅ [POLL] Reaksiyon iletildi "
                                      f"src={msg.id} → dst={mapping.forwarded_msg_id} | {current_str}")
                            except Exception as e:
                                print(f"[{account.name}] ❌ [POLL] Reaksiyon gönderme hatası: {e}")

                    except Exception as e:
                        print(f"[{account.name}] ❌ [POLL] Rule hatası: {e}")
            finally:
                db.close()

        except Exception as e:
            print(f"[{account.name}] ❌ [POLL] Genel hata: {e}")

        await asyncio.sleep(interval)


async def backfill_mappings(client: TelegramClient, account: Account, limit: int = 200):
    """
    Her rule için kaynak kanaldaki son `limit` mesajı çeker.
    Hedef kanaldaki mesajlarla timestamp benzerliğine göre eşleştirir
    ve MessageMapping tablosuna yazar.
    """
    db = SessionLocal()
    try:
        rules = db.query(Rule).filter(
            Rule.account_id == account.id,
            Rule.is_active == True
        ).all()

        for rule in rules:
            src_id = rule.source_chat_id
            dst_id = rule.destination_id
            print(f"[{account.name}] Backfill başlıyor: {src_id} → {dst_id}")

            try:
                # Kaynak ve hedef mesajları çek
                src_msgs = await client.get_messages(int(src_id), limit=limit)
                dst_msgs = await client.get_messages(int(dst_id), limit=limit)

                # Hedef mesajları hızlı arama için index'le
                # (metin[:60], dakika) → dst_msg_id
                dst_index = {}
                for dm in dst_msgs:
                    if not dm:
                        continue
                    key_text = (dm.message or "")[:60]
                    key_ts   = round(dm.date.timestamp() / 60)
                    dst_index[(key_text, key_ts)]    = dm.id
                    dst_index[("__any__",  key_ts)]  = dm.id  # sadece timestamp

                mapped_count = 0
                for sm in src_msgs:
                    if not sm:
                        continue
                    # Sadece belirli göndericiden mi filtre var?
                    if rule.sender_id and str(getattr(sm, 'sender_id', '')) != rule.sender_id:
                        continue

                    # Zaten mapping var mı?
                    exists = db.query(MessageMapping).filter(
                        MessageMapping.original_chat_id == src_id,
                        MessageMapping.original_msg_id  == sm.id,
                        MessageMapping.destination_chat_id == dst_id
                    ).first()
                    if exists:
                        continue

                    key_text = (sm.message or "")[:60]
                    key_ts   = round(sm.date.timestamp() / 60)

                    # Önce metin+timestamp eşleştir
                    forwarded_id = dst_index.get((key_text, key_ts))
                    # Sonra ±1 dakika tolerans
                    if not forwarded_id:
                        forwarded_id = (dst_index.get((key_text, key_ts + 1))
                                     or dst_index.get((key_text, key_ts - 1)))
                    # Son çare: sadece timestamp (medya mesajlar)
                    if not forwarded_id:
                        forwarded_id = (dst_index.get(("__any__", key_ts))
                                     or dst_index.get(("__any__", key_ts + 1))
                                     or dst_index.get(("__any__", key_ts - 1)))

                    if forwarded_id:
                        new_map = MessageMapping(
                            original_chat_id   = src_id,
                            original_msg_id    = sm.id,
                            destination_chat_id= dst_id,
                            forwarded_msg_id   = forwarded_id
                        )
                        db.add(new_map)
                        mapped_count += 1

                db.commit()
                print(f"[{account.name}] Backfill tamamlandı: {src_id} → {dst_id} | {mapped_count} mesaj eşleştirildi")

            except Exception as e:
                print(f"[{account.name}] Backfill hatası ({src_id} → {dst_id}): {e}")
    finally:
        db.close()


async def start_client(account: Account):
    global clients
    client = TelegramClient(account.session_file, int(account.api_id), account.api_hash)
    await client.connect()

    if not await client.is_user_authorized():
        print(f"[{account.name}] Yetkilendirme hatasi! Lutfen tekrar giris yapin.")
        # DB'de is_active=False yap ki dashboard'da uyarı görünsün
        try:
            db = SessionLocal()
            acc_db = db.query(Account).filter(Account.id == account.id).first()
            if acc_db:
                acc_db.is_active = False
                db.commit()
            db.close()
        except Exception:
            pass
        await client.disconnect()
        return

    clients[account.id] = client

    # ── Geçmiş mesajları eşleştir (reaksiyon iletimi için) ──
    asyncio.create_task(backfill_mappings(client, account))
    # ── Reaksiyon polling — event gelmese de her 15s'de kontrol eder ──
    asyncio.create_task(reaction_poll_loop(client, account, interval=15))

    @client.on(events.NewMessage)
    async def message_handler(event):
        chat_id = str(event.chat_id)
        sender = await event.get_sender()
        sender_id = str(sender.id) if sender else None

        db = SessionLocal()
        rules = db.query(Rule).filter(
            Rule.source_chat_id == chat_id,
            Rule.account_id == account.id,
            Rule.is_active == True
        ).all()
        
        for rule in rules:
            if rule.sender_id and rule.sender_id != sender_id:
                continue

            # ── Yanıt (reply) eşleştirmesi ──
            reply_to_msg_id = None
            if event.is_reply:
                reply_msg = await event.get_reply_message()
                if reply_msg:
                    mapping = db.query(MessageMapping).filter(
                        MessageMapping.original_chat_id == chat_id,
                        MessageMapping.original_msg_id == reply_msg.id,
                        MessageMapping.destination_chat_id == rule.destination_id
                    ).first()
                    if mapping:
                        reply_to_msg_id = mapping.forwarded_msg_id

            # ── Kelime filtresi uygula ──
            caption = event.message.message or ""
            for f in rule.filters:
                replace_str = f.replace_word if f.replace_word else ""
                caption = caption.replace(f.search_word, replace_str)

            try:
                sent_msg = None
                media = event.message.media

                if media:
                    # ── Medya türünü tespit et ──
                    is_voice      = False
                    is_video_note = False
                    is_sticker    = False
                    is_photo      = isinstance(media, types.MessageMediaPhoto)
                    filename      = None

                    if isinstance(media, types.MessageMediaDocument):
                        doc = media.document
                        for attr in getattr(doc, "attributes", []):
                            if isinstance(attr, types.DocumentAttributeAudio) and getattr(attr, "voice", False):
                                is_voice = True
                            if isinstance(attr, types.DocumentAttributeVideo) and getattr(attr, "round_message", False):
                                is_video_note = True
                            if isinstance(attr, types.DocumentAttributeSticker):
                                is_sticker = True
                            if isinstance(attr, types.DocumentAttributeFilename):
                                filename = attr.file_name  # orijinal dosya adını koru

                    # ── Medyayı indir ──
                    buf = io.BytesIO()
                    if filename:
                        buf.name = filename  # Telethon bu ismi yükleme sırasında kullanır
                    elif is_voice:
                        buf.name = "voice.ogg"   # Voice note → Telegram ogg kullanır
                    elif is_video_note:
                        buf.name = "video_note.mp4"
                    elif is_photo:
                        buf.name = "photo.jpg"
                    await client.download_media(event.message, file=buf)
                    buf.seek(0)

                    send_kwargs = dict(
                        entity=int(rule.destination_id),
                        file=buf,
                        reply_to=reply_to_msg_id,
                        voice_note=is_voice,
                        video_note=is_video_note,
                    )
                    # Sticker ve ses kaydına caption eklenmez
                    if not is_voice and not is_video_note and not is_sticker:
                        send_kwargs["caption"] = caption

                    sent_msg = await client.send_file(**send_kwargs)

                else:
                    # ── Saf metin mesajı ──
                    if caption:
                        sent_msg = await client.send_message(
                            int(rule.destination_id),
                            message=caption,
                            reply_to=reply_to_msg_id
                        )

                if sent_msg:
                    new_mapping = MessageMapping(
                        original_chat_id=chat_id,
                        original_msg_id=event.id,
                        destination_chat_id=rule.destination_id,
                        forwarded_msg_id=sent_msg.id
                    )
                    db.add(new_mapping)
                    db.commit()
                    print(f"[{account.name}] ✅ İletildi: {chat_id} msg={event.id} → {rule.destination_id} msg={sent_msg.id}")

            except Exception as e:
                print(f"[{account.name}] ❌ İletim hatası (msg={event.id}): {e}")
        db.close()

    @client.on(events.Raw)
    async def raw_handler(update):
        if not isinstance(update, types.UpdateMessageReactions):
            return

        # ── Chat ID'yi standart formata çevir ──
        try:
            chat = await client.get_entity(update.peer)
            chat_id = _resolve_chat_id(chat)
        except Exception as e:
            print(f"[{account.name}] Reaksiyon: peer çözülemedi → {e}")
            return

        original_msg_id = update.msg_id
        print(f"[{account.name}] 🔥 Reaksiyon geldi | chat={chat_id} | msg={original_msg_id}")

        db = SessionLocal()
        try:
            mappings = db.query(MessageMapping).filter(
                MessageMapping.original_chat_id == chat_id,
                MessageMapping.original_msg_id == original_msg_id
            ).all()

            if not mappings:
                print(f"[{account.name}] ⚠️  Mapping bulunamadı (msg_id={original_msg_id}, chat={chat_id}). Backfill tetikleniyor...")
                # Anlık backfill: sadece bu rule için
                rules = db.query(Rule).filter(
                    Rule.source_chat_id == chat_id,
                    Rule.account_id == account.id,
                    Rule.is_active == True
                ).all()
                db.close()
                await backfill_mappings(client, account, limit=50)
                # Tekrar sorgula
                db = SessionLocal()
                mappings = db.query(MessageMapping).filter(
                    MessageMapping.original_chat_id == chat_id,
                    MessageMapping.original_msg_id == original_msg_id
                ).all()

            # Reaksiyonları parse et
            new_reactions = []
            if update.reactions and update.reactions.results:
                for r in update.reactions.results:
                    new_reactions.append(r.reaction)

            for mapping in mappings:
                rule = db.query(Rule).filter(
                    Rule.source_chat_id == chat_id,
                    Rule.destination_id == mapping.destination_chat_id,
                    Rule.account_id == account.id
                ).first()
                if not rule:
                    continue

                try:
                    await client(functions.messages.SendReactionRequest(
                        peer=int(mapping.destination_chat_id),
                        msg_id=mapping.forwarded_msg_id,
                        reaction=new_reactions
                    ))
                    print(f"[{account.name}] ✅ Reaksiyon iletildi → {mapping.destination_chat_id} | msg={mapping.forwarded_msg_id}")
                except Exception as e:
                    print(f"[{account.name}] ❌ Reaksiyon iletme hatasi: {e}")
        finally:
            db.close()

    print(f"[{account.name}] Aktif ve dinliyor.")


@app.on_event("startup")
async def startup_event():
    db = SessionLocal()
    accounts = db.query(Account).filter(Account.is_active == True).all()
    for account in accounts:
        asyncio.create_task(start_client(account))
    db.close()

# ── LOGIN / LOGOUT ──

@app.get("/login", response_class=HTMLResponse)
async def login_page(request: Request, db: Session = Depends(get_db)):
    if request.session.get("user_id"):
        return RedirectResponse(url="/", status_code=303)
    users = db.query(User).all()
    error = request.session.pop("login_error", None)
    return templates.TemplateResponse(request=request, name="login.html",
                                      context={"users": users, "error": error})

@app.post("/login")
async def login_post(
    request: Request,
    username: str = Form(...),
    db: Session = Depends(get_db)
):
    user = db.query(User).filter(User.username == username).first()
    if not user:
        request.session["login_error"] = "Kullanıcı bulunamadı."
        return RedirectResponse(url="/login", status_code=303)
    request.session["user_id"] = user.id
    return RedirectResponse(url="/", status_code=303)

@app.get("/logout")
async def logout(request: Request):
    request.session.clear()
    return RedirectResponse(url="/login", status_code=303)

# ── ANA PANEL ──

@app.get("/", response_class=HTMLResponse)
async def read_root(request: Request, db: Session = Depends(get_db)):
    user = get_current_user(request, db)
    if not user:
        return RedirectResponse(url="/login", status_code=303)

    # user_id=NULL olan sahipsiz hesapları bu kullanıcıya otomatik ata
    unclaimed = db.query(Account).filter(Account.user_id == None).all()
    for acc in unclaimed:
        acc.user_id = user.id
    if unclaimed:
        db.commit()
        print(f"[Dashboard] {len(unclaimed)} sahipsiz hesap {user.username}'e atandı.")

    accounts = db.query(Account).filter(Account.user_id == user.id).all()
    account_ids = [a.id for a in accounts]
    rules = db.query(Rule).filter(Rule.account_id.in_(account_ids)).all() if account_ids else []
    active_client_ids = set(clients.keys())
    return templates.TemplateResponse(request=request, name="index.html",
                                      context={"rules": rules, "accounts": accounts,
                                               "unclaimed": [], "current_user": user,
                                               "active_client_ids": active_client_ids})

@app.get("/accounts/add", response_class=HTMLResponse)
@app.get("/accounts/ekle", response_class=HTMLResponse)
async def add_account_form(request: Request, db: Session = Depends(get_db)):
    user = get_current_user(request, db)
    if not user:
        return RedirectResponse(url="/login", status_code=303)
    return templates.TemplateResponse(request=request, name="add_account.html",
                                      context={"current_user": user})


def _make_qr_b64(url: str) -> str:
    """Verilen URL'yi QR koda çevirip base64 PNG döndürür."""
    qr = qrcode.QRCode(version=1, error_correction=qrcode.constants.ERROR_CORRECT_L,
                       box_size=8, border=2)
    qr.add_data(url)
    qr.make(fit=True)
    img = qr.make_image(fill_color="black", back_color="white")
    buf = io.BytesIO()
    img.save(buf, format="PNG")
    return base64.b64encode(buf.getvalue()).decode()


async def _watch_qr(temp_id: str):
    """QR login tamamlanana kadar bekler, tamamlanınca DB'ye kaydeder."""
    session = qr_sessions.get(temp_id)
    if not session:
        return
    try:
        await session["qr"].wait(timeout=120)
        session["status"] = "ok"
        client: TelegramClient = session["client"]
        me = await client.get_me()
        raw_phone = getattr(me, "phone", None) or f"uid_{me.id}"
        phone = f"+{raw_phone}" if raw_phone and not str(raw_phone).startswith("+") else str(raw_phone)

        db = SessionLocal()
        try:
            exists = db.query(Account).filter(Account.phone == phone).first()
            if not exists:
                new_acc = Account(
                    user_id=session.get("user_id"),
                    name=session["name"],
                    phone=phone,
                    api_id=session["api_id"],
                    api_hash=session["api_hash"],
                    session_file=session["session_file"]
                )
                db.add(new_acc)
                db.commit()
                db.refresh(new_acc)
                asyncio.create_task(start_client(new_acc))
                print(f"[QR] Yeni hesap eklendi: {session['name']} ({phone})")
            else:
                # Hesap var ama user_id atanmamışsa güncelle (dashboard'da görünsün)
                uid = session.get("user_id")
                if uid and exists.user_id is None:
                    exists.user_id = uid
                    db.commit()
                    print(f"[QR] Hesap user_id güncellendi: {phone} → user {uid}")
                else:
                    print(f"[QR] Hesap zaten kayıtlı: {phone}")
                # Aktif değilse client'ı başlat
                if exists.id not in clients:
                    asyncio.create_task(start_client(exists))
        finally:
            db.close()
    except Exception as e:
        print(f"[QR] Login hatası ({temp_id}): {e}")
        session["status"] = "expired"


@app.post("/accounts/qr-start")
async def qr_start(
    request: Request,
    name: str = Form(...),
    api_id: str = Form(...),
    api_hash: str = Form(...),
    db: Session = Depends(get_db)
):
    user = get_current_user(request, db)
    if not user:
        return JSONResponse({"error": "Giriş yapılmamış"}, status_code=401)

    session_file = os.path.join(_DATA_DIR, f"{user.username}_{name.replace(' ', '_')}.session")
    client = TelegramClient(session_file, int(api_id), api_hash)
    await client.connect()

    if await client.is_user_authorized():
        me = await client.get_me()
        raw_phone = getattr(me, "phone", None) or f"uid_{me.id}"
        phone = f"+{raw_phone}" if not str(raw_phone).startswith("+") else str(raw_phone)
        db2 = SessionLocal()
        if not db2.query(Account).filter(Account.phone == phone).first():
            new_acc = Account(user_id=user.id, name=name, phone=phone, api_id=api_id,
                              api_hash=api_hash, session_file=session_file)
            db2.add(new_acc)
            db2.commit()
            db2.refresh(new_acc)
            asyncio.create_task(start_client(new_acc))
        db2.close()
        return JSONResponse({"status": "already_authorized"})

    try:
        qr_login = await client.qr_login()
    except Exception as e:
        await client.disconnect()
        return JSONResponse({"error": str(e)}, status_code=400)

    temp_id = uuid.uuid4().hex[:10]
    qr_sessions[temp_id] = {
        "client": client,
        "qr": qr_login,
        "status": "pending",
        "user_id": user.id,
        "name": name,
        "api_id": api_id,
        "api_hash": api_hash,
        "session_file": session_file,
    }
    asyncio.create_task(_watch_qr(temp_id))
    return JSONResponse({"account_id": temp_id, "qr_b64": _make_qr_b64(qr_login.url)})


@app.get("/accounts/qr-status")
async def qr_status(account_id: str = Query(...)):
    session = qr_sessions.get(account_id)
    if not session:
        return JSONResponse({"status": "not_found"})

    status = session["status"]
    if status == "ok":
        qr_sessions.pop(account_id, None)
        return JSONResponse({"status": "ok"})
    if status == "expired":
        qr_sessions.pop(account_id, None)
        return JSONResponse({"status": "expired"})

    # Hâlâ bekliyor → QR URL taze mi kontrol et, yenile
    try:
        qr_login = session["qr"]
        # URL hâlâ geçerliyse gönder; süresi dolmuşsa recreate
        try:
            await qr_login.recreate()
        except Exception:
            pass
        return JSONResponse({"status": "pending", "qr_b64": _make_qr_b64(qr_login.url)})
    except Exception:
        return JSONResponse({"status": "pending"})

@app.post("/accounts/send_code")
async def send_code(
    request: Request,
    name: str = Form(...),
    phone: str = Form(...),
    api_id: str = Form(...),
    api_hash: str = Form(...),
    db: Session = Depends(get_db)
):
    user = get_current_user(request, db)
    session_file = os.path.join(_DATA_DIR, f"{name}.session")
    client = TelegramClient(session_file, int(api_id), api_hash)
    await client.connect()

    result = await client.send_code_request(phone)
    login_sessions[phone] = (client, result.phone_code_hash, name, api_id, api_hash, session_file, user.id if user else None)

    return RedirectResponse(url=f"/accounts/verify?phone={phone.replace('+', '%2B')}", status_code=303)

@app.get("/accounts/verify", response_class=HTMLResponse)
async def verify_form(request: Request, phone: str):
    return templates.TemplateResponse(request=request, name="verify.html", context={"phone": phone})

@app.post("/accounts/verify")
async def verify_code(
    request: Request,
    phone: str = Form(...),
    code: str = Form(...),
    db: Session = Depends(get_db)
):
    if phone not in login_sessions:
        return RedirectResponse(url="/accounts/add", status_code=303)

    session_data = login_sessions[phone]
    # Geriye uyumluluk: eski tuple (6 eleman) veya yeni (7 eleman)
    if len(session_data) == 7:
        client, phone_code_hash, name, api_id, api_hash, session_file, saved_uid = session_data
    else:
        client, phone_code_hash, name, api_id, api_hash, session_file = session_data
        saved_uid = None

    user = get_current_user(request, db)
    uid = (user.id if user else None) or saved_uid

    try:
        await client.sign_in(phone, code, phone_code_hash=phone_code_hash)
        await client.disconnect()

        # Zaten kayıtlı mı kontrol et
        exists = db.query(Account).filter(Account.phone == phone).first()
        if exists:
            if uid and exists.user_id is None:
                exists.user_id = uid
                db.commit()
            if exists.id not in clients:
                asyncio.create_task(start_client(exists))
        else:
            new_acc = Account(
                user_id=uid,
                name=name,
                phone=phone,
                api_id=api_id,
                api_hash=api_hash,
                session_file=session_file
            )
            db.add(new_acc)
            db.commit()
            asyncio.create_task(start_client(new_acc))

        del login_sessions[phone]
        return RedirectResponse(url="/", status_code=303)
    except Exception as e:
        print(f"Dogrulama hatasi: {e}")
        return RedirectResponse(url=f"/accounts/verify?phone={phone}", status_code=303)

@app.post("/accounts/{account_id}/delete")
async def delete_account(account_id: int, db: Session = Depends(get_db)):
    acc = db.query(Account).filter(Account.id == account_id).first()
    if acc:
        if acc.id in clients:
            await clients[acc.id].disconnect()
            del clients[acc.id]
        if os.path.exists(acc.session_file):
            os.remove(acc.session_file)
        db.delete(acc)
        db.commit()
    return RedirectResponse(url="/", status_code=303)

@app.post("/rules")
async def add_rule(
    account_id: int = Form(...),
    source_chat_id: str = Form(...),
    sender_id: str = Form(None),
    destination_id: str = Form(...),
    description: str = Form(None),
    db: Session = Depends(get_db)
):
    if not sender_id or sender_id.strip() == "":
        sender_id = None
        
    # Linkleri otomatik ID'ye çevir
    if account_id in clients:
        client = clients[account_id]
        from telethon import utils
        
        async def resolve_id(identifier):
            if not identifier: return None
            identifier = identifier.strip()
            if identifier.lstrip('-').isdigit(): return identifier
            try:
                entity = await client.get_entity(identifier)
                return str(utils.get_peer_id(entity))
            except Exception as e:
                print(f"ID Çözülemedi: {identifier} - {e}")
                return identifier
                
        source_chat_id = await resolve_id(source_chat_id)
        sender_id = await resolve_id(sender_id)
        destination_id = await resolve_id(destination_id)

    new_rule = Rule(
        account_id=account_id,
        source_chat_id=source_chat_id,
        sender_id=sender_id,
        destination_id=destination_id,
        description=description
    )
    db.add(new_rule)
    db.commit()
    return RedirectResponse(url="/", status_code=303)

import glob

@app.get("/accounts/bulk", response_class=HTMLResponse)
async def bulk_account_form(request: Request, db: Session = Depends(get_db)):
    user = get_current_user(request, db)
    if not user:
        return RedirectResponse(url="/login", status_code=303)
    return templates.TemplateResponse(request=request, name="bulk_account.html",
                                      context={"current_user": user})

@app.post("/accounts/bulk_load")
async def bulk_load_accounts(
    request: Request,
    folder_path: str = Form(...),
    api_id: str = Form(...),
    api_hash: str = Form(...),
    db: Session = Depends(get_db)
):
    user = get_current_user(request, db)
    if not os.path.exists(folder_path):
        return RedirectResponse(url="/accounts/bulk", status_code=303)

    session_files = glob.glob(os.path.join(folder_path, "*.session"))
    for file in session_files:
        filename = os.path.basename(file)
        name = filename.replace('.session', '')

        temp_client = TelegramClient(file, int(api_id), api_hash)
        await temp_client.connect()
        if await temp_client.is_user_authorized():
            me = await temp_client.get_me()
            phone = getattr(me, 'phone', f"unknown_{name}")

            exists = db.query(Account).filter(Account.phone == phone).first()
            if not exists:
                new_acc = Account(
                    user_id=user.id if user else None,
                    name=name,
                    phone=f"+{phone}" if phone and not str(phone).startswith('+') else str(phone),
                    api_id=api_id,
                    api_hash=api_hash,
                    session_file=file
                )
                db.add(new_acc)
                db.commit()
                asyncio.create_task(start_client(new_acc))
        await temp_client.disconnect()

    return RedirectResponse(url="/", status_code=303)


@app.post("/rules/{rule_id}/delete")
async def delete_rule(rule_id: int, db: Session = Depends(get_db)):
    rule = db.query(Rule).filter(Rule.id == rule_id).first()
    if rule:
        db.delete(rule)
        db.commit()
    return RedirectResponse(url="/", status_code=303)

@app.post("/rules/{rule_id}/filters")
async def add_filter(
    rule_id: int,
    search_word: str = Form(...),
    replace_word: str = Form(""),
    db: Session = Depends(get_db)
):
    new_filter = WordFilter(
        rule_id=rule_id,
        search_word=search_word,
        replace_word=replace_word
    )
    db.add(new_filter)
    db.commit()
    return RedirectResponse(url="/", status_code=303)

@app.post("/filters/{filter_id}/delete")
async def delete_filter(filter_id: int, db: Session = Depends(get_db)):
    f = db.query(WordFilter).filter(WordFilter.id == filter_id).first()
    if f:
        db.delete(f)
        db.commit()
    return RedirectResponse(url="/", status_code=303)

@app.post("/resolve-link")
async def resolve_link(
    link: str = Form(...),
    account_id: int = Form(...),
    db: Session = Depends(get_db)
):
    """
    Verilen bir Telegram linki veya username'ini grup/kanal ID'sine çevirir.
    t.me/+xxxx gibi davet linkleri de desteklenir.
    """
    if account_id not in clients:
        return JSONResponse({"error": "Seçilen hesap aktif değil veya bulunamadı."}, status_code=400)

    client = clients[account_id]
    link = link.strip()

    try:
        from telethon import utils
        # Eğer davet linki ise (t.me/+ veya t.me/joinchat/)
        if "+" in link and ("t.me" in link or link.startswith("+")):
            # Hash'i çıkar
            if "t.me/+" in link:
                invite_hash = link.split("t.me/+")[-1].strip("/")
            elif "t.me/joinchat/" in link:
                invite_hash = link.split("t.me/joinchat/")[-1].strip("/")
            elif link.startswith("+") and "/" not in link:
                invite_hash = link[1:]
            else:
                invite_hash = link

            # CheckChatInvite ile gruba KATILMADAN bilgi al
            try:
                invite_info = await client(functions.messages.CheckChatInviteRequest(hash=invite_hash))
                if hasattr(invite_info, 'chat'):
                    chat = invite_info.chat
                    chat_id = utils.get_peer_id(chat)
                    return JSONResponse({
                        "id": str(chat_id),
                        "title": getattr(chat, 'title', 'Bilinmiyor'),
                        "type": type(chat).__name__
                    })
                elif hasattr(invite_info, 'channel'):
                    ch = invite_info.channel
                    chat_id = utils.get_peer_id(ch)
                    return JSONResponse({
                        "id": str(chat_id),
                        "title": getattr(ch, 'title', 'Bilinmiyor'),
                        "type": type(ch).__name__
                    })
                else:
                    return JSONResponse({"error": "Davet linki geçersiz veya süresi dolmuş."}, status_code=400)
            except Exception as e:
                err_str = str(e)
                # Zaten üye olduğumuz grup için de bilgi al
                if "INVITE_HASH_EXPIRED" in err_str:
                    return JSONResponse({"error": "Bu davet linki artık geçerli değil."}, status_code=400)
                # Zaten üyeyiz durumunda da ID almaya çalış
                try:
                    # join yoluyla dene
                    updates = await client(functions.messages.ImportChatInviteRequest(hash=invite_hash))
                    if hasattr(updates, 'chats') and updates.chats:
                        chat = updates.chats[0]
                        chat_id = utils.get_peer_id(chat)
                        return JSONResponse({
                            "id": str(chat_id),
                            "title": getattr(chat, 'title', 'Bilinmiyor'),
                            "type": type(chat).__name__,
                            "note": "Gruba katılındı!"
                        })
                except Exception as e2:
                    err2 = str(e2)
                    # Zaten üyeyiz mesajı
                    if "INVITE_REQUEST_SENT" in err2 or "USER_ALREADY_PARTICIPANT" in err2:
                        # Diyaloglardan bul
                        async for dialog in client.iter_dialogs():
                            if dialog.is_group or dialog.is_channel:
                                pass  # devam
                    return JSONResponse({"error": f"Link çözülemedi: {err2}"}, status_code=400)
        else:
            # Normal username veya public link
            identifier = link
            if "t.me/" in link:
                identifier = link.split("t.me/")[-1].strip("/")
            if not identifier.startswith("@"):
                identifier = "@" + identifier if not identifier.lstrip('-').isdigit() else identifier

            entity = await client.get_entity(identifier)
            chat_id = utils.get_peer_id(entity)
            return JSONResponse({
                "id": str(chat_id),
                "title": getattr(entity, 'title', getattr(entity, 'first_name', 'Bilinmiyor')),
                "type": type(entity).__name__
            })
    except Exception as e:
        return JSONResponse({"error": f"Hata: {str(e)}"}, status_code=400)


@app.get("/rules/toplu", response_class=HTMLResponse)
async def bulk_rules_page(request: Request, db: Session = Depends(get_db)):
    user = get_current_user(request, db)
    if not user:
        return RedirectResponse(url="/login", status_code=303)
    accounts = db.query(Account).filter(Account.user_id == user.id).all()
    return templates.TemplateResponse(request=request, name="bulk_rules.html",
                                      context={"accounts": accounts, "current_user": user})


@app.post("/rules/toplu")
async def bulk_rules_add(request: Request, db: Session = Depends(get_db)):
    """
    Toplu kural ekleme. JSON body:
    {
      "source_chat_id": "...",
      "destination_id": "...",
      "description": "...",
      "rows": [
        {"account_id": 1, "sender_id": "..."},
        {"account_id": 2, "sender_id": ""},
        ...
      ]
    }
    """
    from telethon import utils as tg_utils

    body = await request.json()
    source_raw = body.get("source_chat_id", "").strip()
    dest_raw   = body.get("destination_id", "").strip()
    description = body.get("description", "").strip() or None
    rows        = body.get("rows", [])

    if not source_raw or not dest_raw or not rows:
        return JSONResponse({"error": "Eksik alan: kaynak, hedef ve en az 1 satır gerekli."}, status_code=400)

    async def resolve(identifier, client):
        if not identifier: return None
        identifier = identifier.strip()
        if identifier.lstrip('-').isdigit(): return identifier
        try:
            if "t.me/+" in identifier or "t.me/joinchat/" in identifier:
                hash_ = identifier.split("/+")[-1].split("/joinchat/")[-1].strip("/")
                info = await client(functions.messages.CheckChatInviteRequest(hash=hash_))
                chat = getattr(info, 'chat', None) or getattr(info, 'channel', None)
                if chat: return str(tg_utils.get_peer_id(chat))
            if "t.me/" in identifier:
                identifier = "@" + identifier.split("t.me/")[-1].strip("/")
            if not identifier.startswith("@"):
                identifier = "@" + identifier
            entity = await client.get_entity(identifier)
            return str(tg_utils.get_peer_id(entity))
        except Exception as e:
            return None

    added = 0
    errors = []

    for row in rows:
        acc_id  = int(row.get("account_id", 0))
        sender  = row.get("sender_id", "").strip() or None

        client = clients.get(acc_id)
        if not client:
            errors.append(f"Hesap {acc_id} aktif değil — atlandı.")
            continue

        src  = await resolve(source_raw, client) or source_raw
        dest = await resolve(dest_raw, client)   or dest_raw
        sid  = await resolve(sender, client) if sender else None

        new_rule = Rule(
            account_id=acc_id,
            source_chat_id=src,
            sender_id=sid,
            destination_id=dest,
            description=description
        )
        db.add(new_rule)
        added += 1

    db.commit()
    return JSONResponse({"added": added, "errors": errors})


@app.post("/search-member")
async def search_member(
    chat_id: str = Form(...),
    query: str = Form(...),
    account_id: int = Form(...),
):
    """
    Verilen grupta 'query' ile eşleşen üyeleri arar.
    İsim, soyisim veya @username ile arama desteklenir.
    """
    if account_id not in clients:
        # Hesap neden aktif değil?
        db = SessionLocal()
        acc = db.query(Account).filter(Account.id == account_id).first()
        db.close()
        if acc and not acc.is_active:
            msg = f"'{acc.name}' hesabının oturumu sona ermiş. Lütfen hesabı silin ve yeniden ekleyin."
        else:
            msg = "Seçilen hesap henüz bağlanmadı (sunucu yeni başlatıldıysa birkaç saniye bekleyin)."
        return JSONResponse({"error": msg}, status_code=400)

    if not chat_id or not chat_id.strip():
        return JSONResponse({"error": "Önce Kaynak Grup ID'sini doldur!"}, status_code=400)

    client = clients[account_id]
    query_clean = query.strip().lstrip("@")

    try:
        from telethon import utils as tg_utils

        # @username ile direkt dene (en hızlı yol)
        try:
            entity = await client.get_entity(f"@{query_clean}")
            first = getattr(entity, "first_name", "") or ""
            last  = getattr(entity, "last_name", "")  or ""
            return JSONResponse({"results": [{
                "id":       str(tg_utils.get_peer_id(entity)),
                "name":     f"{first} {last}".strip() or f"ID:{entity.id}",
                "username": getattr(entity, "username", "") or "",
            }]})
        except Exception:
            pass  # Username ile bulunamazsa grup üyelerinde ara

        # Grup üyelerini tara (Telethon'un search parametresiyle)
        results = []
        try:
            async for member in client.iter_participants(int(chat_id), search=query_clean):
                first = getattr(member, "first_name", "") or ""
                last  = getattr(member, "last_name",  "") or ""
                uname = getattr(member, "username",   "") or ""
                results.append({
                    "id":       str(member.id),
                    "name":     f"{first} {last}".strip() or f"ID:{member.id}",
                    "username": uname,
                })
                if len(results) >= 10:
                    break
        except Exception as e:
            err = str(e)
            if "CHAT_ADMIN_REQUIRED" in err or "not an admin" in err.lower():
                return JSONResponse({"error": "Grup üyelerini görmek için hesabın admin olması gerekiyor. @username ile arayın."}, status_code=400)
            if "CHANNEL_PRIVATE" in err:
                return JSONResponse({"error": "Bu grup/kanal özeldir veya hesap üye değil."}, status_code=400)
            return JSONResponse({"error": f"Arama hatası: {err}"}, status_code=400)

        if not results:
            return JSONResponse({"error": f"'{query}' için sonuç bulunamadı."}, status_code=404)

        return JSONResponse({"results": results})

    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=400)


if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 10000))
    uvicorn.run(app, host="0.0.0.0", port=port)
