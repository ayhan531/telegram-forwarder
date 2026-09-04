import os
import io
import zipfile
import datetime
import re
import base64
import uuid
import asyncio
import glob
from fastapi import FastAPI, Request, Form, Depends, Query
from fastapi.responses import HTMLResponse, RedirectResponse, JSONResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from sqlalchemy.orm import Session
from starlette.middleware.sessions import SessionMiddleware
from telethon import TelegramClient, events, functions, types
from telethon.errors import SessionPasswordNeededError
from dotenv import load_dotenv
import qrcode

import database
from database import (
    SessionLocal, Rule, Account, MessageMapping, WordFilter,
    User, Teacher, TeacherReaction, hash_password, UserTask, IgnoredMember
)

from contextlib import asynccontextmanager

load_dotenv()

_processed_albums = {}

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Uygulama başlangıç ve bitiş olayları."""
    import database as _db_module
    import os
    import urllib.request
    import tarfile

    import database as _db_module
    data_dir = os.environ.get("DATA_DIR", ".")
    db_path  = os.path.join(data_dir, "telegram_forwarder.db")
    disk_ok  = os.path.isdir(data_dir) and os.access(data_dir, os.W_OK)

    print(f"[System] Uygulama başlatılıyor...")
    print(f"[System] DATA_DIR  = {data_dir}")
    print(f"[System] DB path   = {db_path}")
    print(f"[System] Disk OK   = {disk_ok}  ← False ise Render'da disk takılı değil!")
    if not disk_ok:
        print("[System] ⚠️  UYARI: Veri dizini yazılabilir değil. "
              "Render Dashboard → Disks → /data eklendiğinden emin ol!")

    # Veritabanını oluştur
    _db_module.init_db()

    # --- AUTO RESTORE BACKUP IF ACCOUNTS ARE EMPTY ---
    try:
        import sqlite3
        import urllib.request
        c = sqlite3.connect(db_path)
        acc_count = c.execute("SELECT count(*) FROM accounts").fetchone()[0]
        if acc_count == 0:
            print("[System] 🚨 Veritabanı boş. Yedek indiriliyor ve enjekte ediliyor...")
            urllib.request.urlretrieve("https://litter.catbox.moe/wk14ee.gz", "backup.tar.gz")
            os.system("tar -xzvf backup.tar.gz")
            os.system(f"mkdir -p {data_dir}")
            os.system(f"cp -r *.session {data_dir}/")
            
            c.execute("ATTACH DATABASE 'telegram_forwarder.db' AS backup")
            for t in ['users', 'accounts', 'rules', 'word_filters', 'message_mappings']:
                try:
                    c.execute(f"INSERT OR IGNORE INTO {t} SELECT * FROM backup.{t}")
                except Exception as e:
                    print(f"Tablo aktarim hatasi ({t}): {e}")
            c.execute("UPDATE accounts SET user_id = NULL")
            c.commit()
            print("[System] ✅ Yedek başarıyla geri yüklendi ve onarıldı!")
        c.close()
    except Exception as e:
        print(f"[System] ❌ Yedek yükleme hatası: {e}")
    # --------------------------------------------

    # --- TELETHON SESSION REPAIR ---
    # Eğer Render restart sırasında SQLite session dosyaları bozulursa (no such table: entities hatası)
    # Bu kod eksik tabloları veriyi silmeden otomatik onarır.
    import sqlite3
    import glob
    print("[System] Session dosyaları kontrol ediliyor ve onarılıyor...")
    for s_file in glob.glob(os.path.join(data_dir, "*.session")):
        try:
            with sqlite3.connect(s_file) as conn:
                c = conn.cursor()
                c.execute('''CREATE TABLE IF NOT EXISTS entities (
                    id integer primary key,
                    hash integer not null,
                    username text,
                    phone integer,
                    name text,
                    date integer
                )''')
                conn.commit()
        except Exception as e:
            print(f"[System] ⚠️ Session onarma hatası ({s_file}): {e}")
    # -------------------------------

    try:
        print("[System] Veritabanı hazır.")
    except Exception as e:
        print(f"[System] Veritabanı başlatma hatası: {e}")

    async def _staggered_start(acc_list):
        for acc in acc_list:
            asyncio.create_task(start_client(acc))
            await asyncio.sleep(0.15)  # Telegram IP rate limitini ve TimeoutError'ı önler

    try:
        with SessionLocal() as db:
            accounts = db.query(Account).filter(Account.is_active == True).all()
            print(f"[System] {len(accounts)} aktif hesap kademeli başlatılıyor...")
            asyncio.create_task(_staggered_start(accounts))
    except Exception as e:
        print(f"[System] Hesap başlatma hatası: {e}")


    yield  # Uygulama burada çalışır

    # Shutdown: tüm client'ları kapat
    print("[System] Kapatılıyor...")
    for client in list(clients.values()):
        try:
            await client.disconnect()
        except Exception:
            pass

app = FastAPI(lifespan=lifespan)
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

# URL tespit regex — http/https ve www. ile baslayan linkleri bulur
_URL_RE = re.compile(r'https?://[^\s]+|www\.[^\s]+', re.IGNORECASE)

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


async def reaction_poll_loop(client: TelegramClient, account: Account, interval: int = 20):
    """
    Her `interval` saniyede bir kaynak mesajların reaksiyonlarını çeker,
    değişen reaksiyonları hedefe iletir.
    Ağ çağrıları sırasında asla DB bağlantısı tutmaz — QueuePool tükenmesini önler.
    """
    # Hesap ID'sine göre başlangıca ufak jitter ekle
    await asyncio.sleep(15 + (account.id % 20))
    print(f"[{account.name}] 🔄 Reaksiyon polling başladı ({interval}s aralık)")

    while True:
        if not client.is_connected():
            await asyncio.sleep(15)
            continue

        try:
            # 1. Hızlıca aktif kuralları al ve DB'yi HEMEN KAPAT
            rules_data = []
            with SessionLocal() as db:
                rules = db.query(Rule).filter(
                    Rule.account_id == account.id,
                    Rule.is_active == True
                ).all()
                for r in rules:
                    rules_data.append({
                        "source_chat_id": r.source_chat_id,
                        "destination_id": r.destination_id,
                        "sender_id": r.sender_id
                    })

            if not rules_data:
                await asyncio.sleep(interval)
                continue

            # 2. Ağ çağrılarını DB KAPALIYKEN yap
            for r in rules_data:
                src_chat = r["source_chat_id"]
                dst_chat = r["destination_id"]
                sid_filter = r["sender_id"]

                try:
                    src_msgs = await client.get_messages(int(src_chat), limit=30)
                    if not src_msgs:
                        continue

                    # Sadece reaksiyonu olan mesajlar
                    reacted_msgs = [
                        m for m in src_msgs
                        if m and getattr(m, 'reactions', None) and getattr(m.reactions, 'results', None)
                        and (not sid_filter or str(getattr(m, 'sender_id', '')) == sid_filter)
                    ]
                    if not reacted_msgs:
                        continue

                    # Bu mesajların mapping'lerini tek hızlı sorguda çek ve DB'yi kapat
                    msg_ids = [m.id for m in reacted_msgs]
                    mappings_dict = {}
                    with SessionLocal() as db:
                        maps = db.query(MessageMapping).filter(
                            MessageMapping.account_id == account.id,
                            MessageMapping.original_chat_id == src_chat,
                            MessageMapping.original_msg_id.in_(msg_ids),
                            MessageMapping.destination_chat_id == dst_chat
                        ).all()
                        for mp in maps:
                            mappings_dict[mp.original_msg_id] = (mp.id, mp.forwarded_msg_id, mp.last_reactions)

                    # Değişen reaksiyonları ilet
                    for msg in reacted_msgs:
                        current_str = _reactions_to_str(msg.reactions.results)
                        if not current_str:
                            continue

                        map_info = mappings_dict.get(msg.id)
                        if not map_info:
                            continue

                        map_db_id, fwd_msg_id, last_react = map_info
                        if (last_react or "") == current_str:
                            continue

                        new_reactions = [rx.reaction for rx in msg.reactions.results]
                        try:
                            await client(functions.messages.SendReactionRequest(
                                peer=int(dst_chat),
                                msg_id=fwd_msg_id,
                                reaction=new_reactions,
                                add_to_recent=True
                            ))
                            # Başarılıysa DB'ye tek satır yaz ve HEMEN kapat
                            with SessionLocal() as db:
                                row = db.query(MessageMapping).filter(MessageMapping.id == map_db_id).first()
                                if row:
                                    row.last_reactions = current_str
                                    db.commit()
                            print(f"[{account.name}] ✅ [POLL] Reaksiyon iletildi src={msg.id} → dst={fwd_msg_id} | {current_str}")
                        except Exception as e:
                            print(f"[{account.name}] ❌ [POLL] Reaksiyon gönderme hatası: {e}")

                except Exception as e:
                    print(f"[{account.name}] ❌ [POLL] Rule hatası ({src_chat}): {e}")

        except Exception as e:
            print(f"[{account.name}] ❌ [POLL] Genel hata: {e}")

        # Her döngüde hesap bazlı jitter ile bekle
        await asyncio.sleep(interval + (account.id % 5))


async def backfill_mappings(client: TelegramClient, account: Account, limit: int = 100):
    """
    Her rule için kaynak kanaldaki son `limit` mesajı çeker.
    Ağ çağrıları sırasında DB bağlantısı tutulmaz.
    """
    try:
        rules_data = []
        with SessionLocal() as db:
            rules = db.query(Rule).filter(
                Rule.account_id == account.id,
                Rule.is_active == True
            ).all()
            for r in rules:
                rules_data.append((r.source_chat_id, r.destination_id, r.sender_id))

        if not rules_data:
            return

        for src_id, dst_id, sender_id in rules_data:
            try:
                # Kaynak ve hedef mesajları çek (DB KAPALI)
                src_msgs = await client.get_messages(int(src_id), limit=limit)
                dst_msgs = await client.get_messages(int(dst_id), limit=limit)

                if not src_msgs or not dst_msgs:
                    continue

                dst_index = {}
                for dm in dst_msgs:
                    if not dm:
                        continue
                    key_text = (dm.message or "")[:60]
                    key_ts   = round(dm.date.timestamp() / 60)
                    dst_index[(key_text, key_ts)]   = dm.id
                    dst_index[("__any__", key_ts)] = dm.id

                # Mevcut mapping'leri çek
                src_ids = [sm.id for sm in src_msgs if sm]
                with SessionLocal() as db:
                    existing_mapped = set(
                        row[0] for row in db.query(MessageMapping.original_msg_id).filter(
                            MessageMapping.account_id == account.id,
                            MessageMapping.original_chat_id == src_id,
                            MessageMapping.destination_chat_id == dst_id,
                            MessageMapping.original_msg_id.in_(src_ids)
                        ).all()
                    )

                new_mappings = []
                for sm in src_msgs:
                    if not sm or sm.id in existing_mapped:
                        continue
                    if sender_id and str(getattr(sm, 'sender_id', '')) != sender_id:
                        continue

                    key_text = (sm.message or "")[:60]
                    key_ts   = round(sm.date.timestamp() / 60)

                    forwarded_id = dst_index.get((key_text, key_ts))
                    if not forwarded_id:
                        forwarded_id = (dst_index.get((key_text, key_ts + 1))
                                     or dst_index.get((key_text, key_ts - 1)))
                    if not forwarded_id:
                        forwarded_id = (dst_index.get(("__any__", key_ts))
                                     or dst_index.get(("__any__", key_ts + 1))
                                     or dst_index.get(("__any__", key_ts - 1)))

                    if forwarded_id:
                        new_mappings.append(MessageMapping(
                            account_id          = account.id,
                            original_chat_id    = src_id,
                            original_msg_id     = sm.id,
                            destination_chat_id = dst_id,
                            forwarded_msg_id    = forwarded_id
                        ))

                if new_mappings:
                    with SessionLocal() as db:
                        db.add_all(new_mappings)
                        db.commit()
                    print(f"[{account.name}] Backfill tamamlandı: {src_id} → {dst_id} | {len(new_mappings)} mesaj eşleştirildi")

            except Exception as e:
                print(f"[{account.name}] Backfill hatası ({src_id} → {dst_id}): {e}")

    except Exception as e:
        print(f"[{account.name}] Backfill genel hata: {e}")


async def start_client(account: Account, _existing_client: TelegramClient = None):
    global clients

    # — Zaten bağlıysa tekrar başlatma (duplicate event handler önle) —
    if account.id in clients:
        existing = clients[account.id]
        if existing.is_connected():
            print(f"[{account.name}] Zaten bağlı, atlanıyor.")
            return
        else:
            try:
                await existing.disconnect()
            except Exception:
                pass
            del clients[account.id]

    if _existing_client is not None:
        # Kullanıcı az önce giriş yaptı — client zaten bağlı ve authorized
        # Disconnect/reconnect YOK → session flush race condition ortadan kalkar
        client = _existing_client
        print(f"[{account.name}] Mevcut bağlantı kullanılıyor (yeni giriş).")
    else:
        print(f"[{account.name}] Başlatılıyor...")
        client = TelegramClient(
            account.session_file,
            int(account.api_id),
            account.api_hash,
            connection_retries=3,
            retry_delay=5,
            timeout=25,
            auto_reconnect=True
        )

        try:
            await asyncio.wait_for(client.connect(), timeout=35)
        except asyncio.TimeoutError:
            print(f"[{account.name}] Bağlantı zaman aşıldı.")
            return
        except Exception as e:
            print(f"[{account.name}] Bağlantı hatası: {e}")
            return

        try:
            authorized = await asyncio.wait_for(client.is_user_authorized(), timeout=20)
        except asyncio.TimeoutError:
            print(f"[{account.name}] Yetkilendirme kontrolü zaman aşıldı.")
            await client.disconnect()
            return
        except Exception as e:
            print(f"[{account.name}] Yetkilendirme hatası: {e}")
            await client.disconnect()
            return

        if not authorized:
            print(f"[{account.name}] Oturum geçersiz veya süresi dolmuş — atlanıyor (hesap silinmedi).")
            await client.disconnect()
            return

    clients[account.id] = client
    print(f"[{account.name}] Aktif.")

    # ── Geçmiş mesajları eşleştir (reaksiyon iletimi için — kademeli başlat) ──
    async def _delayed_backfill():
        await asyncio.sleep(60 + (account.id % 90))
        await backfill_mappings(client, account)

    asyncio.create_task(_delayed_backfill())
    asyncio.create_task(reaction_poll_loop(client, account, interval=20))

    @client.on(events.NewMessage)
    async def message_handler(event):
        chat_id = str(event.chat_id)
        sender = await event.get_sender()
        sender_id = str(sender.id) if sender else None

        db = SessionLocal()
        try:
            # --- HOCA EMOJISI (TEACHERS) ---
            def _norm_id(v):
                s = str(v).strip()
                if s.startswith('-100'): return s[4:]
                return s.lstrip('-')

            teachers = db.query(Teacher).all()
            matched_teacher = None
            for t in teachers:
                if _norm_id(t.source_chat_id) == _norm_id(chat_id) and str(t.teacher_user_id) == str(sender_id):
                    matched_teacher = t
                    # Bu hocanın reaction ayarlarından BU hesap için olanı bul
                    react_config = db.query(TeacherReaction).filter(
                        TeacherReaction.teacher_id == t.id,
                        TeacherReaction.account_id == account.id
                    ).first()
                    if react_config and react_config.emojis:
                        grouped_id = event.message.grouped_id
                        should_react = True
                        if grouped_id:
                            cache_key = (account.id, grouped_id)
                            now = time.time()
                            if cache_key in _processed_albums and now - _processed_albums[cache_key] < 300:
                                should_react = False
                            else:
                                _processed_albums[cache_key] = now
                                # Cleanup old cache
                                keys_to_delete = [k for k, v in _processed_albums.items() if now - v > 300]
                                for k in keys_to_delete:
                                    del _processed_albums[k]
                        
                        if should_react:
                            print(f"[{account.name}] 👨‍🏫 Hoca mesajı algılandı! Emoji atılacak. (grouped_id={grouped_id})")
                            asyncio.create_task(delayed_react(account.id, event.chat_id, event.id, react_config.emojis, t.delay_max_minutes))

            rules = db.query(Rule).filter(
                Rule.source_chat_id == chat_id,
                Rule.account_id == account.id,
                Rule.is_active == True
            ).all()

            for rule in rules:
                if rule.sender_id and rule.sender_id != sender_id:
                    continue

                # ── Global içerik filtreleri (tüm kurallar için geçerli) ──
                _raw_text = event.message.message or ""

                # 1. @ mention içeren mesajları engelle (@kullanici gibi)
                if '@' in _raw_text:
                    print(f"[{account.name}] 🚫 @ mention içeriği engellendi (msg={event.id})")
                    continue

                # 2. t.me/ linki içeren mesajları engelle
                if re.search(r't\.me/', _raw_text, re.IGNORECASE):
                    print(f"[{account.name}] 🚫 t.me linki engellendi (msg={event.id})")
                    continue

                # 3. Sadece #reklam içeren mesajları engelle
                #    (#AKFIS, #BTC gibi hisse/kripto kodları ETKİLENMEZ)
                if re.search(r'#[Rr][Ee][Kk][Ll][Aa][Mm]\b', _raw_text):
                    print(f"[{account.name}] 🚫 #reklam içeriği engellendi (msg={event.id})")
                    continue

                # ── Yanıt (reply) eşleştirmesi ──
                reply_to_msg_id = None
                reply_quote_text = ""
                if event.is_reply:
                    reply_msg = await event.get_reply_message()
                    if reply_msg:
                        # Hedef grup ID'sini normalize et (format farkı olabilir)
                        def _norm_id(v):
                            s = str(v).strip()
                            # -1001234... → 1234..., -1234 → 1234
                            if s.startswith('-100'):
                                return s[4:]
                            return s.lstrip('-')

                        dest_norm = _norm_id(rule.destination_id)

                        # 1. Bu hesabın kendi mapping'i
                        mapping = db.query(MessageMapping).filter(
                            MessageMapping.account_id == account.id,
                            MessageMapping.original_chat_id == chat_id,
                            MessageMapping.original_msg_id == reply_msg.id,
                            MessageMapping.destination_chat_id == rule.destination_id
                        ).first()

                        # 2. NULL account (eski kayıtlar)
                        if not mapping:
                            mapping = db.query(MessageMapping).filter(
                                MessageMapping.account_id == None,
                                MessageMapping.original_chat_id == chat_id,
                                MessageMapping.original_msg_id == reply_msg.id,
                                MessageMapping.destination_chat_id == rule.destination_id
                            ).first()

                        # 3. Çapraz hesap: aynı kaynak mesajı HERHANGI bir hesap iletmiş mi?
                        #    (destination_chat_id filtresi olmadan al, sonra normalize edeceğiz)
                        if not mapping:
                            all_maps = db.query(MessageMapping).filter(
                                MessageMapping.original_chat_id == chat_id,
                                MessageMapping.original_msg_id == reply_msg.id,
                            ).all()
                            for m in all_maps:
                                if _norm_id(m.destination_chat_id) == dest_norm:
                                    mapping = m
                                    print(f"[{account.name}] ↩️ Çapraz hesap reply eşleşti: "
                                          f"src_msg={reply_msg.id} → dst_msg={m.forwarded_msg_id} "
                                          f"(dst_chat={m.destination_chat_id})")
                                    break

                        if mapping:
                            reply_to_msg_id = mapping.forwarded_msg_id
                        else:
                            # Yanıtlanan mesaj hiç iletilmemiş — debug log + fake alıntı
                            print(f"[{account.name}] ⚠️ Reply mapping bulunamadı: "
                                  f"src_chat={chat_id} src_msg={reply_msg.id} dest={rule.destination_id}(norm={dest_norm})")
                            try:
                                r_sender = await reply_msg.get_sender()
                                r_name = getattr(r_sender, 'first_name', '') or getattr(r_sender, 'title', '') or 'Biri'
                                r_text = reply_msg.message or "[Medya/Görsel]"
                                if len(r_text) > 80:
                                    r_text = r_text[:80] + "..."
                                reply_quote_text = f"┏ 💬 {r_name} yazmıştı:\n┗ ❝{r_text}❞\n\n"
                            except Exception:
                                pass

                # ── Kelime filtresi uygula ──
                caption = event.message.message or ""
                for f in rule.filters:
                    replace_str = f.replace_word if f.replace_word else ""
                    caption = caption.replace(f.search_word, replace_str)

                # Eğer sahte alıntı varsa mesajın başına ekle
                if reply_quote_text:
                    caption = reply_quote_text + caption

                # ── Link kontrol ──
                has_links = bool(_URL_RE.search(caption))
                if rule.block_links and has_links:
                    print(f"[{account.name}] 🔗 Linkli mesaj engellendi (msg={event.id})")
                    continue
                if rule.replace_link and has_links:
                    caption = _URL_RE.sub(rule.replace_link, caption)
                    print(f"[{account.name}] 🔗 Link degistirildi → {rule.replace_link} (msg={event.id})")

                try:
                    sent_msg = None

                    # ── Kanalı / Grubu mesaj içerisinde belirt (Şuradan iletildi başlığı) ──
                    if getattr(rule, 'show_forward_header', False):
                        try:
                            fwd_res = await client.forward_messages(
                                int(rule.destination_id),
                                event.message
                            )
                            if fwd_res:
                                sent_msg = fwd_res[0] if isinstance(fwd_res, list) else fwd_res
                                print(f"[{account.name}] 📢 Orijinal kaynak belirtilerek iletildi (msg={event.id} → {sent_msg.id})")
                        except Exception as fwd_e:
                            print(f"[{account.name}] ⚠️ Native forward yapılamadı ({fwd_e}), kopya ile gönderiliyor...")

                    # Eğer show_forward_header kapalıysa veya native forward başarısız olduysa kopya olarak ilet:
                    if not sent_msg:
                        media = event.message.media

                        if media:
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
                                        filename = attr.file_name

                            buf = io.BytesIO()
                            if filename:
                                buf.name = filename
                            elif is_voice:
                                buf.name = "voice.ogg"
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
                            if not is_voice and not is_video_note and not is_sticker:
                                send_kwargs["caption"] = caption

                            sent_msg = await client.send_file(**send_kwargs)

                        else:
                            if caption:
                                sent_msg = await client.send_message(
                                    int(rule.destination_id),
                                    message=caption,
                                    reply_to=reply_to_msg_id
                                )

                    if sent_msg:
                        new_mapping = MessageMapping(
                            account_id=account.id,
                            original_chat_id=chat_id,
                            original_msg_id=event.id,
                            destination_chat_id=rule.destination_id,
                            forwarded_msg_id=sent_msg.id
                        )
                        db.add(new_mapping)
                        db.commit()
                        print(f"[{account.name}] ✅ İletildi: {chat_id} msg={event.id} → {rule.destination_id} msg={sent_msg.id}")

                        # ── HEDEF GRUP EMOJİSİ (Yönlendirilen Mesaj İçin) ──
                        if matched_teacher:
                            grouped_id = event.message.grouped_id
                            should_react_dest = True
                            if grouped_id:
                                cache_key = ("dest_react", rule.destination_id, grouped_id)
                                now = time.time()
                                if cache_key in _processed_albums and now - _processed_albums[cache_key] < 300:
                                    should_react_dest = False
                                else:
                                    _processed_albums[cache_key] = now
                            
                            if should_react_dest:
                                all_react_configs = db.query(TeacherReaction).filter(TeacherReaction.teacher_id == matched_teacher.id).all()
                                for r_conf in all_react_configs:
                                    asyncio.create_task(delayed_react(r_conf.account_id, int(rule.destination_id), sent_msg.id, r_conf.emojis, matched_teacher.delay_max_minutes))

                except Exception as e:
                    print(f"[{account.name}] ❌ İletim hatası (msg={event.id}): {e}")
        finally:
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

# ── LOGIN / LOGOUT ──

@app.get("/login", response_class=HTMLResponse)
async def login_page(request: Request, db: Session = Depends(get_db)):
    try:
        if request.session.get("user_id"):
            return RedirectResponse(url="/", status_code=303)
        users = db.query(User).order_by(User.id.asc()).all()
        error = request.session.pop("login_error", None)
        return templates.TemplateResponse(request=request, name="login.html",
                                          context={"users": users, "error": error})
    except Exception as e:
        import traceback
        return HTMLResponse(content=f"<h1>Login Hatası</h1><pre>{traceback.format_exc()}</pre>", status_code=500)

@app.post("/login")
async def login_post(
    request: Request,
    username: str = Form(...),
    password: str = Form(...),
    db: Session = Depends(get_db)
):
    user = db.query(User).filter(User.username == username).first()
    if not user or user.password_hash != hash_password(password):
        request.session["login_error"] = "Hatalı şifre veya kullanıcı."
        return RedirectResponse(url="/login", status_code=303)
    
    request.session["user_id"] = user.id
    return RedirectResponse(url="/", status_code=303)

@app.get("/logout")
async def logout(request: Request):
    request.session.clear()
    return RedirectResponse(url="/login", status_code=303)

@app.get("/health")
@app.head("/health")
async def health_check():
    return {"status": "ok"}

@app.head("/")
async def root_head():
    return HTMLResponse(content="", status_code=200)

# ── BACKUP ──

@app.get("/admin/backup")
async def download_backup(request: Request, secret: str = Query("")):
    """
    Tüm veritabanı ve session dosyalarını ZIP olarak indir.
    Kullanım: /admin/backup?secret=BACKUP_SECRET
    """
    backup_secret = os.environ.get("BACKUP_SECRET", "tg-backup-2026-secure")
    if secret != backup_secret:
        return HTMLResponse(content="<h1>403 Yetkisiz</h1>", status_code=403)

    buf = io.BytesIO()
    timestamp = datetime.datetime.utcnow().strftime("%Y%m%d_%H%M%S")

    # WAL checkpoint yaparak tüm verilerin ana veritabanı dosyasına yazılmasını sağla
    try:
        from database import engine as _engine
        with _engine.connect() as _c:
            _c.execute(text("PRAGMA wal_checkpoint(FULL)"))
            _c.commit()
    except Exception as _e:
        print(f"[Backup] WAL checkpoint uyarısı: {_e}")

    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        # Veritabanı
        db_path = os.path.join(_DATA_DIR, "telegram_forwarder.db")
        if os.path.exists(db_path):
            zf.write(db_path, "telegram_forwarder.db")

        # Tüm .session dosyaları
        session_files = glob.glob(os.path.join(_DATA_DIR, "*.session"))
        for sf in session_files:
            zf.write(sf, os.path.basename(sf))

        # WAL / SHM dosyaları (varsa)
        for ext in ["-wal", "-shm"]:
            extra = db_path + ext
            if os.path.exists(extra):
                zf.write(extra, "telegram_forwarder.db" + ext)

    buf.seek(0)
    filename = f"tg_backup_{timestamp}.zip"
    print(f"[Backup] Yedek indirildi: {filename} ({buf.getbuffer().nbytes} bytes)")

    return StreamingResponse(
        buf,
        media_type="application/zip",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'}
    )

# ── ANA PANEL ──

@app.get("/", response_class=HTMLResponse)
async def read_root(request: Request, db: Session = Depends(get_db)):
    try:
        user = get_current_user(request, db)
        if not user:
            return RedirectResponse(url="/login", status_code=303)

        # user_id=NULL olan sahipsiz hesapları bu kullanıcıya otomatik ata
        unclaimed = db.query(Account).filter(Account.user_id == None).all()
        for acc in unclaimed:
            acc.user_id = user.id
        if unclaimed:
            db.commit()

        accounts = db.query(Account).filter(Account.user_id == user.id).all()
        account_ids = [a.id for a in accounts]
        rules = db.query(Rule).filter(Rule.account_id.in_(account_ids)).all() if account_ids else []
        active_client_ids = set(clients.keys())

        # Kullanıcıya atanmış tamamlanmamış görevler
        pending_tasks = db.query(UserTask).filter(
            (UserTask.user_id == user.id) | (UserTask.user_id == None),
            UserTask.status == "pending"
        ).order_by(UserTask.id.desc()).all()
        
        return templates.TemplateResponse(request=request, name="index.html",
                                          context={"rules": rules, "accounts": accounts,
                                                   "unclaimed": [], "current_user": user,
                                                   "active_client_ids": active_client_ids,
                                                   "pending_tasks": pending_tasks})
    except Exception as e:
        import traceback
        return HTMLResponse(content=f"<h1>Hata Oluştu</h1><pre>{traceback.format_exc()}</pre>", status_code=500)

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
    except SessionPasswordNeededError:
        # QR tarandı ama 2FA şifresi gerekiyor — client bağlı kalsın
        session["status"] = "2fa_required"
        print(f"[QR] 2FA gerekli: {temp_id}")
        return
    except Exception as e:
        print(f"[QR] Login hatası ({temp_id}): {e}")
        session["status"] = "expired"
        return

    session["status"] = "ok"
    await _finalize_qr_session(temp_id)


async def _finalize_qr_session(temp_id: str):
    """QR veya 2FA tamamlandıktan sonra hesabı DB'ye kaydeder ve mevcut client ile başlar."""
    session = qr_sessions.get(temp_id)
    if not session:
        return
    client: TelegramClient = session["client"]
    try:
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
                # Mevcut bağlı client'i kullan — disconnect/reconnect yok!
                asyncio.create_task(start_client(new_acc, _existing_client=client))
                print(f"[QR] Yeni hesap eklendi: {session['name']} ({phone})")
            else:
                uid = session.get("user_id")
                if uid and exists.user_id is None:
                    exists.user_id = uid
                    db.commit()
                if exists.is_active is False:
                    exists.is_active = True
                    db.commit()
                asyncio.create_task(start_client(exists, _existing_client=client))
                print(f"[QR] Hesap güncellendi: {phone}")
        finally:
            db.close()
    except Exception as e:
        print(f"[QR] Finalize hatası ({temp_id}): {e}")
        if temp_id in qr_sessions:
            qr_sessions[temp_id]["status"] = "expired"


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

    # Check if name is already used
    existing_acc = db.query(Account).filter(Account.user_id == user.id, Account.name == name).first()
    if existing_acc:
        return JSONResponse({"error": f"'{name}' isminde bir hesap zaten var. Lütfen farklı bir isim seçin."}, status_code=400)

    session_file = os.path.join(_DATA_DIR, f"{user.username}_{name.replace(' ', '_')}.session")
    client = TelegramClient(session_file, int(api_id), api_hash)
    try:
        await asyncio.wait_for(client.connect(), timeout=20)
    except asyncio.TimeoutError:
        return JSONResponse({"error": "Telegram sunucusuna bağlanma zaman aşıldı. Lütfen tekrar dene."}, status_code=408)
    except Exception as e:
        return JSONResponse({"error": f"Bağlantı hatası: {e}"}, status_code=400)

    try:
        authorized = await asyncio.wait_for(client.is_user_authorized(), timeout=10)
    except asyncio.TimeoutError:
        await client.disconnect()
        return JSONResponse({"error": "Yetkilendirme kontrolü zaman aşıldı."}, status_code=408)

    if authorized:
        me = await asyncio.wait_for(client.get_me(), timeout=10)
        raw_phone = getattr(me, "phone", None) or f"uid_{me.id}"
        phone = f"+{raw_phone}" if not str(raw_phone).startswith("+") else str(raw_phone)
        await client.disconnect()
        await asyncio.sleep(0.5)
        db2 = SessionLocal()
        exists2 = db2.query(Account).filter(Account.phone == phone).first()
        if not exists2:
            new_acc = Account(user_id=user.id, name=name, phone=phone, api_id=api_id,
                              api_hash=api_hash, session_file=session_file)
            db2.add(new_acc)
            db2.commit()
            db2.refresh(new_acc)
            asyncio.create_task(start_client(new_acc))
        else:
            if exists2.is_active is False:
                exists2.is_active = True
                db2.commit()
            if exists2.id not in clients:
                asyncio.create_task(start_client(exists2))
        db2.close()
        return JSONResponse({"status": "already_authorized"})

    try:
        qr_login = await asyncio.wait_for(client.qr_login(), timeout=15)
    except asyncio.TimeoutError:
        await client.disconnect()
        return JSONResponse({"error": "QR oluşturma zaman aşıldı. Tekrar dene."}, status_code=408)
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
    if status == "2fa_required":
        # Session'u silme — şifre bekleniyor
        return JSONResponse({"status": "2fa_required"})

    # Hâlâ bekliyor → QR URL taze mi kontrol et, yenile
    try:
        qr_login = session["qr"]
        try:
            await qr_login.recreate()
        except Exception:
            pass
        return JSONResponse({"status": "pending", "qr_b64": _make_qr_b64(qr_login.url)})
    except Exception:
        return JSONResponse({"status": "pending"})


@app.post("/accounts/qr-2fa")
async def qr_2fa_verify(
    account_id: str = Form(...),
    password: str = Form(...),
):
    """
    QR tarandıktan sonra 2 Adımlı Doğrulama şifresini alır ve girifi tamamlar.
    """
    session = qr_sessions.get(account_id)
    if not session:
        return JSONResponse({"error": "Oturum bulunamadı veya süresi doldu. Tekrar QR oluştur."}, status_code=404)
    if session.get("status") != "2fa_required":
        return JSONResponse({"error": "2FA beklenmiyordu."}, status_code=400)

    client: TelegramClient = session["client"]
    try:
        await asyncio.wait_for(client.sign_in(password=password), timeout=20)
    except asyncio.TimeoutError:
        return JSONResponse({"error": "Şifre doğrulama zaman aşıldı. Tekrar dene."}, status_code=408)
    except Exception as e:
        err = str(e)
        if "PASSWORD_HASH_INVALID" in err or "The password is invalid" in err:
            return JSONResponse({"error": "Hatalı şifre. Lütfen tekrar dene."}, status_code=400)
        return JSONResponse({"error": f"Giriş hatası: {err}"}, status_code=400)

    # Şifre doğru — hesabı kaydet
    session["status"] = "ok"
    asyncio.create_task(_finalize_qr_session(account_id))
    return JSONResponse({"status": "ok"})

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
    
    # Check if name is already used by this user
    if user:
        existing_acc = db.query(Account).filter(Account.user_id == user.id, Account.name == name).first()
        if existing_acc:
            return RedirectResponse(url=f"/accounts/add?error='{name}'+isminde+bir+hesap+zaten+var.+Lutfen+farkli+bir+isim+secin.", status_code=303)

    session_file = os.path.join(_DATA_DIR, f"{user.username if user else 'anon'}_{name.replace(' ', '_')}.session")
    client = TelegramClient(session_file, int(api_id), api_hash)
    try:
        await asyncio.wait_for(client.connect(), timeout=20)
    except asyncio.TimeoutError:
        return RedirectResponse(url=f"/accounts/verify?phone={phone.replace('+', '%2B')}&error=timeout", status_code=303)
    except Exception as e:
        return RedirectResponse(url=f"/accounts/add?error={str(e)}", status_code=303)

    try:
        result = await asyncio.wait_for(client.send_code_request(phone), timeout=20)
    except asyncio.TimeoutError:
        await client.disconnect()
        return RedirectResponse(url=f"/accounts/add?error=Telegram+kodu+gonderme+zaman+asimi", status_code=303)
    except Exception as e:
        await client.disconnect()
        return RedirectResponse(url=f"/accounts/add?error={str(e)}", status_code=303)

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
        await asyncio.wait_for(
            client.sign_in(phone, code, phone_code_hash=phone_code_hash),
            timeout=20
        )
    except SessionPasswordNeededError:
        # 2FA şifresi gerekiyor — client hala login_sessions'ta, 2FA sayfasina yönlendir
        print(f"[Phone] 2FA gerekli: {phone}")
        return RedirectResponse(
            url=f"/accounts/verify-2fa?phone={phone.replace('+', '%2B')}",
            status_code=303
        )
    except asyncio.TimeoutError:
        return RedirectResponse(url=f"/accounts/verify?phone={phone}&error=timeout", status_code=303)
    except Exception as e:
        print(f"Dogrulama hatasi: {e}")
        return RedirectResponse(url=f"/accounts/verify?phone={phone}", status_code=303)

    # 2FA yoksa başarılı — mevcut client'i kullan (disconnect/reconnect yok!)
    exists = db.query(Account).filter(Account.phone == phone).first()
    if exists:
        if uid and exists.user_id is None:
            exists.user_id = uid
            db.commit()
        asyncio.create_task(start_client(exists, _existing_client=client))
    else:
        new_acc = Account(
            user_id=uid, name=name, phone=phone,
            api_id=api_id, api_hash=api_hash, session_file=session_file
        )
        db.add(new_acc)
        db.commit()
        db.refresh(new_acc)
        asyncio.create_task(start_client(new_acc, _existing_client=client))

    del login_sessions[phone]
    return RedirectResponse(url="/", status_code=303)


@app.get("/accounts/verify-2fa", response_class=HTMLResponse)
async def verify_2fa_form(request: Request, phone: str, error: str = None):
    return templates.TemplateResponse(
        request=request, name="verify.html",
        context={"phone": phone, "step": "2fa", "error": error}
    )


@app.post("/accounts/verify-2fa")
async def verify_2fa_code(
    request: Request,
    phone: str = Form(...),
    password: str = Form(...),
    db: Session = Depends(get_db)
):
    if phone not in login_sessions:
        return RedirectResponse(url="/accounts/add", status_code=303)

    session_data = login_sessions[phone]
    if len(session_data) == 7:
        client, phone_code_hash, name, api_id, api_hash, session_file, saved_uid = session_data
    else:
        client, phone_code_hash, name, api_id, api_hash, session_file = session_data
        saved_uid = None

    user = get_current_user(request, db)
    uid = (user.id if user else None) or saved_uid

    try:
        await asyncio.wait_for(client.sign_in(password=password), timeout=20)
    except asyncio.TimeoutError:
        return RedirectResponse(
            url=f"/accounts/verify-2fa?phone={phone.replace('+', '%2B')}&error=Zaman+asimi",
            status_code=303
        )
    except Exception as e:
        err_msg = "Şifre hatalı" if "PASSWORD_HASH_INVALID" in str(e) or "invalid" in str(e).lower() else str(e)
        return RedirectResponse(
            url=f"/accounts/verify-2fa?phone={phone.replace('+', '%2B')}&error={err_msg}",
            status_code=303
        )

    # Başarılı — mevcut client'i kullan (disconnect/reconnect yok!)
    exists = db.query(Account).filter(Account.phone == phone).first()
    if exists:
        if uid and exists.user_id is None:
            exists.user_id = uid
            db.commit()
        asyncio.create_task(start_client(exists, _existing_client=client))
    else:
        new_acc = Account(
            user_id=uid, name=name, phone=phone,
            api_id=api_id, api_hash=api_hash, session_file=session_file
        )
        db.add(new_acc)
        db.commit()
        db.refresh(new_acc)
        asyncio.create_task(start_client(new_acc, _existing_client=client))

    del login_sessions[phone]
    return RedirectResponse(url="/", status_code=303)

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
    sender_name: str = Form(None),
    destination_id: str = Form(...),
    description: str = Form(None),
    show_forward_header: bool = Form(False),
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
        sender_name=sender_name.strip() if sender_name and sender_name.strip() else None,
        destination_id=destination_id,
        description=description,
        show_forward_header=show_forward_header
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

@app.post("/rules/{rule_id}/toggle")
async def toggle_rule(rule_id: int, request: Request, db: Session = Depends(get_db)):
    user = get_current_user(request, db)
    if not user:
        return RedirectResponse(url="/login", status_code=303)
    rule = db.query(Rule).filter(Rule.id == rule_id).first()
    if rule:
        rule.is_active = not rule.is_active
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

@app.post("/rules/{rule_id}/link-settings")
async def update_rule_links(
    rule_id: int,
    block_links: bool = Form(False),
    replace_link: str = Form(None),
    show_forward_header: bool = Form(False),
    db: Session = Depends(get_db)
):
    rule = db.query(Rule).filter(Rule.id == rule_id).first()
    if rule:
        rule.block_links = block_links
        rule.replace_link = replace_link if replace_link else None
        rule.show_forward_header = show_forward_header
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
        sender_name = row.get("sender_name", "").strip() or None

        client = clients.get(acc_id)
        if not client:
            errors.append(f"Hesap {acc_id} aktif değil — atlandı.")
            continue

        src  = await resolve(source_raw, client) or source_raw
        dest = await resolve(dest_raw, client)   or dest_raw
        sid  = await resolve(sender, client) if sender else None
        show_fwd = bool(row.get("show_forward_header", False)) or bool(body.get("show_forward_header", False))

        new_rule = Rule(
            account_id=acc_id,
            source_chat_id=src,
            sender_id=sid,
            sender_name=sender_name,
            destination_id=dest,
            description=description,
            show_forward_header=show_fwd
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


@app.post("/find-sender")
async def find_sender(
    message_link: str = Form(...),
    account_id: int = Form(...),
):
    """
    Verilen Telegram mesaj linkinden gönderen kişinin ID'sini bulur.
    İsim gizleyen veya anonim admin olan kullanıcılar için kullanılır.
    Desteklenen formatlar:
      - https://t.me/grupadi/123
      - https://t.me/c/1234567890/123   (özel grup)
      - t.me/c/...  veya  t.me/grupadi/...
    """
    from telethon import utils as tg_utils
    from telethon.tl.types import PeerChannel, PeerUser

    if account_id not in clients:
        return JSONResponse({"error": "Seçilen hesap aktif değil."}, status_code=400)

    client = clients[account_id]
    link = message_link.strip().rstrip("/")

    try:
        # ── Link'i parçala ──
        if "t.me/c/" in link:
            # Özel grup: t.me/c/<channel_id>/<msg_id>
            tail = link.split("t.me/c/")[-1]
            parts = [p for p in tail.split("/") if p]
            if len(parts) < 2:
                return JSONResponse({"error": "Link formatı yanlış. Örn: t.me/c/1234567/89"}, status_code=400)
            channel_id = int(parts[0])
            msg_id = int(parts[1])
            peer = await client.get_entity(PeerChannel(channel_id))
        elif "t.me/" in link:
            # Açık grup/kanal: t.me/<username>/<msg_id>
            tail = link.split("t.me/")[-1]
            parts = [p for p in tail.split("/") if p]
            if len(parts) < 2:
                return JSONResponse({"error": "Link formatı yanlış. Örn: t.me/grupadi/123"}, status_code=400)
            username = parts[0]
            msg_id = int(parts[1])
            peer = await client.get_entity(f"@{username}")
        else:
            return JSONResponse({"error": "Geçersiz link. t.me/... formatında olmalı."}, status_code=400)

        # ── Mesajı çek ──
        msg = await client.get_messages(peer, ids=msg_id)
        if not msg:
            return JSONResponse({"error": "Mesaj bulunamadı. Hesabın bu gruba üye olduğundan emin ol."}, status_code=404)

        # ── Göndereni belirle ──
        from_id = getattr(msg, "from_id", None)

        # Anonim admin: from_id PeerChannel olur (mesaj kanal kimliğiyle gönderilmiş)
        if from_id is None or isinstance(from_id, PeerChannel):
            chat_id = str(tg_utils.get_peer_id(peer))
            chat_title = getattr(peer, "title", "Anonim Admin")
            return JSONResponse({
                "id": chat_id,
                "name": chat_title,
                "username": "",
                "note": "⚠️ Bu kişi anonim admin olarak yazıyor. Filtre için grubun kendi ID'si kullanılır."
            })

        # Normal kullanıcı
        sender = await client.get_entity(from_id)
        sender_id = str(tg_utils.get_peer_id(sender))
        first = getattr(sender, "first_name", "") or ""
        last  = getattr(sender, "last_name",  "") or ""
        uname = getattr(sender, "username",   "") or ""
        display = f"{first} {last}".strip() or f"ID:{sender_id}"

        return JSONResponse({
            "id": sender_id,
            "name": display,
            "username": uname,
            "note": ""
        })

    except ValueError:
        return JSONResponse({"error": "Mesaj ID sayı olmalı."}, status_code=400)
    except Exception as e:
        return JSONResponse({"error": f"Hata: {str(e)}"}, status_code=400)



# ── HESAP DURDUR / DEVAM ETTİR ──

@app.post("/accounts/{account_id}/pause")
async def pause_account(account_id: int, request: Request, db: Session = Depends(get_db)):
    """Hesabın tüm kurallarını durdurur (is_active=False). Hesabı SİLMEZ."""
    user = get_current_user(request, db)
    if not user:
        return JSONResponse({"error": "Giriş yapılmamış"}, status_code=401)
    acc = db.query(Account).filter(Account.id == account_id).first()
    if not acc:
        return JSONResponse({"error": "Hesap bulunamadı"}, status_code=404)
    updated = db.query(Rule).filter(Rule.account_id == account_id).update({"is_active": False})
    db.commit()
    return JSONResponse({"ok": True, "paused_rules": updated})

@app.post("/accounts/{account_id}/resume")
async def resume_account(account_id: int, request: Request, db: Session = Depends(get_db)):
    """Hesabın tüm kurallarını başlatır (is_active=True). Hesabı SİLMEZ."""
    user = get_current_user(request, db)
    if not user:
        return JSONResponse({"error": "Giriş yapılmamış"}, status_code=401)
    acc = db.query(Account).filter(Account.id == account_id).first()
    if not acc:
        return JSONResponse({"error": "Hesap bulunamadı"}, status_code=404)
    updated = db.query(Rule).filter(Rule.account_id == account_id).update({"is_active": True})
    db.commit()
    return JSONResponse({"ok": True, "resumed_rules": updated})

@app.post("/accounts/pause-all")
async def pause_all_accounts(request: Request, db: Session = Depends(get_db)):
    """Tüm hesapların tüm kurallarını durdurur."""
    user = get_current_user(request, db)
    if not user:
        return JSONResponse({"error": "Giriş yapılmamış"}, status_code=401)
    account_ids = [a.id for a in db.query(Account).filter(Account.user_id == user.id).all()]
    if account_ids:
        updated = db.query(Rule).filter(Rule.account_id.in_(account_ids)).update({"is_active": False}, synchronize_session=False)
        db.commit()
    else:
        updated = 0
    return JSONResponse({"ok": True, "paused_rules": updated})

@app.post("/accounts/resume-all")
async def resume_all_accounts(request: Request, db: Session = Depends(get_db)):
    """Tüm hesapların tüm kurallarını başlatır."""
    user = get_current_user(request, db)
    if not user:
        return JSONResponse({"error": "Giriş yapılmamış"}, status_code=401)
    account_ids = [a.id for a in db.query(Account).filter(Account.user_id == user.id).all()]
    if account_ids:
        updated = db.query(Rule).filter(Rule.account_id.in_(account_ids)).update({"is_active": True}, synchronize_session=False)
        db.commit()
    else:
        updated = 0
    return JSONResponse({"ok": True, "resumed_rules": updated})

@app.post("/accounts/bulk-pause")
async def bulk_pause_accounts(request: Request, db: Session = Depends(get_db)):
    """Seçilen hesapların kurallarını durdurur. JSON body: {"account_ids": [1,2,3]}"""
    user = get_current_user(request, db)
    if not user:
        return JSONResponse({"error": "Giriş yapılmamış"}, status_code=401)
    body = await request.json()
    ids = body.get("account_ids", [])
    if not ids:
        return JSONResponse({"error": "Hesap seçilmedi"}, status_code=400)
    # Sadece bu kullanıcının hesaplarını etkile
    allowed = [a.id for a in db.query(Account).filter(Account.user_id == user.id, Account.id.in_(ids)).all()]
    updated = db.query(Rule).filter(Rule.account_id.in_(allowed)).update({"is_active": False}, synchronize_session=False)
    db.commit()
    return JSONResponse({"ok": True, "paused_rules": updated})

@app.post("/accounts/bulk-resume")
async def bulk_resume_accounts(request: Request, db: Session = Depends(get_db)):
    """Seçilen hesapların kurallarını başlatır. JSON body: {"account_ids": [1,2,3]}"""
    user = get_current_user(request, db)
    if not user:
        return JSONResponse({"error": "Giriş yapılmamış"}, status_code=401)
    body = await request.json()
    ids = body.get("account_ids", [])
    if not ids:
        return JSONResponse({"error": "Hesap seçilmedi"}, status_code=400)
    allowed = [a.id for a in db.query(Account).filter(Account.user_id == user.id, Account.id.in_(ids)).all()]
    updated = db.query(Rule).filter(Rule.account_id.in_(allowed)).update({"is_active": True}, synchronize_session=False)
    db.commit()
    return JSONResponse({"ok": True, "resumed_rules": updated})


# ── TOPLU FİLTRE UYGULA ──

@app.post("/filters/bulk-copy")
async def bulk_copy_filter(request: Request, db: Session = Depends(get_db)):
    """
    Bir kuralın filtrelerini seçilen hesapların aynı kaynak→hedef kurallarına kopyalar.
    JSON body:
    {
      "source_rule_id": 5,        # Bu kuralın filtreleri kopyalanacak
      "target_account_ids": [1,2,3],  # Bu hesaplara kopyala (boş = tüm hesaplar)
      "copy_all_accounts": false  # true ise tüm hesaplara uygula
    }
    """
    user = get_current_user(request, db)
    if not user:
        return JSONResponse({"error": "Giriş yapılmamış"}, status_code=401)
    
    body = await request.json()
    source_rule_id = body.get("source_rule_id")
    target_account_ids = body.get("target_account_ids", [])
    copy_all = body.get("copy_all_accounts", False)
    
    if not source_rule_id:
        return JSONResponse({"error": "source_rule_id gerekli"}, status_code=400)
    
    # Kaynak kuralı al
    source_rule = db.query(Rule).filter(Rule.id == source_rule_id).first()
    if not source_rule:
        return JSONResponse({"error": "Kaynak kural bulunamadı"}, status_code=404)
    
    source_filters = db.query(WordFilter).filter(WordFilter.rule_id == source_rule_id).all()
    if not source_filters:
        return JSONResponse({"error": "Kaynak kuralda filtre yok"}, status_code=400)
    
    # Hedef hesapları belirle
    user_account_ids = [a.id for a in db.query(Account).filter(Account.user_id == user.id).all()]
    
    if copy_all:
        effective_ids = user_account_ids
    else:
        effective_ids = [i for i in target_account_ids if i in user_account_ids]
    
    # Kaynak hesabı hariç tut (zaten var)
    effective_ids = [i for i in effective_ids if i != source_rule.account_id]
    
    if not effective_ids:
        return JSONResponse({"error": "Geçerli hedef hesap yok"}, status_code=400)
    
    added = 0
    skipped = 0
    
    for acc_id in effective_ids:
        # Bu hesabın aynı kaynak→hedef kuralını bul
        target_rule = db.query(Rule).filter(
            Rule.account_id == acc_id,
            Rule.source_chat_id == source_rule.source_chat_id,
            Rule.destination_id == source_rule.destination_id
        ).first()
        
        if not target_rule:
            skipped += 1
            continue
        
        # Filtreleri ekle (zaten varsa atla)
        existing_words = {f.search_word for f in db.query(WordFilter).filter(WordFilter.rule_id == target_rule.id).all()}
        
        for f in source_filters:
            if f.search_word not in existing_words:
                db.add(WordFilter(
                    rule_id=target_rule.id,
                    search_word=f.search_word,
                    replace_word=f.replace_word
                ))
                added += 1
    
    db.commit()
    return JSONResponse({
        "ok": True,
        "filters_added": added,
        "accounts_skipped": skipped,
        "reason": f"{skipped} hesapta aynı kaynak→hedef kural bulunamadı" if skipped else ""
    })

async def delayed_react(account_id, chat_id, msg_id, emojis_str, delay_max_minutes):
    import random
    
    # 1 sn ile delay_max_minutes (dk) arasında rastgele bekle
    delay_sec = random.uniform(1, max(1, delay_max_minutes * 60))
    print(f"[Emoji Task] Hesab {account_id} -> {delay_sec:.1f} saniye bekleyecek...")
    await asyncio.sleep(delay_sec)
    
    emoji_list = [e.strip() for e in emojis_str.split(',') if e.strip()]
    if not emoji_list:
        return
    
    # 1 adet rastgele emoji seç
    chosen = random.choice(emoji_list)
    
    # Telegram sadece baz emojileri reaksiyon olarak kabul eder. (Örn: 👏🏻 kabul etmez, 👏 kabul eder).
    # Ten rengi vb. eklentileri (modifiers) temizleyelim:
    for m in ['\U0001f3fb', '\U0001f3fc', '\U0001f3fd', '\U0001f3fe', '\U0001f3ff']:
        chosen = chosen.replace(m, '')

    client = clients.get(account_id)
    if not client:
        print(f"[Emoji Task] Hesap {account_id} aktif değil, emoji atılamadı.")
        return
    
    try:
        from telethon import functions, types
        await client(functions.messages.SendReactionRequest(
            peer=chat_id,
            msg_id=msg_id,
            reaction=[types.ReactionEmoji(emoticon=chosen)]
        ))
        print(f"[Emoji Task] ✅ {chosen} emojisi atıldı! (Hesap {account_id}, msg={msg_id})")
    except Exception as e:
        print(f"[Emoji Task] ❌ Emoji atılamadı (Hesap {account_id}): {e}")


# ── HOCA EMOJISI (TEACHERS) ──

@app.get("/teachers", response_class=HTMLResponse)
async def teachers_page(request: Request, db: Session = Depends(get_db)):
    user = get_current_user(request, db)
    if not user:
        return RedirectResponse(url="/login", status_code=303)
    accounts = db.query(Account).filter(Account.user_id == user.id).all()
    teachers = db.query(Teacher).all()
    
    return templates.TemplateResponse(request=request, name="teachers.html", context={"accounts": accounts, "teachers": teachers})

@app.post("/teachers/add")
async def add_teacher(
    request: Request,
    name: str = Form(...),
    source_chat_id: str = Form(...),
    teacher_user_id: str = Form(...),
    delay_max_minutes: int = Form(1),
    db: Session = Depends(get_db)
):
    user = get_current_user(request, db)
    if not user:
        return JSONResponse({"error": "Giriş yapılmamış"}, status_code=401)
    
    teacher = Teacher(
        name=name,
        source_chat_id=source_chat_id,
        teacher_user_id=teacher_user_id,
        delay_max_minutes=delay_max_minutes
    )
    db.add(teacher)
    db.commit()
    return RedirectResponse(url="/teachers", status_code=303)

@app.post("/teachers/delete/{t_id}")
async def delete_teacher(request: Request, t_id: int, db: Session = Depends(get_db)):
    user = get_current_user(request, db)
    if not user:
        return JSONResponse({"error": "Giriş yapılmamış"}, status_code=401)
    
    t = db.query(Teacher).filter(Teacher.id == t_id).first()
    if t:
        db.delete(t)
        db.commit()
    return JSONResponse({"ok": True})

@app.post("/teachers/{t_id}/reactions/add_bulk")
async def add_teacher_reactions_bulk(request: Request, t_id: int, db: Session = Depends(get_db)):
    user = get_current_user(request, db)
    if not user:
        return JSONResponse({"error": "Giriş yapılmamış"}, status_code=401)
    
    body = await request.json()
    for row in body:
        acc_id = row.get("account_id")
        emojis = row.get("emojis", "").strip()
        if acc_id and emojis:
            existing = db.query(TeacherReaction).filter(
                TeacherReaction.teacher_id == t_id,
                TeacherReaction.account_id == acc_id
            ).first()
            if existing:
                existing.emojis = emojis
            else:
                db.add(TeacherReaction(teacher_id=t_id, account_id=acc_id, emojis=emojis))
            
    db.commit()
    return JSONResponse({"ok": True})

@app.post("/teachers/{t_id}/reactions/delete/{acc_id}")
async def delete_teacher_reaction(request: Request, t_id: int, acc_id: int, db: Session = Depends(get_db)):
    user = get_current_user(request, db)
    if not user:
        return JSONResponse({"error": "Giriş yapılmamış"}, status_code=401)
        
    db.query(TeacherReaction).filter(
        TeacherReaction.teacher_id == t_id,
        TeacherReaction.account_id == acc_id
    ).delete()
    db.commit()
    return JSONResponse({"ok": True})


# ── HÜKÜMDAR & YÖNETİM ENDPOINTLERİ ──

def check_hukumdar_auth(request: Request) -> bool:
    return bool(request.session.get("is_hukumdar"))

@app.post("/tasks/{task_id}/complete")
async def complete_task(
    task_id: int,
    request: Request,
    feedback: str = Form(""),
    db: Session = Depends(get_db)
):
    user = get_current_user(request, db)
    if not user:
        return RedirectResponse(url="/login", status_code=303)
    task = db.query(UserTask).filter(UserTask.id == task_id).first()
    if task and (task.user_id == user.id or task.user_id is None):
        task.status = "completed"
        task.feedback = feedback.strip() if feedback else "Tamamlandı olarak işaretlendi."
        task.updated_at = datetime.datetime.utcnow().isoformat()
        db.commit()
    return RedirectResponse(url="/", status_code=303)

@app.post("/accounts/rename")
async def rename_account(
    request: Request,
    account_id: int = Form(...),
    name: str = Form(...),
    db: Session = Depends(get_db)
):
    user = get_current_user(request, db)
    is_admin = check_hukumdar_auth(request)
    if not user and not is_admin:
        return RedirectResponse(url="/login", status_code=303)

    acc = db.query(Account).filter(Account.id == account_id).first()
    if acc:
        if is_admin or (user and acc.user_id == user.id):
            acc.name = name.strip()
            db.commit()

    ref = request.headers.get("referer", "/")
    return RedirectResponse(url=ref, status_code=303)

@app.post("/accounts/{account_id}/rename")
async def rename_account_path(
    account_id: int,
    request: Request,
    name: str = Form(...),
    db: Session = Depends(get_db)
):
    return await rename_account(request=request, account_id=account_id, name=name, db=db)

@app.post("/rules/edit")
async def edit_rule(
    request: Request,
    rule_id: int = Form(...),
    account_id: int = Form(...),
    source_chat_id: str = Form(...),
    destination_id: str = Form(...),
    sender_id: str = Form(None),
    sender_name: str = Form(None),
    description: str = Form(None),
    block_links: bool = Form(False),
    replace_link: str = Form(None),
    show_forward_header: bool = Form(False),
    db: Session = Depends(get_db)
):
    user = get_current_user(request, db)
    is_admin = check_hukumdar_auth(request)
    if not user and not is_admin:
        return RedirectResponse(url="/login", status_code=303)

    rule = db.query(Rule).filter(Rule.id == rule_id).first()
    if rule:
        if is_admin or (user and rule.account and rule.account.user_id == user.id):
            rule.account_id = account_id
            rule.source_chat_id = source_chat_id.strip()
            rule.destination_id = destination_id.strip()
            rule.sender_id = sender_id.strip() if sender_id and sender_id.strip() else None
            rule.sender_name = sender_name.strip() if sender_name and sender_name.strip() else None
            rule.description = description.strip() if description and description.strip() else None
            rule.block_links = block_links
            rule.replace_link = replace_link.strip() if replace_link and replace_link.strip() else None
            rule.show_forward_header = show_forward_header
            db.commit()

    ref = request.headers.get("referer", "/")
    return RedirectResponse(url=ref, status_code=303)

@app.post("/rules/{rule_id}/edit")
async def edit_rule_path(
    rule_id: int,
    request: Request,
    account_id: int = Form(...),
    source_chat_id: str = Form(...),
    destination_id: str = Form(...),
    sender_id: str = Form(None),
    sender_name: str = Form(None),
    description: str = Form(None),
    block_links: bool = Form(False),
    replace_link: str = Form(None),
    show_forward_header: bool = Form(False),
    db: Session = Depends(get_db)
):
    return await edit_rule(
        request=request,
        rule_id=rule_id,
        account_id=account_id,
        source_chat_id=source_chat_id,
        destination_id=destination_id,
        sender_id=sender_id,
        sender_name=sender_name,
        description=description,
        block_links=block_links,
        replace_link=replace_link,
        show_forward_header=show_forward_header,
        db=db
    )

@app.get("/hukumdar", response_class=HTMLResponse)
async def hukumdar_panel(request: Request, db: Session = Depends(get_db)):
    if not check_hukumdar_auth(request):
        return RedirectResponse(url="/hukumdar/login", status_code=303)

    users = db.query(User).order_by(User.id.asc()).all()
    accounts = db.query(Account).order_by(Account.id.asc()).all()
    rules = db.query(Rule).order_by(Rule.id.asc()).all()
    tasks = db.query(UserTask).order_by(UserTask.id.desc()).all()
    ignored_members = db.query(IgnoredMember).order_by(IgnoredMember.id.desc()).all()
    active_client_ids = set(clients.keys())
    user_acc_counts = {u.id: sum(1 for a in accounts if a.user_id == u.id) for u in users}
    pending_tasks_count = sum(1 for t in tasks if t.status == "pending")

    return templates.TemplateResponse(
        request=request,
        name="hukumdar.html",
        context={
            "users": users,
            "accounts": accounts,
            "rules": rules,
            "tasks": tasks,
            "ignored_members": ignored_members,
            "active_client_ids": active_client_ids,
            "user_acc_counts": user_acc_counts,
            "pending_tasks_count": pending_tasks_count
        }
    )

@app.get("/hukumdar/login", response_class=HTMLResponse)
async def hukumdar_login_page(request: Request):
    if check_hukumdar_auth(request):
        return RedirectResponse(url="/hukumdar", status_code=303)
    error = request.session.pop("hukumdar_error", None)
    return templates.TemplateResponse(request=request, name="hukumdar_login.html", context={"error": error})

@app.post("/hukumdar/login")
async def hukumdar_login_post(
    request: Request,
    username: str = Form(...),
    password: str = Form(...)
):
    valid_un = username.strip().lower() in ["hükümdar", "hukumdar"]
    valid_pw = password.strip() in ["hükümdar123", "hukumdar123"]
    if valid_un and valid_pw:
        request.session["is_hukumdar"] = True
        return RedirectResponse(url="/hukumdar", status_code=303)
    else:
        request.session["hukumdar_error"] = "Hatalı Hükümdar kullanıcı adı veya şifresi!"
        return RedirectResponse(url="/hukumdar/login", status_code=303)

@app.get("/hukumdar/logout")
async def hukumdar_logout(request: Request):
    request.session.pop("is_hukumdar", None)
    return RedirectResponse(url="/hukumdar/login", status_code=303)

@app.post("/hukumdar/accounts/move")
async def hukumdar_move_account(
    request: Request,
    account_id: int = Form(...),
    target_user_id: int = Form(...),
    db: Session = Depends(get_db)
):
    if not check_hukumdar_auth(request):
        return RedirectResponse(url="/hukumdar/login", status_code=303)
    acc = db.query(Account).filter(Account.id == account_id).first()
    if acc:
        acc.user_id = target_user_id
        db.commit()
    return RedirectResponse(url="/hukumdar", status_code=303)

@app.post("/hukumdar/accounts/copy")
async def hukumdar_copy_account(
    request: Request,
    account_id: int = Form(...),
    target_user_id: int = Form(...),
    copy_rules: bool = Form(False),
    db: Session = Depends(get_db)
):
    if not check_hukumdar_auth(request):
        return RedirectResponse(url="/hukumdar/login", status_code=303)
    src_acc = db.query(Account).filter(Account.id == account_id).first()
    if src_acc:
        import shutil
        new_phone = f"{src_acc.phone}_u{target_user_id}"
        new_session = src_acc.session_file
        if os.path.exists(src_acc.session_file):
            new_session = src_acc.session_file.replace(".session", f"_u{target_user_id}.session")
            try:
                shutil.copy2(src_acc.session_file, new_session)
            except Exception:
                new_session = src_acc.session_file

        new_acc = Account(
            user_id=target_user_id,
            name=f"{src_acc.name} (Kopya)",
            phone=new_phone,
            api_id=src_acc.api_id,
            api_hash=src_acc.api_hash,
            session_file=new_session,
            is_active=src_acc.is_active
        )
        db.add(new_acc)
        db.commit()
        db.refresh(new_acc)

        if copy_rules:
            for r in src_acc.rules:
                cloned_rule = Rule(
                    account_id=new_acc.id,
                    source_chat_id=r.source_chat_id,
                    sender_id=r.sender_id,
                    sender_name=r.sender_name,
                    destination_id=r.destination_id,
                    is_active=r.is_active,
                    description=r.description,
                    block_links=r.block_links,
                    replace_link=r.replace_link,
                    show_forward_header=getattr(r, 'show_forward_header', False)
                )
                db.add(cloned_rule)
                db.commit()
                db.refresh(cloned_rule)
                for f in r.filters:
                    db.add(WordFilter(rule_id=cloned_rule.id, search_word=f.search_word, replace_word=f.replace_word))
            db.commit()

        asyncio.create_task(start_client(new_acc))

    return RedirectResponse(url="/hukumdar", status_code=303)

@app.post("/hukumdar/accounts/batch-transfer")
async def hukumdar_batch_transfer(
    request: Request,
    source_user_id: int = Form(...),
    target_user_id: int = Form(...),
    action_type: str = Form(...),
    db: Session = Depends(get_db)
):
    if not check_hukumdar_auth(request):
        return RedirectResponse(url="/hukumdar/login", status_code=303)

    if source_user_id == target_user_id:
        return RedirectResponse(url="/hukumdar", status_code=303)

    src_accs = db.query(Account).filter(Account.user_id == source_user_id).all()
    if action_type == "move":
        for acc in src_accs:
            acc.user_id = target_user_id
        db.commit()
    elif action_type == "copy_rules":
        target_accs = db.query(Account).filter(Account.user_id == target_user_id).all()
        if target_accs:
            first_target_acc = target_accs[0]
            for acc in src_accs:
                for r in acc.rules:
                    cloned_r = Rule(
                        account_id=first_target_acc.id,
                        source_chat_id=r.source_chat_id,
                        sender_id=r.sender_id,
                        sender_name=r.sender_name,
                        destination_id=r.destination_id,
                        is_active=r.is_active,
                        description=f"{r.description or ''} (Kopya)",
                        block_links=r.block_links,
                        replace_link=r.replace_link,
                        show_forward_header=getattr(r, 'show_forward_header', False)
                    )
                    db.add(cloned_r)
                    db.commit()
                    db.refresh(cloned_r)
                    for f in r.filters:
                        db.add(WordFilter(rule_id=cloned_r.id, search_word=f.search_word, replace_word=f.replace_word))
            db.commit()

    return RedirectResponse(url="/hukumdar", status_code=303)

@app.post("/hukumdar/rules/copy")
async def hukumdar_copy_rule(
    request: Request,
    rule_id: int = Form(...),
    target_account_id: int = Form(...),
    db: Session = Depends(get_db)
):
    if not check_hukumdar_auth(request):
        return RedirectResponse(url="/hukumdar/login", status_code=303)
    r = db.query(Rule).filter(Rule.id == rule_id).first()
    if r:
        cloned = Rule(
            account_id=target_account_id,
            source_chat_id=r.source_chat_id,
            sender_id=r.sender_id,
            sender_name=r.sender_name,
            destination_id=r.destination_id,
            is_active=r.is_active,
            description=r.description,
            block_links=r.block_links,
            replace_link=r.replace_link,
            show_forward_header=getattr(r, 'show_forward_header', False)
        )
        db.add(cloned)
        db.commit()
        db.refresh(cloned)
        for f in r.filters:
            db.add(WordFilter(rule_id=cloned.id, search_word=f.search_word, replace_word=f.replace_word))
        db.commit()
    return RedirectResponse(url="/hukumdar", status_code=303)

@app.post("/hukumdar/rules/move")
async def hukumdar_move_rule(
    request: Request,
    rule_id: int = Form(...),
    target_account_id: int = Form(...),
    db: Session = Depends(get_db)
):
    if not check_hukumdar_auth(request):
        return RedirectResponse(url="/hukumdar/login", status_code=303)
    r = db.query(Rule).filter(Rule.id == rule_id).first()
    if r:
        r.account_id = target_account_id
        db.commit()
    return RedirectResponse(url="/hukumdar", status_code=303)

@app.post("/hukumdar/users/change-password")
async def hukumdar_change_password(
    request: Request,
    user_id: int = Form(...),
    new_password: str = Form(...),
    db: Session = Depends(get_db)
):
    if not check_hukumdar_auth(request):
        return RedirectResponse(url="/hukumdar/login", status_code=303)
    u = db.query(User).filter(User.id == user_id).first()
    if u and new_password.strip():
        u.password_hash = hash_password(new_password.strip())
        db.commit()
    return RedirectResponse(url="/hukumdar", status_code=303)

@app.post("/hukumdar/tasks/create")
async def hukumdar_create_task(
    request: Request,
    user_id: str = Form(...),
    title: str = Form(...),
    content: str = Form(...),
    deadline: str = Form(None),
    db: Session = Depends(get_db)
):
    if not check_hukumdar_auth(request):
        return RedirectResponse(url="/hukumdar/login", status_code=303)

    now_iso = datetime.datetime.utcnow().isoformat()
    if user_id == "all":
        users = db.query(User).all()
        for u in users:
            db.add(UserTask(
                user_id=u.id,
                title=title.strip(),
                content=content.strip(),
                deadline=deadline.strip() if deadline and deadline.strip() else None,
                status="pending",
                created_at=now_iso
            ))
    else:
        db.add(UserTask(
            user_id=int(user_id),
            title=title.strip(),
            content=content.strip(),
            deadline=deadline.strip() if deadline and deadline.strip() else None,
            status="pending",
            created_at=now_iso
        ))
    db.commit()
    return RedirectResponse(url="/hukumdar", status_code=303)

@app.post("/hukumdar/tasks/{task_id}/delete")
async def hukumdar_delete_task(
    task_id: int,
    request: Request,
    db: Session = Depends(get_db)
):
    if not check_hukumdar_auth(request):
        return RedirectResponse(url="/hukumdar/login", status_code=303)
    t = db.query(UserTask).filter(UserTask.id == task_id).first()
    if t:
        db.delete(t)
        db.commit()
    return RedirectResponse(url="/hukumdar", status_code=303)

@app.post("/hukumdar/tasks/{task_id}/status")
async def hukumdar_toggle_task_status(
    task_id: int,
    request: Request,
    status: str = Form(...),
    db: Session = Depends(get_db)
):
    if not check_hukumdar_auth(request):
        return RedirectResponse(url="/hukumdar/login", status_code=303)
    t = db.query(UserTask).filter(UserTask.id == task_id).first()
    if t:
        t.status = status
        db.commit()
    return RedirectResponse(url="/hukumdar", status_code=303)

@app.post("/members/inspect")
async def inspect_members(
    request: Request,
    account_id: int = Form(...),
    chat_a: str = Form(...),
    chat_b: str = Form(...),
    db: Session = Depends(get_db)
):
    if account_id not in clients:
        return JSONResponse({"error": "Seçilen hesap şu an aktif veya bağlı değil."}, status_code=400)

    client: TelegramClient = clients[account_id]

    async def get_chat_members(chat_str):
        chat_str = chat_str.strip()
        if chat_str.lstrip("-").isdigit():
            chat_entity = int(chat_str)
        else:
            chat_entity = chat_str

        members = {}
        try:
            entity = await client.get_entity(chat_entity)
            async for user in client.iter_participants(entity, limit=2000):
                if getattr(user, "deleted", False):
                    continue
                first = getattr(user, "first_name", "") or ""
                last = getattr(user, "last_name", "") or ""
                full_name = f"{first} {last}".strip() or "İsimsiz"
                u_phone = getattr(user, "phone", "")
                members[user.id] = {
                    "id": user.id,
                    "name": full_name,
                    "username": getattr(user, "username", "") or "",
                    "phone": f"+{u_phone}" if u_phone else ""
                }
        except Exception as e:
            print(f"[Inspect] get_participants hatası ({chat_str}): {e}")
            raise e
        return members

    try:
        members_a = await asyncio.wait_for(get_chat_members(chat_a), timeout=45)
    except Exception as e:
        return JSONResponse({"error": f"Grup A üyeleri alınamadı: {e}"}, status_code=400)

    try:
        members_b = await asyncio.wait_for(get_chat_members(chat_b), timeout=45)
    except Exception as e:
        return JSONResponse({"error": f"Grup B üyeleri alınamadı: {e}"}, status_code=400)

    ignored_rows = db.query(IgnoredMember).all()
    ignored_set = set()
    for row in ignored_rows:
        val = row.identifier.strip().lower().lstrip("+").lstrip("@")
        ignored_set.add(val)

    common_ids = set(members_a.keys()).intersection(set(members_b.keys()))
    common_members = []
    for uid in common_ids:
        m = members_a[uid]
        u_id_str = str(m["id"])
        u_phone_str = m["phone"].lstrip("+")
        u_uname_str = m["username"].lower()

        if u_id_str in ignored_set or (u_phone_str and u_phone_str in ignored_set) or (u_uname_str and u_uname_str in ignored_set):
            continue
        common_members.append(m)

    return JSONResponse({
        "count_a": len(members_a),
        "count_b": len(members_b),
        "common_count": len(common_members),
        "common_members": common_members
    })

@app.post("/members/kick")
async def kick_chat_member(
    request: Request,
    account_id: int = Form(...),
    chat_id: str = Form(...),
    user_id: int = Form(...),
    db: Session = Depends(get_db)
):
    if account_id not in clients:
        return JSONResponse({"error": "Hesap aktif değil"}, status_code=400)

    client: TelegramClient = clients[account_id]
    chat_str = chat_id.strip()
    chat_entity = int(chat_str) if chat_str.lstrip("-").isdigit() else chat_str

    try:
        entity = await client.get_entity(chat_entity)
        user_entity = await client.get_entity(user_id)
        await client.kick_participant(entity, user_entity)
        return JSONResponse({"ok": True})
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=400)

@app.post("/ignored-members/add")
async def add_ignored_member(
    request: Request,
    identifier: str = Form(...),
    note: str = Form(None),
    db: Session = Depends(get_db)
):
    clean = identifier.strip()
    if clean:
        existing = db.query(IgnoredMember).filter(IgnoredMember.identifier == clean).first()
        if not existing:
            db.add(IgnoredMember(identifier=clean, note=note, created_at=datetime.datetime.utcnow().isoformat()))
            db.commit()
    ref = request.headers.get("referer", "/hukumdar")
    return RedirectResponse(url=ref, status_code=303)

@app.post("/ignored-members/{item_id}/delete")
async def delete_ignored_member(
    item_id: int,
    request: Request,
    db: Session = Depends(get_db)
):
    item = db.query(IgnoredMember).filter(IgnoredMember.id == item_id).first()
    if item:
        db.delete(item)
        db.commit()
    ref = request.headers.get("referer", "/hukumdar")
    return RedirectResponse(url=ref, status_code=303)


# ── TOPLU SİLME & KONTROL ENDPOINTLERİ ──

@app.post("/accounts/bulk-delete")
async def bulk_delete_accounts(request: Request, db: Session = Depends(get_db)):
    user = get_current_user(request, db)
    is_admin = check_hukumdar_auth(request)
    if not user and not is_admin:
        return JSONResponse({"error": "Giriş yapılmamış"}, status_code=401)

    try:
        body = await request.json()
        account_ids = body.get("account_ids", [])
    except Exception:
        account_ids = []

    if not account_ids:
        return JSONResponse({"error": "Hiçbir hesap seçilmedi"}, status_code=400)

    deleted_count = 0
    for acc_id in account_ids:
        acc = db.query(Account).filter(Account.id == acc_id).first()
        if acc:
            if is_admin or (user and acc.user_id == user.id):
                if acc.id in clients:
                    try:
                        await clients[acc.id].disconnect()
                    except Exception:
                        pass
                    del clients[acc.id]
                if acc.session_file and os.path.exists(acc.session_file):
                    try:
                        os.remove(acc.session_file)
                    except Exception:
                        pass
                db.delete(acc)
                deleted_count += 1

    db.commit()
    return JSONResponse({"ok": True, "deleted_count": deleted_count})


@app.post("/rules/bulk-delete")
async def bulk_delete_rules(request: Request, db: Session = Depends(get_db)):
    user = get_current_user(request, db)
    is_admin = check_hukumdar_auth(request)
    if not user and not is_admin:
        return JSONResponse({"error": "Giriş yapılmamış"}, status_code=401)

    try:
        body = await request.json()
        rule_ids = body.get("rule_ids", [])
    except Exception:
        rule_ids = []

    if not rule_ids:
        return JSONResponse({"error": "Hiçbir kural seçilmedi"}, status_code=400)

    deleted_count = 0
    for r_id in rule_ids:
        rule = db.query(Rule).filter(Rule.id == r_id).first()
        if rule:
            if is_admin or (user and rule.account and rule.account.user_id == user.id):
                db.delete(rule)
                deleted_count += 1

    db.commit()
    return JSONResponse({"ok": True, "deleted_count": deleted_count})


@app.post("/rules/bulk-toggle")
async def bulk_toggle_rules(request: Request, db: Session = Depends(get_db)):
    user = get_current_user(request, db)
    is_admin = check_hukumdar_auth(request)
    if not user and not is_admin:
        return JSONResponse({"error": "Giriş yapılmamış"}, status_code=401)

    try:
        body = await request.json()
        rule_ids = body.get("rule_ids", [])
        action = body.get("action", "toggle")
    except Exception:
        rule_ids = []
        action = "toggle"

    if not rule_ids:
        return JSONResponse({"error": "Hiçbir kural seçilmedi"}, status_code=400)

    count = 0
    for r_id in rule_ids:
        rule = db.query(Rule).filter(Rule.id == r_id).first()
        if rule:
            if is_admin or (user and rule.account and rule.account.user_id == user.id):
                if action == "pause":
                    rule.is_active = False
                elif action == "resume":
                    rule.is_active = True
                else:
                    rule.is_active = not rule.is_active
                count += 1

    db.commit()
    return JSONResponse({"ok": True, "count": count})


if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 10000))
    uvicorn.run("app:app", host="0.0.0.0", port=port)

