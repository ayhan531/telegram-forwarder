import os
import hashlib
from sqlalchemy import Column, Integer, String, Boolean, ForeignKey, text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship
from sqlalchemy import create_engine

# Render'da /data persistent disk, lokalde . (mevcut dizin)
DATA_DIR = os.environ.get("DATA_DIR", ".")
SQLALCHEMY_DATABASE_URL = f"sqlite:///{DATA_DIR}/telegram_forwarder.db"

engine = create_engine(
    SQLALCHEMY_DATABASE_URL,
    connect_args={"check_same_thread": False, "timeout": 30},
    pool_size=10,
    max_overflow=20,
)

# WAL modu: eş zamanlı okuma/yazma çakışmasını önler
with engine.connect() as _conn:
    _conn.execute(text("PRAGMA journal_mode=WAL"))
    _conn.execute(text("PRAGMA busy_timeout=10000"))
    _conn.commit()

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()


def hash_password(password: str) -> str:
    return hashlib.sha256(password.encode()).hexdigest()


class User(Base):
    __tablename__ = "users"
    id            = Column(Integer, primary_key=True, index=True)
    username      = Column(String, unique=True, index=True)
    password_hash = Column(String)
    display_name  = Column(String)   # Cemal, Furkan, vs.

    accounts = relationship("Account", back_populates="owner", cascade="all, delete-orphan")


class Account(Base):
    __tablename__ = "accounts"
    id           = Column(Integer, primary_key=True, index=True)
    user_id      = Column(Integer, ForeignKey("users.id"), nullable=True)
    name         = Column(String)
    phone        = Column(String, unique=True)
    api_id       = Column(String)
    api_hash     = Column(String)
    session_file = Column(String)
    is_active    = Column(Boolean, default=True)

    owner = relationship("User",  back_populates="accounts")
    rules = relationship("Rule",  back_populates="account", cascade="all, delete-orphan")


class Rule(Base):
    __tablename__ = "rules"
    id             = Column(Integer, primary_key=True, index=True)
    account_id     = Column(Integer, ForeignKey("accounts.id"))
    source_chat_id = Column(String, index=True)
    sender_id      = Column(String, index=True, nullable=True)
    destination_id = Column(String)
    is_active      = Column(Boolean, default=True)
    description    = Column(String, nullable=True)

    account = relationship("Account",    back_populates="rules")
    filters = relationship("WordFilter", back_populates="rule", cascade="all, delete-orphan")


class WordFilter(Base):
    __tablename__ = "word_filters"
    id           = Column(Integer, primary_key=True, index=True)
    rule_id      = Column(Integer, ForeignKey("rules.id"))
    search_word  = Column(String)
    replace_word = Column(String)

    rule = relationship("Rule", back_populates="filters")


class MessageMapping(Base):
    __tablename__ = "message_mappings"
    id                  = Column(Integer, primary_key=True, index=True)
    account_id          = Column(Integer, nullable=True, index=True)  # hangi hesap iletti
    original_chat_id    = Column(String, index=True)
    original_msg_id     = Column(Integer, index=True)
    destination_chat_id = Column(String, index=True)
    forwarded_msg_id    = Column(Integer)
    last_reactions      = Column(String, nullable=True, default="")


def init_db():
    # ── Tabloları oluştur ──
    Base.metadata.create_all(bind=engine)

    # ── Otomatik migration: eksik kolonları ekle ──
    _migrations = [
        "ALTER TABLE message_mappings ADD COLUMN last_reactions TEXT DEFAULT ''",
        "ALTER TABLE accounts ADD COLUMN user_id INTEGER REFERENCES users(id)",
        "ALTER TABLE message_mappings ADD COLUMN account_id INTEGER",
    ]
    for _sql in _migrations:
        try:
            with engine.connect() as conn:
                conn.execute(text(_sql))
                conn.commit()
        except Exception:
            pass  # Zaten varsa sessizce geç

    # ── Varsayılan kullanıcıları oluştur ──
    _DEFAULT_USERS = [
        ("kayhan",  "ky_5510_*",  "Kayhan"),
        ("levent",  "lv_4432_?",  "Levent"),
        ("cuneyt",  "cn_9821_!",  "Cüneyt"),
    ]

    db = SessionLocal()

    # ── Eski kullanıcıları düzgün taşı / sil ──
    # Eski ad → yeni ad (None = sil, hesapları sahipsiz bırak)
    _rename_map = {
        "cemal":  None,
        "comert": "cuneyt",
        "tolga":  "levent",
        "furkan": "kayhan",
    }
    try:
        for old_un, new_un in _rename_map.items():
            old_user = db.query(User).filter(User.username == old_un).first()
            if not old_user:
                continue

            if new_un:
                new_user = db.query(User).filter(User.username == new_un).first()
                if new_user:
                    # Hedef zaten var → hesapları hedefe aktar, eskiyi sil
                    db.query(Account).filter(Account.user_id == old_user.id)\
                        .update({"user_id": new_user.id}, synchronize_session=False)
                    print(f"[Migration] {old_un} hesaplari {new_un}'e aktarildi.")
                else:
                    # Hedef yok → sadece yeniden adlandir
                    old_user.username = new_un
                    old_user.display_name = new_un.capitalize()
                    # Silme yapma, kayit hala gecerli
                    continue
            else:
                # cemal vb. → hesaplari NULL yap (unclaimed), kullaniciyi sil
                db.query(Account).filter(Account.user_id == old_user.id)\
                    .update({"user_id": None}, synchronize_session=False)
                print(f"[Migration] {old_un} hesaplari sahipsiz birakildi.")

            db.delete(old_user)

        # Sifre ve display_name guncelle
        _updates = {
            "cuneyt": ("Cuneyt", "cn_9821_!"),
            "levent": ("Levent", "lv_4432_?"),
            "kayhan": ("Kayhan", "ky_5510_*"),
        }
        for uname, (disp, pwd) in _updates.items():
            u = db.query(User).filter(User.username == uname).first()
            if u:
                u.display_name  = disp
                u.password_hash = hash_password(pwd)

        db.commit()
    except Exception as e:
        print(f"[Migration] User migration error: {e}")
        db.rollback()

    # ── Eksik kullanicilari ekle ──
    for _uname, _pwd, _display in _DEFAULT_USERS:
        if not db.query(User).filter(User.username == _uname).first():
            db.add(User(
                username=_uname,
                password_hash=hash_password(_pwd),
                display_name=_display
            ))
    db.commit()

    # ── Sahipsiz hesaplari kurtar: user_id var ama kullanici silinmis ──
    try:
        valid_ids = [u.id for u in db.query(User).all()]
        if valid_ids:
            orphaned = db.query(Account).filter(
                Account.user_id != None,
                ~Account.user_id.in_(valid_ids)
            ).all()
            for acc in orphaned:
                print(f"[Migration] Orphan duzeltildi: {acc.name} user_id={acc.user_id} -> NULL")
                acc.user_id = None
            if orphaned:
                db.commit()
    except Exception as e:
        print(f"[Migration] Orphan cleanup error: {e}")
        db.rollback()

    db.close()
