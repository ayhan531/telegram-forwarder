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
    original_chat_id    = Column(String, index=True)
    original_msg_id     = Column(Integer, index=True)
    destination_chat_id = Column(String, index=True)
    forwarded_msg_id    = Column(Integer)
    last_reactions      = Column(String, nullable=True, default="")


# ── Tabloları oluştur ──
Base.metadata.create_all(bind=engine)

# ── Otomatik migration: eksik kolonları ekle ──
_migrations = [
    "ALTER TABLE message_mappings ADD COLUMN last_reactions TEXT DEFAULT ''",
    "ALTER TABLE accounts ADD COLUMN user_id INTEGER REFERENCES users(id)",
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
    ("kayhan",  "kayhan123",  "Kayhan"),
    ("levent",  "levent123",  "Levent"),
    ("cuneyt",  "cuneyt123",  "Cüneyt"),
]

db = SessionLocal()

# Eskileri sil/güncelle (Migration)
try:
    # Cemal'i sil
    db.query(User).filter(User.username == "cemal").delete()
    
    # Eskileri yeni isimlere taşı
    mapping = {
        "comert": ("cuneyt", "Cüneyt", "cuneyt123"),
        "tolga":  ("levent", "Levent", "levent123"),
        "furkan": ("kayhan", "Kayhan", "kayhan123"),
    }
    for old_un, (new_un, new_disp, new_pwd) in mapping.items():
        existing = db.query(User).filter(User.username == old_un).first()
        if existing:
            existing.username = new_un
            existing.display_name = new_disp
            existing.password_hash = hash_password(new_pwd)

    db.commit()
except Exception as e:
    print(f"User migration error: {e}")
    db.rollback()

# Eksikleri ekle
for _uname, _pwd, _display in _DEFAULT_USERS:
    if not db.query(User).filter(User.username == _uname).first():
        db.add(User(
            username=_uname,
            password_hash=hash_password(_pwd),
            display_name=_display
        ))
db.commit()
db.close()
