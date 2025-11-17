# src/io/db_writer.py
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError

from src.settings import runtime as R

_engine = None
_Session = None
_session = None


def _ensure_session():
    """Create engine/session on first use, but only if WRITE_TO_DB is True."""
    global _engine, _Session, _session
    if not R.WRITE_TO_DB:
        raise RuntimeError("DB writing is disabled (WRITE_TO_DB=false).")

    if _session is not None:
        return _session

    from src.settings.infra import DATABASE_URL # Uncomment and change to your actual import path

    _engine = create_engine(DATABASE_URL, echo=False)
    _Session = sessionmaker(bind=_engine)
    _session = _Session()
    return _session


def insert_data(model_class, data):
    """
    Lazily open a session and perform bulk insert.
    """
    sess = _ensure_session()
    try:
        sess.bulk_insert_mappings(model_class, data)
        sess.commit()
        print(f"Successfully inserted data into {model_class.__tablename__}")
    except SQLAlchemyError as e:
        sess.rollback()
        print(f"Error inserting data into {model_class.__tablename__}: {e}")
    # Do not close sess so batch inserts can reuse it; provide an explicit close if needed.


def close_session():
    global _engine, _Session, _session
    if _session is not None:
        _session.close()
    _engine = None
    _Session = None
    _session = None
