from sqlalchemy.exc import DBAPIError, OperationalError
from app.db.session import SessionLocal, ping_db, is_transient_db_error


def check_db() -> dict:
    db = SessionLocal()
    try:
        ping_db(db)
        return {"ok": True}
    except (OperationalError, DBAPIError) as exc:
        return {
            "ok": False,
            "error_type": exc.__class__.__name__,
            "transient": is_transient_db_error(exc),
            "error": (str(exc)[:300] if str(exc) else ""),
        }
    except Exception as exc:
        return {
            "ok": False,
            "error_type": exc.__class__.__name__,
            "transient": False,
            "error": (str(exc)[:300] if str(exc) else repr(exc)),
        }
    finally:
        db.close()
