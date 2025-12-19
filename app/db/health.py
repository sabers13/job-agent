from sqlalchemy import text

from app.db.engine import get_engine


def check_db() -> bool:
    eng = get_engine()
    with eng.connect() as conn:
        conn.execute(text("SELECT 1"))
    return True
