from pathlib import Path
import sqlite3
from etl.extract import load_movielens
from etl.transform import transform
from etl.dq_checks import run_basic_checks

DATA_DIR = Path("data") / "ml-latest-small"

def test_transform_and_dq():
    movies, ratings, tags, links = load_movielens(str(DATA_DIR))
    tables = transform(movies, ratings, tags, links)
    run_basic_checks(tables)  # raises if something wrong
    assert {"dim_movie","fact_rating","dim_genre","bridge_movie_genre"} <= set(tables)

def test_gold_not_empty(tmp_path):
    # quick end-to-end on SQLite temp DB
    from etl.load import load_to_db
    from etl.build_gold import build_gold
    movies, ratings, tags, links = load_movielens(str(DATA_DIR))
    tables = transform(movies, ratings, tags, links)
    db_path = tmp_path / "test.sqlite"
    load_to_db(tables, str(db_path), engine="sqlite")
    build_gold(str(db_path), engine="sqlite")
    con = sqlite3.connect(db_path)
    n = con.execute("SELECT COUNT(*) FROM gold_movie_stats").fetchone()[0]
    con.close()
    assert n > 0
