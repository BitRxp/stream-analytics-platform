from pathlib import Path
import argparse, os, time, logging
from dotenv import load_dotenv
from etl.extract import load_movielens
from etl.transform import transform
from etl.load import load_to_db
from etl.build_gold import build_gold
from etl.dq_checks import run_basic_checks, DQError

def setup_logging(level: str = "INFO"):
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(message)s"
    )

def main(engine: str | None = None):
    load_dotenv()

    engine = (engine or os.getenv("ENGINE", "sqlite")).lower()
    data_dir = Path(os.getenv("DATA_DIR", "data/ml-latest-small"))
    db_path = os.getenv("DB_PATH", f"data/movielens.{ 'sqlite' if engine=='sqlite' else 'duckdb'}")
    log_level = os.getenv("LOG_LEVEL", "INFO")
    setup_logging(log_level)

    t0 = time.time()
    logging.info("ETL start | engine=%s | data_dir=%s | db=%s", engine, data_dir, db_path)

    movies, ratings, tags, links = load_movielens(str(data_dir))
    logging.info("extract done: movies=%d, ratings=%d, tags=%d, links=%d",
                 len(movies), len(ratings), len(tags), len(links))

    tables = transform(movies, ratings, tags, links)
    run_basic_checks(tables)
    logging.info("data-quality checks: OK")
    # small row-count telemetry
    for name, df in tables.items():
        logging.info("transform table=%s rows=%d cols=%d", name, len(df), len(df.columns))

    load_to_db(tables, db_path=db_path, engine=engine)
    logging.info("load done → %s", db_path)

    build_gold(db_path=db_path, engine=engine)
    logging.info("gold built")

    logging.info("ETL finished in %.2fs", time.time() - t0)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run ETL pipeline")
    parser.add_argument("--engine", choices=["sqlite", "duckdb"], default=None)
    args = parser.parse_args()
    main(args.engine)
