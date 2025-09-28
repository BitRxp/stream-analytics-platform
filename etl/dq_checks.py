import pandas as pd

class DQError(Exception):
    pass

def assert_non_empty(df: pd.DataFrame, name: str):
    if df.empty:
        raise DQError(f"{name} is empty")

def assert_ratings_range(ratings: pd.DataFrame):
    ok = ratings["rating"].between(0.5, 5.0, inclusive="both").all()
    if not ok:
        raise DQError("rating values out of [0.5, 5.0]")

def assert_unique_rating_key(ratings: pd.DataFrame):
    key = ratings[["userId", "movieId", "timestamp"]]
    if len(key.drop_duplicates()) != len(key):
        raise DQError("duplicate (userId,movieId,timestamp) in ratings")

def run_basic_checks(tables: dict):
    assert_non_empty(tables["dim_movie"], "dim_movie")
    assert_non_empty(tables["fact_rating"], "fact_rating")
    assert_non_empty(tables["dim_genre"], "dim_genre")
    assert_ratings_range(tables["fact_rating"])
    assert_unique_rating_key(tables["fact_rating"])
