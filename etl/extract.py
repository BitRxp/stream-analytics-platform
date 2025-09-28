from pathlib import Path
import pandas as pd

def load_movielens(data_dir: str):
    p = Path(data_dir)
    movies = pd.read_csv(p / "movies.csv")
    ratings = pd.read_csv(p / "ratings.csv")
    tags = pd.read_csv(p / "tags.csv")
    links = pd.read_csv(p / "links.csv")
    return movies, ratings, tags, links
