import pandas as pd
import numpy as np
import re

def _extract_year(title: str):
    m = re.search(r"\((\d{4})\)\s*$", str(title))
    return int(m.group(1)) if m else np.nan

def transform(movies, ratings, tags, links):
    movies = movies.copy()
    ratings = ratings.copy()
    tags = tags.copy()
    links = links.copy()

    movies["year"] = movies["title"].apply(_extract_year)
    movies["genres"] = movies["genres"].replace("(no genres listed)", np.nan)

    ratings["ts_utc"] = pd.to_datetime(ratings["timestamp"], unit="s", utc=True)
    ratings["rating_date"] = ratings["ts_utc"].dt.date
    ratings["rating_year"] = ratings["ts_utc"].dt.year
    ratings["rating_month"] = ratings["ts_utc"].dt.month
    ratings["rating_day"] = ratings["ts_utc"].dt.day
    ratings["rating_hour"] = ratings["ts_utc"].dt.hour
    ratings["rating_dow"] = ratings["ts_utc"].dt.dayofweek

    if "timestamp" in tags.columns:
        tags["ts_utc"] = pd.to_datetime(tags["timestamp"], unit="s", utc=True)
        tags["tag_year"] = tags["ts_utc"].dt.year

    mg = (
        movies.assign(genres_list=movies["genres"].str.split("|"))[["movieId", "genres_list"]]
        .explode("genres_list")
        .rename(columns={"genres_list": "genre"})
    )
    mg["genre"] = mg["genre"].str.strip()
    mg = mg.dropna(subset=["genre"]).drop_duplicates()

    dim_genre = pd.DataFrame({"genre": sorted(mg["genre"].unique().tolist())})
    dim_genre["genre_id"] = range(1, len(dim_genre) + 1)
    bridge_movie_genre = mg.merge(dim_genre, on="genre", how="left")[["movieId", "genre_id"]]

    links = links.astype({"imdbId": "string", "tmdbId": "string"})

    return {
        "dim_movie": movies[["movieId", "title", "genres", "year"]],
        "dim_genre": dim_genre[["genre_id", "genre"]],
        "bridge_movie_genre": bridge_movie_genre,
        "fact_rating": ratings,
        "fact_tag": tags,
        "dim_links": links.rename(columns={"movieId": "movieId"}),
    }
