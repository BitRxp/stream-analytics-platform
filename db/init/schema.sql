CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;


DROP TABLE IF EXISTS silver.dim_movie CASCADE;
CREATE TABLE silver.dim_movie (
  movieId   INTEGER PRIMARY KEY,
  title     TEXT,
  genres    TEXT,
  year      INTEGER
);

DROP TABLE IF EXISTS silver.dim_genre CASCADE;
CREATE TABLE silver.dim_genre (
  genre_id  INTEGER PRIMARY KEY,
  genre     TEXT NOT NULL
);

DROP TABLE IF EXISTS silver.bridge_movie_genre CASCADE;
CREATE TABLE silver.bridge_movie_genre (
  movieId   INTEGER NOT NULL,
  genre_id  INTEGER NOT NULL
);

DROP TABLE IF EXISTS silver.fact_rating CASCADE;
CREATE TABLE silver.fact_rating (
  userId INTEGER,
  movieId INTEGER,
  rating NUMERIC,
  timestamp BIGINT,
  ts_utc TIMESTAMPTZ,
  rating_date DATE,
  rating_year INT,
  rating_month INT,
  rating_day INT,
  rating_hour INT,
  rating_dow INT
);

DROP TABLE IF EXISTS silver.fact_tag CASCADE;
CREATE TABLE silver.fact_tag (
  userId INTEGER,
  movieId INTEGER,
  tag TEXT,
  timestamp BIGINT,
  ts_utc TIMESTAMPTZ,
  tag_year INT
);

DROP TABLE IF EXISTS silver.dim_links CASCADE;
CREATE TABLE silver.dim_links (
  movieId INTEGER PRIMARY KEY,
  imdbId  TEXT,
  tmdbId  TEXT
);


DROP TABLE IF EXISTS gold.movie_stats;
CREATE TABLE gold.movie_stats (
  movieId INTEGER,
  avg_rating NUMERIC,
  n_ratings BIGINT,
  last_rating_at TIMESTAMPTZ,
  title TEXT,
  year INT
);

DROP TABLE IF EXISTS gold.genre_year;
CREATE TABLE gold.genre_year (
  genre TEXT,
  year INT,
  ratings_count BIGINT,
  avg_rating NUMERIC
);

DROP TABLE IF EXISTS gold.activity_by_hour;
CREATE TABLE gold.activity_by_hour (
  rating_hour INT,
  events BIGINT
);
