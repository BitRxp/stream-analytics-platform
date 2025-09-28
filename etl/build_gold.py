import sqlite3, duckdb

SQL = {
    "gold_movie_stats": """
    CREATE TABLE gold_movie_stats AS
    SELECT r.movieId,
           AVG(r.rating)  AS avg_rating,
           COUNT(*)       AS n_ratings,
           MAX(r.ts_utc)  AS last_rating_at,
           m.title, m.year
    FROM fact_rating r
    JOIN dim_movie m USING (movieId)
    GROUP BY r.movieId, m.title, m.year;
    """,
    "gold_activity_by_hour": """
    CREATE TABLE gold_activity_by_hour AS
    SELECT rating_hour, COUNT(*) AS events
    FROM fact_rating
    GROUP BY 1
    ORDER BY 1;
    """,
    "gold_genre_year": """
    CREATE TABLE gold_genre_year AS
    SELECT g.genre, r.rating_year AS year,
           COUNT(*) AS ratings_count,
           AVG(r.rating) AS avg_rating
    FROM fact_rating r
    JOIN bridge_movie_genre USING (movieId)
    JOIN dim_genre g USING (genre_id)
    GROUP BY 1,2;
    """
}

def build_gold(db_path: str, engine: str = "sqlite"):
    if engine == "sqlite":
        con = sqlite3.connect(db_path)
        cur = con.cursor()
        for name, q in SQL.items():
            cur.executescript(f"DROP TABLE IF EXISTS {name};\n{q}")
        con.commit()
        con.close()
    elif engine == "duckdb":
        con = duckdb.connect(db_path)
        for name, q in SQL.items():
            con.execute(f"DROP TABLE IF EXISTS {name}")
            con.execute(q)
        con.close()
    else:
        raise ValueError(f"Unsupported engine: {engine}")
