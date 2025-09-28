import duckdb, sqlite3

def load_to_db(tables: dict, db_path: str, engine: str = "sqlite"):
    if engine == "duckdb":
        con = duckdb.connect(db_path)
        for name, df in tables.items():
            con.execute(f"DROP TABLE IF EXISTS {name}")
            con.register(f"{name}_v", df)
            con.execute(f"CREATE TABLE {name} AS SELECT * FROM {name}_v")
            con.unregister(f"{name}_v")
        con.close()

    elif engine == "sqlite":
        con = sqlite3.connect(db_path)
        cur = con.cursor()
        for name, df in tables.items():
            cur.execute(f"DROP TABLE IF EXISTS {name}")
            df.to_sql(name, con, index=False)
        con.commit()
        con.close()

    else:
        raise ValueError(f"Unsupported engine: {engine}")

    return db_path
