import duckdb
import os

DB_PATH = "data/github.duckdb"
DATA_PATH = "data/github_sample.json.gz"

print("Conectando ao DuckDB...")
con = duckdb.connect(DB_PATH)

print("Criando schema raw...")
con.execute("CREATE SCHEMA IF NOT EXISTS raw")

print("Carregando dados do GitHub Archive...")
con.execute(f"""
    CREATE OR REPLACE TABLE raw.github_events AS
    SELECT
        id,
        type,
        json(actor)      AS actor,
        json(repo)       AS repo,
        json(payload)    AS payload,
        public,
        created_at::TIMESTAMP AS created_at,
        json(org)        AS org
    FROM read_json('{DATA_PATH}',
        columns = {{
            'id': 'VARCHAR',
            'type': 'VARCHAR',
            'actor': 'JSON',
            'repo': 'JSON',
            'payload': 'JSON',
            'public': 'BOOLEAN',
            'created_at': 'VARCHAR',
            'org': 'JSON'
        }},
        format = 'newline_delimited'
    )
""")

count = con.execute("SELECT COUNT(*) FROM raw.github_events").fetchone()[0]
print(f"✅ {count:,} eventos carregados na tabela raw.github_events")

types = con.execute("""
    SELECT type, COUNT(*) as total
    FROM raw.github_events
    GROUP BY type
    ORDER BY total DESC
""").fetchdf()

print("\n📊 Distribuição por tipo de evento:")
print(types.to_string(index=False))

con.close()