
import sqlite3

DB_PATH = "dados.db"

def create_tables():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    # Internações
    c.execute("""
    CREATE TABLE IF NOT EXISTS Internacoes (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        numero_internacao REAL,
        hospital TEXT,
        atendimento TEXT UNIQUE,
        paciente TEXT,
        data_internacao TEXT,
        convenio TEXT
    );
    """)

    # Procedimentos (já com situação/observação e UNIQUE por (internação, data))
    c.execute("""
    CREATE TABLE IF NOT EXISTS Procedimentos (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        internacao_id INTEGER,
        data_procedimento TEXT,
        profissional TEXT,
        procedimento TEXT,
        situacao TEXT NOT NULL DEFAULT 'Pendente',
        observacao TEXT,
        FOREIGN KEY(internacao_id) REFERENCES Internacoes(id),
        UNIQUE(internacao_id, data_procedimento)
    );
    """)

    conn.commit()
    conn.close()
