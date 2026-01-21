
import sqlite3

def create_tables():
    conn = sqlite3.connect("dados.db")
    cursor = conn.cursor()

    cursor.execute("""
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

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS Procedimentos (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        internacao_id INTEGER,
        data_procedimento TEXT,
        profissional TEXT,
        procedimento TEXT,
        FOREIGN KEY(internacao_id) REFERENCES Internacoes(id)
    );
    """)

    conn.commit()
    conn.close()
