
import sqlite3

DB_PATH = "dados.db"

def get_internacao_by_atendimento(atendimento):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM Internacoes WHERE atendimento = ?", (atendimento,))
    row = cursor.fetchone()
    conn.close()
    return row

def get_internacao_id_by_atendimento(atendimento):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("SELECT id FROM Internacoes WHERE atendimento = ?", (atendimento,))
    row = cursor.fetchone()
    conn.close()
    return row[0] if row else None

def criar_internacao(numero_internacao, hospital, atendimento, paciente, data_internacao, convenio):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO Internacoes
        (numero_internacao, hospital, atendimento, paciente, data_internacao, convenio)
        VALUES (?, ?, ?, ?, ?, ?)
    """, (numero_internacao, hospital, atendimento, paciente, data_internacao, convenio))
    conn.commit()
    internacao_id = cursor.lastrowid
    conn.close()
    return internacao_id

def criar_procedimento(internacao_id, data_procedimento, profissional, procedimento,
                       situacao="Pendente", observacao=None):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO Procedimentos
        (internacao_id, data_procedimento, profissional, procedimento, situacao, observacao)
        VALUES (?, ?, ?, ?, ?, ?)
    """, (internacao_id, data_procedimento, profissional, procedimento, situacao, observacao))
    conn.commit()
    conn.close()

def existe_procedimento_no_dia(internacao_id, data_procedimento):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("""
        SELECT 1 FROM Procedimentos
        WHERE internacao_id = ? AND data_procedimento = ?
        LIMIT 1
    """, (internacao_id, data_procedimento))
    ok = cursor.fetchone() is not None
    conn.close()
    return ok
