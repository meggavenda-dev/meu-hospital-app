
import sqlite3

def get_internacao_by_atendimento(atendimento):
    conn = sqlite3.connect("dados.db")
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM Internacoes WHERE atendimento = ?", (atendimento,))
    row = cursor.fetchone()
    conn.close()
    return row

def criar_internacao(numero_internacao, hospital, atendimento, paciente, data_internacao, convenio):
    conn = sqlite3.connect("dados.db")
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

def criar_procedimento(internacao_id, data_procedimento, profissional, procedimento):
    conn = sqlite3.connect("dados.db")
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO Procedimentos (internacao_id, data_procedimento, profissional, procedimento)
        VALUES (?, ?, ?, ?)
    """, (internacao_id, data_procedimento, profissional, procedimento))
    conn.commit()
    conn.close()
