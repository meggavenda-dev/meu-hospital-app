
import streamlit as st
import sqlite3
import pandas as pd
import re

# -------------------------------------------------------------
# CREATE TABLES
# -------------------------------------------------------------
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

create_tables()

# -------------------------------------------------------------
# PARSER PARA SUA PLANILHA REAL
# -------------------------------------------------------------
def parse_csv(file):
    linhas = file.read().decode("latin1", errors="ignore").splitlines()

    registros = []
    data_atual = None

    atendimento = None
    paciente = None

    for linha in linhas:
        original = linha
        linha = linha.strip().replace("\x00", "")

        if linha == "":
            continue

        # Detecta a linha da data
        if "Data de Realização" in linha:
            partes = linha.split(",")
            for p in partes:
                p = p.strip()
                if re.match(r"\d{2}/\d{2}/\d{4}", p):
                    data_atual = p
            continue

        # Detecta linha mestre
        if re.match(r"^,\s*\d{7,12}", original):
            partes = original.split(",")

            atendimento = partes[1].strip()
            paciente = partes[2].strip() if len(partes) > 2 else ""

            procedimento = partes[10].strip() if len(partes) > 10 else ""
            convenio = partes[11].strip() if len(partes) > 11 else ""
            profissional = partes[12].strip() if len(partes) > 12 else ""

            if procedimento:
                registros.append({
                    "atendimento": atendimento,
                    "paciente": paciente,
                    "data": data_atual,
                    "procedimento": procedimento,
                    "convenio": convenio,
                    "profissional": profissional
                })

            continue

        # Linhas filhas (procedimentos extras)
        if original.startswith(",,,,,,,,,,"):
            partes = original.split(",")

            procedimento = partes[10].strip() if len(partes) > 10 else ""
            convenio = partes[11].strip() if len(partes) > 11 else ""
            profissional = partes[12].strip() if len(partes) > 12 else ""

            if atendimento and procedimento:
                registros.append({
                    "atendimento": atendimento,
                    "paciente": paciente,
                    "data": data_atual,
                    "procedimento": procedimento,
                    "convenio": convenio,
                    "profissional": profissional
                })

    return registros


# -------------------------------------------------------------
# BANCO DE DADOS - FUNÇÕES CRUD
# -------------------------------------------------------------
def get_internacao_by_atendimento(att):
    conn = sqlite3.connect("dados.db")
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM Internacoes WHERE atendimento = ?", (att,))
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
    novo_id = cursor.lastrowid
    conn.close()
    return novo_id

def criar_procedimento(internacao_id, data_procedimento, profissional, procedimento):
    conn = sqlite3.connect("dados.db")
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO Procedimentos
        (internacao_id, data_procedimento, profissional, procedimento)
        VALUES (?, ?, ?, ?)
    """, (internacao_id, data_procedimento, profissional, procedimento))
    conn.commit()
    conn.close()


# -------------------------------------------------------------
# INTERFACE STREAMLIT
# -------------------------------------------------------------
st.set_page_config(page_title="Gestão de Internações", layout="wide")
st.title("🏥 Sistema de Importação e Consulta Hospitalar")

tabs = st.tabs(["📤 Importar Arquivo", "🔍 Consultar Internação",
                "📋 Procedimentos", "🧾 Profissionais", "💸 Convênios"])


# -------------------------------------------------------------
# 📤 ABA 1 — IMPORTAR
# -------------------------------------------------------------
with tabs[0]:
    st.header("📤 Importar arquivo CSV")

    hospital = st.text_input("Digite o nome do hospital (obrigatório):")
    arquivo = st.file_uploader("Selecione o arquivo CSV")

    if arquivo and hospital.strip():
        registros = parse_csv(arquivo)
        st.success(f"{len(registros)} itens interpretados no arquivo!")

        for r in registros:
            atendimento = r["atendimento"]
            paciente = r["paciente"]
            data = r["data"]
            proc = r["procedimento"]
            prof = r["profissional"]
            conv = r["convenio"]

            existente = get_internacao_by_atendimento(atendimento)

            if not existente:
                internacao_id = criar_internacao(
                    float(atendimento),
                    hospital,
                    atendimento,
                    paciente,
                    data,
                    conv
                )
            else:
                internacao_id = existente[0]

            criar_procedimento(
                internacao_id,
                data,
                prof,
                proc
            )

        st.success("Importação concluída com sucesso! 🎉")


# -------------------------------------------------------------
# 🔍 ABA 2 — CONSULTAR INTERNAÇÃO
# -------------------------------------------------------------
with tabs[1]:
    st.header("🔍 Consultar Internação")

    codigo = st.text_input("Digite o número do atendimento:")

    if codigo:
        conn = sqlite3.connect("dados.db")
        df_int = pd.read_sql_query(
            f"SELECT * FROM Internacoes WHERE atendimento = '{codigo}'", conn)

        if df_int.empty:
            st.warning("Nenhuma internação encontrada.")
        else:
            st.subheader("Dados da internação")
            st.dataframe(df_int)

            internacao_id = df_int["id"].iloc[0]

            df_proc = pd.read_sql_query(
                f"SELECT * FROM Procedimentos WHERE internacao_id = {internacao_id}",
                conn
            )
            st.subheader("Procedimentos registrados")
            st.dataframe(df_proc)

        conn.close()


# -------------------------------------------------------------
# 📋 ABA 3 — LISTA DE PROCEDIMENTOS
# -------------------------------------------------------------
with tabs[2]:
    st.header("📋 Lista de Procedimentos")

    if st.button("Carregar"):
        conn = sqlite3.connect("dados.db")
        df = pd.read_sql_query("""
            SELECT P.id, I.atendimento, I.paciente, P.data_procedimento,
                   P.profissional, P.procedimento, I.convenio
            FROM Procedimentos P
            INNER JOIN Internacoes I ON I.id = P.internacao_id
            ORDER BY P.data_procedimento DESC
        """, conn)
        st.dataframe(df)
        conn.close()


# -------------------------------------------------------------
# 🧾 ABA 4 — RESUMO POR PROFISSIONAL
# -------------------------------------------------------------
with tabs[3]:
    st.header("🧾 Resumo por Profissional")

    conn = sqlite3.connect("dados.db")
    df = pd.read_sql_query("""
        SELECT profissional,
               COUNT(*) AS total_procedimentos
        FROM Procedimentos
        WHERE profissional IS NOT NULL AND profissional <> ''
        GROUP BY profissional
        ORDER BY total_procedimentos DESC
    """, conn)
    conn.close()

    st.dataframe(df)


# -------------------------------------------------------------
# 💸 ABA 5 — RESUMO POR CONVÊNIO
# -------------------------------------------------------------
with tabs[4]:
    st.header("💸 Resumo por Convênio")

    conn = sqlite3.connect("dados.db")
    df = pd.read_sql_query("""
        SELECT convenio,
               COUNT(*) AS total_procedimentos
        FROM Internacoes I
        INNER JOIN Procedimentos P ON P.internacao_id = I.id
        WHERE convenio IS NOT NULL AND convenio <> ''
        GROUP BY convenio
        ORDER BY total_procedimentos DESC
    """, conn)
    conn.close()

    st.dataframe(df)
