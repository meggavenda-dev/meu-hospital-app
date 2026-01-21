
import streamlit as st
import sqlite3
import pandas as pd
import re

# =====================================================================
# BANCO - CRIAÇÃO DE TABELAS E SEED
# =====================================================================
def get_conn():
    # Ativa FKs por segurança futura (caso você crie FKs)
    conn = sqlite3.connect("dados.db")
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn

def create_tables():
    conn = get_conn()
    cur = conn.cursor()

    # Tabela de Hospitais (catálogo)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS Hospitals (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT UNIQUE NOT NULL,
        active INTEGER NOT NULL DEFAULT 1
    );
    """)

    # Internações
    cur.execute("""
    CREATE TABLE IF NOT EXISTS Internacoes (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        numero_internacao REAL,
        hospital TEXT,           -- mantido como texto para compatibilidade
        atendimento TEXT UNIQUE,  -- atendimento é único
        paciente TEXT,
        data_internacao TEXT,
        convenio TEXT
    );
    """)

    # Procedimentos
    cur.execute("""
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

def seed_hospitais():
    """
    Semeia os hospitais uma única vez (operações idempotentes).
    Atualize a lista se quiser incluir mais hospitais padronizados.
    """
    HOSPITAIS_INICIAIS = [
        "Santa Lucia Sul",
        "Santa Lucia Norte",
        "Maria Auxiliadora",
        # --- adicionados conforme seu pedido ---
        "Santa Lucia Taguatinga",
        "Santa Lucia Águas Claras",
        "Santa Lucia Sudoeste",
    ]
    conn = get_conn()
    cur = conn.cursor()
    for nome in HOSPITAIS_INICIAIS:
        cur.execute("INSERT OR IGNORE INTO Hospitals (name, active) VALUES (?, 1)", (nome,))
    conn.commit()
    conn.close()

def get_hospitais_ativos():
    conn = get_conn()
    df = pd.read_sql_query("SELECT name FROM Hospitals WHERE active = 1 ORDER BY name ASC", conn)
    conn.close()
    return df["name"].tolist()


# =====================================================================
# PARSER ESPECIAL PARA SUA PLANILHA
# =====================================================================
def parse_csv(file):
    """
    Lê o relatório 'CSV-like' da sua planilha, identificando:
      - mudança de data ("Data de Realização")
      - linhas mestre (começam por ,<atendimento>)
      - linhas filhas (procedimentos adicionais iniciados por ,,,,,,,,,,)
    Retorna lista de dicts com: atendimento, paciente, data, procedimento, convenio, profissional
    """
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

        # Detecta a data do bloco
        if "Data de Realização" in linha:
            partes = linha.split(",")
            for p in partes:
                p = p.strip()
                if re.match(r"\d{2}/\d{2}/\d{4}", p):
                    data_atual = p
            continue

        # Linha mestre: atendimento começa na 2ª coluna (linha inicia com vírgula e um número longo)
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

        # Linhas filhas (procedimentos extras): começam com 10 vírgulas
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


# =====================================================================
# BANCO - FUNÇÕES CRUD
# =====================================================================
def get_internacao_by_atendimento(att):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT * FROM Internacoes WHERE atendimento = ?", (att,))
    row = cur.fetchone()
    conn.close()
    return row

def criar_internacao(numero_internacao, hospital, atendimento, paciente, data_internacao, convenio):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO Internacoes
        (numero_internacao, hospital, atendimento, paciente, data_internacao, convenio)
        VALUES (?, ?, ?, ?, ?, ?)
    """, (numero_internacao, hospital, atendimento, paciente, data_internacao, convenio))
    conn.commit()
    new_id = cur.lastrowid
    conn.close()
    return new_id

def criar_procedimento(internacao_id, data_procedimento, profissional, procedimento):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO Procedimentos
        (internacao_id, data_procedimento, profissional, procedimento)
        VALUES (?, ?, ?, ?)
    """, (internacao_id, data_procedimento, profissional, procedimento))
    conn.commit()
    conn.close()


# =====================================================================
# BOOTSTRAP DO BANCO
# =====================================================================
create_tables()
seed_hospitais()  # garante hospitais iniciais


# =====================================================================
# APP - UI
# =====================================================================
st.set_page_config(page_title="Gestão de Internações", layout="wide")
st.title("🏥 Sistema de Importação e Consulta Hospitalar")

tabs = st.tabs([
    "📤 Importar Arquivo",
    "🔍 Consultar Internação",
    "📋 Procedimentos",
    "🧾 Profissionais",
    "💸 Convênios"
])


# ---------------------------------------------------------------------
# 📤 ABA 1 — IMPORTAR (apenas hospitais pré-definidos; sem entrada manual)
# ---------------------------------------------------------------------
with tabs[0]:
    st.header("📤 Importar arquivo CSV")

    hospitais = get_hospitais_ativos()
    if not hospitais:
        st.error("Nenhum hospital ativo no catálogo. Verifique a tabela Hospitals.")
    else:
        hospital = st.selectbox("Selecione o hospital:", hospitais, index=0)

    arquivo = st.file_uploader("Selecione o arquivo CSV")

    if arquivo and hospitais:
        registros = parse_csv(arquivo)
        st.success(f"{len(registros)} itens interpretados no arquivo!")

        with st.spinner("Gravando no banco..."):
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
                    internacao_id = existente[0]  # id da internação existente

                criar_procedimento(
                    internacao_id,
                    data,
                    prof,
                    proc
                )

        st.success("Importação concluída com sucesso! 🎉")


# ---------------------------------------------------------------------
# 🔍 ABA 2 — CONSULTAR INTERNAÇÃO (com filtro por hospital)
# ---------------------------------------------------------------------
with tabs[1]:
    st.header("🔍 Consultar Internação")

    hospitais = ["Todos"] + get_hospitais_ativos()
    filtro_hosp = st.selectbox("Filtrar por hospital:", hospitais, index=0)

    codigo = st.text_input("Digite o número do atendimento:")

    if codigo:
        conn = get_conn()

        if filtro_hosp == "Todos":
            df_int = pd.read_sql_query(
                "SELECT * FROM Internacoes WHERE atendimento = ?",
                conn, params=(codigo,)
            )
        else:
            df_int = pd.read_sql_query(
                "SELECT * FROM Internacoes WHERE atendimento = ? AND hospital = ?",
                conn, params=(codigo, filtro_hosp)
            )

        if df_int.empty:
            st.warning("Nenhuma internação encontrada.")
        else:
            st.subheader("Dados da internação")
            st.dataframe(df_int)

            internacao_id = int(df_int["id"].iloc[0])
            df_proc = pd.read_sql_query(
                "SELECT * FROM Procedimentos WHERE internacao_id = ?",
                conn, params=(internacao_id,)
            )

            st.subheader("Procedimentos registrados")
            st.dataframe(df_proc)

        conn.close()


# ---------------------------------------------------------------------
# 📋 ABA 3 — LISTA DE PROCEDIMENTOS (com filtro por hospital)
# ---------------------------------------------------------------------
with tabs[2]:
    st.header("📋 Lista de Procedimentos")

    hospitais = ["Todos"] + get_hospitais_ativos()
    filtro_hosp = st.selectbox("Filtrar por hospital:", hospitais, index=0, key="proc_hosp")

    if st.button("Carregar lista"):
        conn = get_conn()

        base_sql = """
            SELECT P.id, I.hospital, I.atendimento, I.paciente, P.data_procedimento,
                   P.profissional, P.procedimento, I.convenio
            FROM Procedimentos P
            INNER JOIN Internacoes I ON I.id = P.internacao_id
        """
        if filtro_hosp == "Todos":
            sql = base_sql + " ORDER BY P.data_procedimento DESC"
            df = pd.read_sql_query(sql, conn)
        else:
            sql = base_sql + " WHERE I.hospital = ? ORDER BY P.data_procedimento DESC"
            df = pd.read_sql_query(sql, conn, params=(filtro_hosp,))

        st.dataframe(df)
        conn.close()


# ---------------------------------------------------------------------
# 🧾 ABA 4 — RESUMO POR PROFISSIONAL (com filtro por hospital)
# ---------------------------------------------------------------------
with tabs[3]:
    st.header("🧾 Resumo por Profissional")

    hospitais = ["Todos"] + get_hospitais_ativos()
    filtro_hosp = st.selectbox("Filtrar por hospital:", hospitais, index=0, key="prof_hosp")

    conn = get_conn()
    base_sql = """
        SELECT P.profissional,
               COUNT(*) AS total_procedimentos
        FROM Procedimentos P
        INNER JOIN Internacoes I ON I.id = P.internacao_id
        WHERE P.profissional IS NOT NULL AND P.profissional <> ''
    """
    if filtro_hosp == "Todos":
        sql = base_sql + " GROUP BY P.profissional ORDER BY total_procedimentos DESC"
        df = pd.read_sql_query(sql, conn)
    else:
        sql = base_sql + " AND I.hospital = ? GROUP BY P.profissional ORDER BY total_procedimentos DESC"
        df = pd.read_sql_query(sql, conn, params=(filtro_hosp,))
    conn.close()

    st.dataframe(df)


# ---------------------------------------------------------------------
# 💸 ABA 5 — RESUMO POR CONVÊNIO (com filtro por hospital)
# ---------------------------------------------------------------------
with tabs[4]:
    st.header("💸 Resumo por Convênio")

    hospitais = ["Todos"] + get_hospitais_ativos()
    filtro_hosp = st.selectbox("Filtrar por hospital:", hospitais, index=0, key="conv_hosp")

    conn = get_conn()
    base_sql = """
        SELECT I.convenio,
               COUNT(*) AS total_procedimentos
        FROM Internacoes I
        INNER JOIN Procedimentos P ON P.internacao_id = I.id
        WHERE I.convenio IS NOT NULL AND I.convenio <> ''
    """
    if filtro_hosp == "Todos":
        sql = base_sql + " GROUP BY I.convenio ORDER BY total_procedimentos DESC"
        df = pd.read_sql_query(sql, conn)
    else:
        sql = base_sql + " AND I.hospital = ? GROUP BY I.convenio ORDER BY total_procedimentos DESC"
        df = pd.read_sql_query(sql, conn, params=(filtro_hosp,))
    conn.close()

    st.dataframe(df)
