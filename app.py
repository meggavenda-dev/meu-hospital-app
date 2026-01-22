
# ============================================================
#  SISTEMA DE INTERNAÇÕES — VERSÃO FINAL (modular como você mandou)
#  - Usa db.py (create_tables, criar_internacao, etc.)
#  - Usa parser_tiss.py (parse_tiss_original)
#  - Hospitais (tabela + seed) permanecem no app, como no seu código
#  - Dry-run + reprocessamento
# ============================================================

import streamlit as st
import sqlite3
import pandas as pd
import re

# Módulos próprios (iguais aos que você enviou)
from db import (
    create_tables as create_core_tables,
    get_internacao_by_atendimento as db_get_internacao_by_atendimento,
    criar_internacao as db_criar_internacao,
    criar_procedimento as db_criar_procedimento,
)
from parser_tiss import parse_tiss_original

# ============================================================
# BANCO (apenas catálogo de hospitais no app, como no seu arquivo)
# ============================================================

def get_conn():
    conn = sqlite3.connect("dados.db")
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn

def create_hospitals_table():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS Hospitals (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT UNIQUE NOT NULL,
        active INTEGER NOT NULL DEFAULT 1
    );
    """)
    conn.commit()
    conn.close()

def seed_hospitais():
    H = [
        "Santa Lucia Sul",
        "Santa Lucia Norte",
        "Maria Auxiliadora",
        "Santa Lucia Taguatinga",
        "Santa Lucia Águas Claras",
        "Santa Lucia Sudoeste"
    ]
    conn = get_conn()
    cur = conn.cursor()
    for nome in H:
        cur.execute("INSERT OR IGNORE INTO Hospitals (name, active) VALUES (?,1)", (nome,))
    conn.commit()
    conn.close()

def get_hospitais():
    conn = get_conn()
    df = pd.read_sql_query("SELECT name FROM Hospitals WHERE active = 1 ORDER BY name", conn)
    conn.close()
    return df["name"].tolist()

# ============================================================
# UTIL
# ============================================================

def clean(s):
    return s.strip().strip('"').strip()

# ============================================================
# FUNÇÕES DE BANCO (auxiliares locais para DataFrames)
# ============================================================

def apagar_internacoes(lista_at):
    if not lista_at:
        return
    conn = get_conn()
    cur = conn.cursor()

    qmarks = ",".join(["?"] * len(lista_at))
    # Apaga procedimentos
    cur.execute(f"""
        DELETE FROM Procedimentos
         WHERE internacao_id IN (
             SELECT id FROM Internacoes
              WHERE atendimento IN ({qmarks})
         )
    """, lista_at)
    # Apaga internações
    cur.execute(f"DELETE FROM Internacoes WHERE atendimento IN ({qmarks})", lista_at)

    conn.commit()
    conn.close()

def get_internacao_df_by_atendimento(att):
    conn = get_conn()
    df = pd.read_sql_query("SELECT * FROM Internacoes WHERE atendimento = ?", conn, params=(att,))
    conn.close()
    return df

def get_procedimentos_df(internacao_id):
    conn = get_conn()
    df = pd.read_sql_query("SELECT * FROM Procedimentos WHERE internacao_id = ?", conn, params=(internacao_id,))
    conn.close()
    return df

# ============================================================
# INICIALIZAÇÃO
# ============================================================

# Cria tabelas núcleo (Internacoes/Procedimentos) via módulo db.py
create_core_tables()
# Cria/seed de Hospitals aqui no app (como no seu arquivo grande)
create_hospitals_table()
seed_hospitais()

st.set_page_config("Gestão de Internações", layout="wide")
st.title("🏥 Sistema de Internações — Versão Final (modular)")

# ============================================================
# INTERFACE EM ABAS
# ============================================================

tabs = st.tabs([
    "📤 Importar Arquivo",
    "🔍 Consultar Internação",
    "📋 Procedimentos",
    "🧾 Profissionais",
    "💸 Convênios"
])

# ============================================================
# 📤 ABA 1 — IMPORTAR COM DRY RUN
# ============================================================

with tabs[0]:
    st.header("📤 Importar arquivo")

    hospitais = get_hospitais()
    hospital = st.selectbox("Hospital:", hospitais)

    arquivo = st.file_uploader("Selecione o arquivo CSV")

    if arquivo:
        # Mantive a decodificação igual você já usa
        csv_text = arquivo.getvalue().decode("latin1", errors="ignore")

        # Aqui, usamos o parser do módulo exatamente como você enviou
        registros = parse_tiss_original(csv_text)

        st.success(f"{len(registros)} registros interpretados!")

        df_preview = pd.DataFrame(registros)
        st.subheader("Pré-visualização (DRY RUN) — nada foi gravado ainda")
        st.dataframe(df_preview, use_container_width=True)

        lista_at = sorted(set(df_preview["atendimento"].tolist()))
        st.info(f"O sistema reprocessará {len(lista_at)} atendimentos.")

        if st.button("Gravar no banco"):
            apagar_internacoes(lista_at)

            # Agrupar por atendimento
            agrupado = {}
            for r in registros:
                att = r["atendimento"]
                if att not in agrupado:
                    agrupado[att] = {
                        "paciente": r["paciente"],
                        "data": r["data"],
                        "procedimentos": []
                    }
                agrupado[att]["procedimentos"].append(r)

            # Inserção (mantendo sua lógica original: numero_internacao = float(atendimento))
            for att, info in agrupado.items():
                paciente = info["paciente"]
                data = info["data"]
                conv_total = info["procedimentos"][0]["convenio"] if info["procedimentos"] else ""

                internacao_id = db_criar_internacao(
                    float(att) if att else None,
                    hospital,
                    att,
                    paciente,
                    data,
                    conv_total
                )

                for p in info["procedimentos"]:
                    db_criar_procedimento(
                        internacao_id,
                        p["data"],
                        p["profissional"],
                        p["procedimento"]
                    )

            st.success("Importação concluída com sucesso!")

# ============================================================
# 🔍 ABA 2 — CONSULTAR
# ============================================================

with tabs[1]:
    st.header("🔍 Consultar Internação")

    hlist = ["Todos"] + get_hospitais()
    filtro_hosp = st.selectbox("Filtrar hospital:", hlist)

    codigo = st.text_input("Digite o atendimento:")

    if codigo:
        df_int = get_internacao_df_by_atendimento(codigo)
        if filtro_hosp != "Todos":
            df_int = df_int[df_int["hospital"] == filtro_hosp]

        if df_int.empty:
            st.warning("Nenhuma internação encontrada.")
        else:
            st.subheader("Dados da internação")
            st.dataframe(df_int, use_container_width=True)

            internacao_id = df_int["id"].iloc[0]
            df_proc = get_procedimentos_df(internacao_id)

            st.subheader("Procedimentos registrados")
            st.dataframe(df_proc, use_container_width=True)

# ============================================================
# 📋 ABA 3 — LISTA PROCEDIMENTOS
# ============================================================

with tabs[2]:
    st.header("📋 Todos os procedimentos")

    filtro = ["Todos"] + get_hospitais()
    chosen = st.selectbox("Hospital:", filtro)

    if st.button("Carregar procedimentos"):
        conn = get_conn()
        base = """
            SELECT P.id, I.hospital, I.atendimento, I.paciente,
                   P.data_procedimento, P.profissional, P.procedimento
            FROM Procedimentos P
            INNER JOIN Internacoes I ON I.id = P.internacao_id
        """

        if chosen == "Todos":
            sql = base + " ORDER BY P.data_procedimento DESC"
            df = pd.read_sql_query(sql, conn)
        else:
            sql = base + " WHERE I.hospital = ? ORDER BY P.data_procedimento DESC"
            df = pd.read_sql_query(sql, conn, params=(chosen,))

        conn.close()
        st.dataframe(df, use_container_width=True)

# ============================================================
# 🧾 ABA 4 — RESUMO POR PROFISSIONAL
# ============================================================

with tabs[3]:
    st.header("🧾 Resumo por Profissional")

    filtro = ["Todos"] + get_hospitais()
    chosen = st.selectbox("Hospital:", filtro, key="prof_h")

    conn = get_conn()
    base = """
        SELECT profissional, COUNT(*) AS total
        FROM Procedimentos P
        INNER JOIN Internacoes I ON I.id = P.internacao_id
        WHERE profissional IS NOT NULL AND profissional <> ''
    """

    if chosen == "Todos":
        sql = base + " GROUP BY profissional ORDER BY total DESC"
        df = pd.read_sql_query(sql, conn)
    else:
        sql = base + " AND I.hospital = ? GROUP BY profissional ORDER BY total DESC"
        df = pd.read_sql_query(sql, conn, params=(chosen,))
    conn.close()

    st.dataframe(df, use_container_width=True)

# ============================================================
# 💸 ABA 5 — RESUMO POR CONVÊNIO
# ============================================================

with tabs[4]:
    st.header("💸 Resumo por Convênio")

    filtro = ["Todos"] + get_hospitais()
    chosen = st.selectbox("Hospital:", filtro, key="conv_h")

    conn = get_conn()
    base = """
        SELECT I.convenio, COUNT(*) AS total
        FROM Internacoes I
        INNER JOIN Procedimentos P ON P.internacao_id = I.id
        WHERE I.convenio IS NOT NULL AND I.convenio <> ''
    """

    if chosen == "Todos":
        sql = base + " GROUP BY I.convenio ORDER BY total DESC"
        df = pd.read_sql_query(sql, conn)
    else:
        sql = base + " AND I.hospital = ? GROUP BY I.convenio ORDER BY total DESC"
        df = pd.read_sql_query(sql, conn, params=(chosen,))
    conn.close()

    st.dataframe(df, use_container_width=True)
