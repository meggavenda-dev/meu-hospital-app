esse é o app.py completo original # ============================================================
#  SISTEMA DE INTERNAÇÕES — VERSÃO FINAL
#  Inclui:
#  - Parser robusto
#  - Dry-run antes de gravar
#  - Reprocessamento de atendimentos existentes
#  - Filtros por hospital
#  - Seeds de hospitais
# ============================================================

import streamlit as st
import sqlite3
import pandas as pd
import re

# ============================================================
# BANCO
# ============================================================

def get_conn():
    conn = sqlite3.connect("dados.db")
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn

def create_tables():
    conn = get_conn()
    cur = conn.cursor()

    # Catálogo de hospitais
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
        hospital TEXT,
        atendimento TEXT UNIQUE,
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

def tail(cols, n):
    pad = [""] * max(0, n - len(cols))
    return (pad + cols)[-n:]


def clean(s):
    return s.strip().strip('"').strip()


# ============================================================
# PARSER ROBUSTO — versão final com ancoragem pelas 5 últimas colunas
# ============================================================

def parse_csv_text(csv_text):
    internacoes = []
    data_atual = None
    atual = None
    hora_ini_mestre = ""
    hora_fim_mestre = ""

    for raw in csv_text.splitlines():
        linha = raw.replace("\x00", "").rstrip("\n")
        if not linha or linha.strip() == "":
            continue

        # DATA DO BLOCO
        if "Data de Realização" in linha:
            partes = [p.strip() for p in linha.split(",")]
            for p in partes:
                if re.fullmatch(r"\d{2}/\d{2}/\d{4}", p):
                    data_atual = p
            continue

        # LINHA MESTRE
        if re.match(r"^,\s*\d{7,12},", raw):
            cols = [c for c in raw.split(",")]

            # 5 últimas colunas (convênio, prestador, anestesista, tipo, quarto)
            conv, prest, anest, tipo, quarto = map(clean, tail(cols, 5))

            # cirurgia = 6ª coluna a partir do fim
            procedimento = clean(tail(cols, 6)[0])

            # horas
            hora_ini = clean(tail(cols, 8)[2])   # posição -8
            hora_fim = clean(tail(cols, 7)[1])   # posição -7

            # aviso
            aviso = clean(tail(cols, 9)[0])

            # lado esquerdo: atendimento + paciente
            esquerda = [c.strip() for c in cols]
            i = 0
            while i &lt; len(esquerda) and esquerda[i] == "":
                i += 1
            atendimento = esquerda[i] if i &lt; len(esquerda) else ""
            j = i + 1
            while j &lt; len(esquerda) and esquerda[j] == "":
                j += 1
            paciente = esquerda[j] if j &lt; len(esquerda) else ""

            hora_ini_mestre, hora_fim_mestre = hora_ini, hora_fim

            atual = {
                "data": data_atual or "",
                "atendimento": atendimento,
                "paciente": paciente,
                "hora_ini": hora_ini,
                "hora_fim": hora_fim,
                "procedimentos": []
            }

            atual["procedimentos"].append({
                "procedimento": procedimento,
                "convenio": conv,
                "profissional": prest,
                "anestesista": anest,
                "tipo": tipo,
                "quarto": quarto,
                "hora_ini": hora_ini,
                "hora_fim": hora_fim
            })

            internacoes.append(atual)
            continue

        # LINHA FILHA (procedimento extra)
        if re.match(r"^,{10,}", raw):
            cols = [c for c in raw.split(",")]

            conv, prest, anest, tipo, quarto = map(clean, tail(cols, 5))
            procedimento = clean(tail(cols, 6)[0])

            if atual:
                atual["procedimentos"].append({
                    "procedimento": procedimento,
                    "convenio": conv,
                    "profissional": prest,
                    "anestesista": anest,
                    "tipo": tipo,
                    "quarto": quarto,
                    "hora_ini": hora_ini_mestre,
                    "hora_fim": hora_fim_mestre
                })
            continue

        # Totais ignorados
        if "Total de Avisos" in linha or "Total de Cirurgias" in linha:
            continue

        # Demais linhas ignoradas
        continue

    # FLAT: um registro por procedimento
    registros = []
    for it in internacoes:
        for p in it["procedimentos"]:
            registros.append({
                "atendimento": it["atendimento"],
                "paciente": it["paciente"],
                "data": it["data"],
                "procedimento": p["procedimento"],
                "convenio": p["convenio"],
                "profissional": p["profissional"],
                "anestesista": p["anestesista"],
                "tipo": p["tipo"],
                "quarto": p["quarto"],
                "hora_ini": p["hora_ini"],
                "hora_fim": p["hora_fim"]
            })
    return registros


# ============================================================
# FUNÇÕES DE BANCO
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


def criar_internacao(hospital, atendimento, paciente, data, convenio):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO Internacoes (numero_internacao, hospital, atendimento, paciente, data_internacao, convenio)
        VALUES (?, ?, ?, ?, ?, ?)
    """, (float(atendimento), hospital, atendimento, paciente, data, convenio))
    conn.commit()
    nid = cur.lastrowid
    conn.close()
    return nid


def criar_procedimento(internacao_id, data_proc, profissional, procedimento):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO Procedimentos (internacao_id, data_procedimento, profissional, procedimento)
        VALUES (?, ?, ?, ?)
    """, (internacao_id, data_proc, profissional, procedimento))
    conn.commit()
    conn.close()


def get_internacao_by_atendimento(att):
    conn = get_conn()
    df = pd.read_sql_query("SELECT * FROM Internacoes WHERE atendimento = ?", conn, params=(att,))
    conn.close()
    return df


def get_procedimentos(internacao_id):
    conn = get_conn()
    df = pd.read_sql_query("SELECT * FROM Procedimentos WHERE internacao_id = ?", conn, params=(internacao_id,))
    conn.close()
    return df


# ============================================================
# INICIALIZAÇÃO
# ============================================================

create_tables()
seed_hospitais()

st.set_page_config("Gestão de Internações", layout="wide")
st.title("🏥 Sistema de Internações — Versão Final")


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
        csv_text = arquivo.getvalue().decode("latin1", errors="ignore")
        registros = parse_csv_text(csv_text)

        st.success(f"{len(registros)} registros interpretados!")

        df_preview = pd.DataFrame(registros)
        st.subheader("Pré-visualização (DRY RUN) — nada foi gravado ainda")
        st.dataframe(df_preview)

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

            # Inserção
            for att, info in agrupado.items():
                paciente = info["paciente"]
                data = info["data"]
                conv_total = info["procedimentos"][0]["convenio"]

                internacao_id = criar_internacao(
                    hospital,
                    att,
                    paciente,
                    data,
                    conv_total
                )

                for p in info["procedimentos"]:
                    criar_procedimento(
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
        df_int = get_internacao_by_atendimento(codigo)
        if filtro_hosp != "Todos":
            df_int = df_int[df_int["hospital"] == filtro_hosp]

        if df_int.empty:
            st.warning("Nenhuma internação encontrada.")
        else:
            st.subheader("Dados da internação")
            st.dataframe(df_int)

            internacao_id = df_int["id"].iloc[0]
            df_proc = get_procedimentos(internacao_id)

            st.subheader("Procedimentos registrados")
            st.dataframe(df_proc)


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
        st.dataframe(df)


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
        WHERE profissional IS NOT NULL AND profissional &lt;&gt; ''
    """

    if chosen == "Todos":
        sql = base + " GROUP BY profissional ORDER BY total DESC"
        df = pd.read_sql_query(sql, conn)
    else:
        sql = base + " AND I.hospital = ? GROUP BY profissional ORDER BY total DESC"
        df = pd.read_sql_query(sql, conn, params=(chosen,))
    conn.close()

    st.dataframe(df)


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
        WHERE I.convenio IS NOT NULL AND I.convenio &lt;&gt; ''
    """

    if chosen == "Todos":
        sql = base + " GROUP BY I.convenio ORDER BY total DESC"
        df = pd.read_sql_query(sql, conn)
    else:
        sql = base + " AND I.hospital = ? GROUP BY I.convenio ORDER BY total DESC"
        df = pd.read_sql_query(sql, conn, params=(chosen,))
    conn.close()

    st.dataframe(df)
