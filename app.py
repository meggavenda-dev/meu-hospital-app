
# ============================================================
#  SISTEMA DE INTERNAÇÕES — VERSÃO FINAL
#  Inclui:
#  - Parser robusto
#  - Dry-run antes de gravar
#  - 1 procedimento-do-dia automático por (internação, data)
#  - Edição de situação/observação/tipo de procedimento
#  - Lançamento manual (permite mais de um no mesmo dia)
#  - Filtros por hospital
#  - Seeds de hospitais
# ============================================================

import streamlit as st
import sqlite3
import pandas as pd
import re
from datetime import date

# Parser robusto do seu módulo
from parser import parse_tiss_original

STATUS_OPCOES = [
    "Pendente",
    "Não Cobrar",
    "Enviado para pagamento",
    "Aguardando Digitação - AMHP",
    "Finalizado",
]
PROCEDIMENTO_OPCOES = ["Cirurgia / Procedimento", "Parecer"]

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

    # >>> RECRIAR Procedimentos para suportar manual (múltiplos no dia)
    # Remove a tabela se existir (você informou que não tem dados)
    cur.execute("DROP TABLE IF EXISTS Procedimentos;")
    cur.execute("""
    CREATE TABLE Procedimentos (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        internacao_id INTEGER,
        data_procedimento TEXT,
        profissional TEXT,
        procedimento TEXT,
        situacao TEXT NOT NULL DEFAULT 'Pendente',
        observacao TEXT,
        is_manual INTEGER NOT NULL DEFAULT 0,  -- 0 = automático (import), 1 = manual
        FOREIGN KEY(internacao_id) REFERENCES Internacoes(id)
    );
    """)
    # Índice ÚNICO parcial: impede duplicar AUTOMÁTICO no mesmo (internação, data)
    cur.execute("""
    CREATE UNIQUE INDEX IF NOT EXISTS ux_proc_auto
      ON Procedimentos(internacao_id, data_procedimento)
      WHERE is_manual = 0;
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
# (LEGADO) PARSER AUXILIAR — mantido para referência (NÃO USADO)
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

        if "Data de Realização" in linha:
            partes = [p.strip() for p in linha.split(",")]
            for p in partes:
                if re.fullmatch(r"\d{2}/\d{2}/\d{4}", p):
                    data_atual = p
            continue

        if re.match(r"^,\s*\d{7,12},", raw):
            cols = [c for c in raw.split(",")]
            conv, prest, anest, tipo, quarto = map(clean, tail(cols, 5))
            procedimento = clean(tail(cols, 6)[0])
            hora_ini = clean(tail(cols, 8)[2])
            hora_fim = clean(tail(cols, 7)[1])
            aviso = clean(tail(cols, 9)[0])
            esquerda = [c.strip() for c in cols]
            i = 0
            while i < len(esquerda) and esquerda[i] == "":
                i += 1
            atendimento = esquerda[i] if i < len(esquerda) else ""
            j = i + 1
            while j < len(esquerda) and esquerda[j] == "":
                j += 1
            paciente = esquerda[j] if j < len(esquerda) else ""
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

        if "Total de Avisos" in linha or "Total de Cirurgias" in linha:
            continue

        continue

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
    cur.execute(f"""
        DELETE FROM Procedimentos
         WHERE internacao_id IN (
             SELECT id FROM Internacoes
              WHERE atendimento IN ({qmarks})
         )
    """, lista_at)
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

# Aceita situacao/observacao/is_manual. OR IGNORE por conta do UNIQUE parcial nos automáticos.
def criar_procedimento(internacao_id, data_proc, profissional, procedimento,
                       situacao="Pendente", observacao=None, is_manual=0):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        INSERT OR IGNORE INTO Procedimentos
        (internacao_id, data_procedimento, profissional, procedimento, situacao, observacao, is_manual)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, (internacao_id, data_proc, profissional, procedimento, situacao, observacao, is_manual))
    conn.commit()
    conn.close()

# Para o import: só impede duplicar AUTOMÁTICO
def existe_procedimento_no_dia(internacao_id, data_proc):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT 1 FROM Procedimentos
        WHERE internacao_id = ? AND data_procedimento = ? AND is_manual = 0
        LIMIT 1
    """, (internacao_id, data_proc))
    ok = cur.fetchone() is not None
    conn.close()
    return ok

def atualizar_procedimento(proc_id, procedimento=None, situacao=None, observacao=None):
    sets, params = [], []
    if procedimento is not None:
        sets.append("procedimento = ?"); params.append(procedimento)
    if situacao is not None:
        sets.append("situacao = ?"); params.append(situacao)
    if observacao is not None:
        sets.append("observacao = ?"); params.append(observacao)
    if not sets:
        return
    params.append(proc_id)
    sql = f"UPDATE Procedimentos SET {', '.join(sets)} WHERE id = ?"
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(sql, params)
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

st.set_page_config(page_title="Gestão de Internações", layout="wide")
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
# 📤 ABA 1 — IMPORTAR COM DRY RUN (1 auto por dia; manual pode ter vários)
# ============================================================

with tabs[0]:
    st.header("📤 Importar arquivo")

    hospitais = get_hospitais()
    hospital = st.selectbox("Hospital:", hospitais)

    arquivo = st.file_uploader("Selecione o arquivo CSV")

    if arquivo:
        raw_bytes = arquivo.getvalue()
        try:
            csv_text = raw_bytes.decode("latin1")
        except UnicodeDecodeError:
            csv_text = raw_bytes.decode("utf-8-sig", errors="ignore")

        registros = parse_tiss_original(csv_text)

        st.success(f"{len(registros)} registros interpretados!")
        df_preview = pd.DataFrame(registros)
        st.subheader("Pré-visualização (DRY RUN) — nada foi gravado ainda")
        st.dataframe(df_preview, use_container_width=True)

        pares = sorted({(r["atendimento"], r["data"]) for r in registros if r.get("atendimento") and r.get("data")})
        st.info(f"O arquivo contém {len(pares)} par(es) (atendimento, data). "
                "Regra: 1 procedimento AUTOMÁTICO por internação/dia (manuais podem ser vários).")

        if st.button("Gravar no banco"):
            total_criados, total_ignorados, total_internacoes = 0, 0, 0

            for (att, data_proc) in pares:
                if not att:
                    continue

                # Internação: get or create
                df_int = get_internacao_by_atendimento(att)
                if df_int.empty:
                    itens_att = [r for r in registros if r["atendimento"] == att]
                    paciente = next((x.get("paciente") for x in itens_att if x.get("paciente")), "") if itens_att else ""
                    conv_total = next((x.get("convenio") for x in itens_att if x.get("convenio")), "") if itens_att else ""
                    data_int = next((x.get("data") for x in itens_att if x.get("data")), data_proc)

                    internacao_id = criar_internacao(
                        hospital,
                        att,
                        paciente,
                        data_int,
                        conv_total
                    )
                    total_internacoes += 1
                else:
                    internacao_id = int(df_int["id"].iloc[0])

                # Profissional do dia = primeiro que surgir para (att, data)
                prof_dia = ""
                for it in registros:
                    if it["atendimento"] == att and it["data"] == data_proc and it.get("profissional"):
                        prof_dia = it["profissional"]
                        break

                # Se já existir proc-do-dia AUTOMÁTICO => ignorar
                if existe_procedimento_no_dia(internacao_id, data_proc):
                    total_ignorados += 1
                    continue

                # Criar 1 (um) procedimento-do-dia AUTOMÁTICO
                criar_procedimento(
                    internacao_id,
                    data_proc,
                    prof_dia,
                    procedimento="Cirurgia / Procedimento",
                    situacao="Pendente",
                    observacao=None,
                    is_manual=0
                )
                total_criados += 1

            st.success(f"Concluído! Internações criadas: {total_internacoes} | "
                       f"Automáticos criados: {total_criados} | Ignorados (auto já existia): {total_ignorados}")


# ============================================================
# 🔍 ABA 2 — CONSULTAR (edição + manual pode lançar vários no dia)
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
            st.dataframe(df_int, use_container_width=True)

            internacao_id = int(df_int["id"].iloc[0])

            conn = get_conn()
            df_proc = pd.read_sql_query(
                "SELECT id, data_procedimento, profissional, procedimento, situacao, observacao "
                "FROM Procedimentos WHERE internacao_id = ? ORDER BY data_procedimento, id",
                conn, params=(internacao_id,)
            )
            conn.close()

            if "procedimento" not in df_proc.columns:
                df_proc["procedimento"] = "Cirurgia / Procedimento"
            df_proc["procedimento"] = df_proc["procedimento"].fillna("Cirurgia / Procedimento")
            df_proc["situacao"] = df_proc.get("situacao", pd.Series(dtype=str)).fillna("Pendente")
            df_proc["observacao"] = df_proc.get("observacao", pd.Series(dtype=str)).fillna("")

            st.subheader("Procedimentos — Editáveis")
            edited = st.data_editor(
                df_proc,
                key="editor_proc",
                use_container_width=True,
                hide_index=True,
                column_config={
                    "id": st.column_config.Column("ID", disabled=True),
                    "data_procedimento": st.column_config.Column("Data", disabled=True),
                    "profissional": st.column_config.Column("Profissional", disabled=True),
                    "procedimento": st.column_config.SelectboxColumn(
                        "Tipo de Procedimento", options=PROCEDIMENTO_OPCOES, required=True
                    ),
                    "situacao": st.column_config.SelectboxColumn(
                        "Situação", options=STATUS_OPCOES, required=True
                    ),
                    "observacao": st.column_config.TextColumn(
                        "Observações", help="Texto livre"
                    ),
                },
            )

            if st.button("💾 Salvar alterações"):
                cols_chk = ["procedimento", "situacao", "observacao"]
                df_compare = df_proc[["id"] + cols_chk].merge(
                    edited[["id"] + cols_chk], on="id", suffixes=("_old", "_new")
                )
                alterados = []
                for _, row in df_compare.iterrows():
                    changed = any(
                        (str(row[c + "_old"] or "") != str(row[c + "_new"] or ""))
                        for c in cols_chk
                    )
                    if changed:
                        alterados.append({
                            "id": int(row["id"]),
                            "procedimento": row["procedimento_new"],
                            "situacao": row["situacao_new"],
                            "observacao": row["observacao_new"],
                        })
                if not alterados:
                    st.info("Nenhuma alteração detectada.")
                else:
                    for item in alterados:
                        atualizar_procedimento(
                            proc_id=item["id"],
                            procedimento=item["procedimento"],
                            situacao=item["situacao"],
                            observacao=item["observacao"],
                        )
                    st.success(f"{len(alterados)} procedimento(s) atualizado(s).")

            st.divider()
            st.subheader("➕ Lançar procedimento manual (permite mais de um no mesmo dia)")

            c1, c2, c3 = st.columns(3)
            with c1:
                data_proc = st.date_input("Data do procedimento", value=date.today())
            with c2:
                profissional = st.text_input("Profissional (opcional)")
            with c3:
                situacao = st.selectbox("Situação", STATUS_OPCOES, index=0)

            colp1, colp2 = st.columns(2)
            with colp1:
                procedimento_tipo = st.selectbox("Tipo de Procedimento", PROCEDIMENTO_OPCOES, index=0)
            with colp2:
                observacao = st.text_input("Observações (opcional)")

            if st.button("Adicionar procedimento"):
                data_str = data_proc.strftime("%d/%m/%Y")
                # AGORA: manual pode ter vários no mesmo dia (não checamos existência)
                criar_procedimento(
                    internacao_id,
                    data_str,
                    profissional,
                    procedimento_tipo,
                    situacao=situacao,
                    observacao=(observacao or None),
                    is_manual=1
                )
                st.success("Procedimento (manual) adicionado.")


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
                   P.data_procedimento, P.profissional, P.procedimento,
                   P.situacao, P.observacao
            FROM Procedimentos P
            INNER JOIN Internacoes I ON I.id = P.internacao_id
        """

        if chosen == "Todos":
            sql = base + " ORDER BY P.data_procedimento DESC, P.id DESC"
            df = pd.read_sql_query(sql, conn)
        else:
            sql = base + " WHERE I.hospital = ? ORDER BY P.data_procedimento DESC, P.id DESC"
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
