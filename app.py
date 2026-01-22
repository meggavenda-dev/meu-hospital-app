
# ============================================================
#  SISTEMA DE INTERNAÇÕES — VERSÃO FINAL
#  Inclui:
#  - Parser robusto
#  - Dry-run antes de gravar
#  - 1 procedimento AUTOMÁTICO por (internação, data)
#  - Lançamento manual (permite >1 no mesmo dia)
#  - Edição de procedimento (tipo/situação/observações/grau participação)
#  - Filtros por hospital + Seeds
#  - Relatórios (PDF) — Cirurgias por Status (paisagem, ordem: Atendimento, Aviso, Convênio, Paciente, Data, Profissional, Hospital)
#  - Quitação de Cirurgias (com atualização automática para "Finalizado")
#  - Ver quitação em cirurgias Finalizadas (botão)
#  - ⚙️ Sistema: agrega Procedimentos, Resumo por Profissional e por Convênio
#  - NOVO: Grau de participação + Excluir internação + Excluir cirurgia + Cadastrar internação manualmente
# ============================================================

import streamlit as st
import sqlite3
import pandas as pd
import re
from datetime import date, datetime
import io

# ==== PDF (ReportLab) - import protegido ====
REPORTLAB_OK = True
try:
    from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer
    from reportlab.lib.pagesizes import A4, landscape  # paisagem
    from reportlab.lib import colors
    from reportlab.lib.styles import getSampleStyleSheet
except ModuleNotFoundError:
    REPORTLAB_OK = False

# >>> Parser robusto do seu módulo parser.py (precisa retornar 'aviso' nos registros)
from parser import parse_tiss_original

# Opções de domínio
STATUS_OPCOES = [
    "Pendente",
    "Não Cobrar",
    "Enviado para pagamento",
    "Aguardando Digitação - AMHP",
    "Finalizado",
]
PROCEDIMENTO_OPCOES = ["Cirurgia / Procedimento", "Parecer"]
GRAU_PARTICIPACAO_OPCOES = ["Cirurgião", "1 Auxiliar", "2 Auxiliar", "3 Auxiliar", "Clínico"]

# ============================================================
# BANCO
# ============================================================

def get_conn():
    conn = sqlite3.connect("dados.db")
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn

def create_tables():
    """
    Cria/migra tabelas sem apagar dados (sem DROP).
    - Adiciona colunas faltantes em Procedimentos (situacao, observacao, is_manual, aviso, grau_participacao).
    - Adiciona colunas de quitação (quitacao_*).
    - Cria índice único parcial ux_proc_auto para evitar duplicata AUTOMÁTICA por dia.
    """
    conn = get_conn()
    cur = conn.cursor()

    # Hospitals
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

    # Procedimentos (se não existir, cria já com as colunas novas)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS Procedimentos (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        internacao_id INTEGER,
        data_procedimento TEXT,
        profissional TEXT,
        procedimento TEXT,
        situacao TEXT NOT NULL DEFAULT 'Pendente',
        observacao TEXT,
        is_manual INTEGER NOT NULL DEFAULT 0,  -- 0=automático(import), 1=manual
        aviso TEXT,
        grau_participacao TEXT,                -- NOVO
        -- Campos de quitação
        quitacao_data TEXT,
        quitacao_guia_amhptiss TEXT,
        quitacao_valor_amhptiss REAL,
        quitacao_guia_complemento TEXT,
        quitacao_valor_complemento REAL,
        FOREIGN KEY(internacao_id) REFERENCES Internacoes(id)
    );
    """)

    # Migração incremental (se a tabela já existia sem as colunas)
    for alter in [
        "ALTER TABLE Procedimentos ADD COLUMN situacao TEXT NOT NULL DEFAULT 'Pendente';",
        "ALTER TABLE Procedimentos ADD COLUMN observacao TEXT;",
        "ALTER TABLE Procedimentos ADD COLUMN is_manual INTEGER NOT NULL DEFAULT 0;",
        "ALTER TABLE Procedimentos ADD COLUMN aviso TEXT;",
        "ALTER TABLE Procedimentos ADD COLUMN grau_participacao TEXT;",
        "ALTER TABLE Procedimentos ADD COLUMN quitacao_data TEXT;",
        "ALTER TABLE Procedimentos ADD COLUMN quitacao_guia_amhptiss TEXT;",
        "ALTER TABLE Procedimentos ADD COLUMN quitacao_valor_amhptiss REAL;",
        "ALTER TABLE Procedimentos ADD COLUMN quitacao_guia_complemento TEXT;",
        "ALTER TABLE Procedimentos ADD COLUMN quitacao_valor_complemento REAL;",
    ]:
        try:
            cur.execute(alter)
        except sqlite3.OperationalError:
            pass

    # Índice ÚNICO parcial: evita duplicar AUTOMÁTICO no mesmo (internacao_id, data)
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

def _pt_date_to_dt(s):
    """Converte 'dd/mm/AAAA' -> datetime.date; retorna None se inválido."""
    try:
        return datetime.strptime(s, "%d/%m/%Y").date()
    except Exception:
        return None

def _to_ddmmyyyy(value):
    """Converte pandas.Timestamp/date/str em 'dd/mm/AAAA' (ou retorna '')."""
    if value is None or value == "":
        return ""
    if isinstance(value, pd.Timestamp):
        return value.strftime("%d/%m/%Y")
    if isinstance(value, datetime):
        return value.strftime("%d/%m/%Y")
    if isinstance(value, date):
        return value.strftime("%d/%m/%Y")
    # string já no formato?
    try:
        dt = datetime.strptime(str(value), "%Y-%m-%d")
        return dt.strftime("%d/%m/%Y")
    except Exception:
        pass
    try:
        dt = datetime.strptime(str(value), "%d/%m/%Y")
        return dt.strftime("%d/%m/%Y")
    except Exception:
        return str(value)

def _to_float_or_none(v):
    if v is None or v == "":
        return None
    try:
        # aceita vírgula decimal
        return float(str(v).replace(".", "").replace(",", "."))
    except Exception:
        try:
            return float(v)
        except Exception:
            return None

def _format_currency_br(v) -> str:
    """Formata número em BRL simples sem depender de locale do SO."""
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return "R$ 0,00"
    try:
        v = float(v)
        s = f"{v:,.2f}"
        # USA -> BR: separador milhar ponto, decimal vírgula
        s = s.replace(",", "X").replace(".", ",").replace("X", ".")
        return f"R$ {s}"
    except Exception:
        return f"R$ {v}"

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
            procedimento = clean(tail(cols, 6)[0])
            hora_ini = clean(tail(cols, 8)[2])
            hora_fim = clean(tail(cols, 7)[1])

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

        # LINHA FILHA
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

def deletar_internacao(internacao_id: int):
    """Exclui procedimentos da internação e depois a própria internação."""
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("DELETE FROM Procedimentos WHERE internacao_id = ?", (internacao_id,))
    cur.execute("DELETE FROM Internacoes WHERE id = ?", (internacao_id,))
    conn.commit()
    conn.close()

def deletar_procedimento(proc_id: int):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("DELETE FROM Procedimentos WHERE id = ?", (proc_id,))
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

# Aceita situacao/observacao/is_manual/aviso/grau_participacao; OR IGNORE por causa do UNIQUE parcial nos automáticos
def criar_procedimento(internacao_id, data_proc, profissional, procedimento,
                       situacao="Pendente", observacao=None, is_manual=0, aviso=None, grau_participacao=None):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        INSERT OR IGNORE INTO Procedimentos
        (internacao_id, data_procedimento, profissional, procedimento, situacao, observacao, is_manual, aviso, grau_participacao)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (internacao_id, data_proc, profissional, procedimento, situacao, observacao, is_manual, aviso, grau_participacao))
    conn.commit()
    conn.close()

# Import automático só verifica duplicata automática (is_manual=0)
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

# Atualiza campos editáveis
def atualizar_procedimento(proc_id, procedimento=None, situacao=None, observacao=None, grau_participacao=None):
    sets, params = [], []
    if procedimento is not None:
        sets.append("procedimento = ?"); params.append(procedimento)
    if situacao is not None:
        sets.append("situacao = ?"); params.append(situacao)
    if observacao is not None:
        sets.append("observacao = ?"); params.append(observacao)
    if grau_participacao is not None:
        sets.append("grau_participacao = ?"); params.append(grau_participacao)
    if not sets:
        return
    params.append(proc_id)
    sql = f"UPDATE Procedimentos SET {', '.join(sets)} WHERE id = ?"
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(sql, params)
    conn.commit()
    conn.close()

def quitar_procedimento(proc_id, data_quitacao=None, guia_amhptiss=None, valor_amhptiss=None,
                        guia_complemento=None, valor_complemento=None):
    """Grava quitação e atualiza situação para 'Finalizado'."""
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        UPDATE Procedimentos
           SET quitacao_data = ?,
               quitacao_guia_amhptiss = ?,
               quitacao_valor_amhptiss = ?,
               quitacao_guia_complemento = ?,
               quitacao_valor_complemento = ?,
               situacao = 'Finalizado'
         WHERE id = ?
    """, (data_quitacao, guia_amhptiss, valor_amhptiss, guia_complemento, valor_complemento, proc_id))
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

def get_quitacao_by_proc_id(proc_id: int):
    """Busca dados completos da quitação e metadados da cirurgia."""
    conn = get_conn()
    sql = """
        SELECT
            P.id, P.data_procedimento, P.profissional, P.situacao, P.aviso, P.observacao, P.grau_participacao,
            P.quitacao_data, P.quitacao_guia_amhptiss, P.quitacao_valor_amhptiss,
            P.quitacao_guia_complemento, P.quitacao_valor_complemento,
            I.hospital, I.atendimento, I.paciente, I.convenio
        FROM Procedimentos P
        INNER JOIN Internacoes I ON I.id = P.internacao_id
        WHERE P.id = ?
    """
    df = pd.read_sql_query(sql, conn, params=(proc_id,))
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
    "📑 Relatórios",
    "💼 Quitação",
    "⚙️ Sistema",
])


# ============================================================
# 📤 ABA 1 — IMPORTAR (1 AUTOMÁTICO por dia; manuais podem repetir)
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

                # Profissional e Aviso do dia = primeiros que surgirem para (att, data)
                prof_dia = ""
                aviso_dia = ""
                for it in registros:
                    if it["atendimento"] == att and it["data"] == data_proc:
                        if not prof_dia and it.get("profissional"):
                            prof_dia = it["profissional"]
                        if not aviso_dia and it.get("aviso"):
                            aviso_dia = it["aviso"]
                        if prof_dia and aviso_dia:
                            break

                # Já existe auto do dia?
                if existe_procedimento_no_dia(internacao_id, data_proc):
                    total_ignorados += 1
                    continue

                # Criar 1 (um) procedimento-do-dia AUTOMÁTICO (grau_participacao vazio)
                criar_procedimento(
                    internacao_id,
                    data_proc,
                    prof_dia,
                    procedimento="Cirurgia / Procedimento",
                    situacao="Pendente",
                    observacao=None,
                    is_manual=0,
                    aviso=aviso_dia or None,
                    grau_participacao=None
                )
                total_criados += 1

            st.success(f"Concluído! Internações criadas: {total_internacoes} | "
                       f"Automáticos criados: {total_criados} | Ignorados (auto já existia): {total_ignorados}")


# ============================================================
# 🔍 ABA 2 — CONSULTAR (editar + lançar manual + ver/excluir + cadastrar internação manual)
# ============================================================

with tabs[1]:
    st.header("🔍 Consultar Internação")

    # ========== Cadastrar internação manualmente ==========
    st.subheader("➕ Cadastrar internação manualmente")
    colm1, colm2, colm3, colm4, colm5 = st.columns(5)
    with colm1:
        hosp_new = st.selectbox("Hospital", get_hospitais(), key="new_int_hosp")
    with colm2:
        att_new = st.text_input("Atendimento (único)", key="new_int_att")
    with colm3:
        pac_new = st.text_input("Paciente", key="new_int_pac")
    with colm4:
        data_new = st.date_input("Data de internação", value=date.today(), key="new_int_data")
    with colm5:
        conv_new = st.text_input("Convênio", key="new_int_conv")

    if st.button("Criar internação", key="btn_criar_int"):
        if not att_new:
            st.warning("Informe o atendimento.")
        else:
            if not get_internacao_by_atendimento(att_new).empty:
                st.error("Já existe uma internação com este atendimento.")
            else:
                nid = criar_internacao(hosp_new, att_new, pac_new, data_new.strftime("%d/%m/%Y"), conv_new)
                st.success(f"Internação criada (ID {nid}).")
                st.rerun()

    st.divider()
    # =======================================================

    hlist = ["Todos"] + get_hospitais()
    filtro_hosp = st.selectbox("Filtrar hospital (consulta):", hlist)

    codigo = st.text_input("Digite o atendimento para consultar:")

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

            # --------- Ações de exclusão da internação ---------
            with st.expander("🗑️ Excluir esta internação"):
                st.warning("Esta ação apagará a internação e TODOS os procedimentos vinculados.")
                confirm_txt = st.text_input("Digite APAGAR para confirmar", key="confirm_del_int")
                if st.button("Excluir internação", key="btn_del_int"):
                    if confirm_txt.strip().upper() == "APAGAR":
                        deletar_internacao(internacao_id)
                        st.success("Internação excluída com sucesso.")
                        st.rerun()
                    else:
                        st.info("Confirmação inválida. Digite APAGAR.")

            # --------- Procedimentos da internação ---------
            conn = get_conn()
            df_proc = pd.read_sql_query(
                "SELECT id, data_procedimento, profissional, procedimento, situacao, observacao, aviso, grau_participacao "
                "FROM Procedimentos WHERE internacao_id = ? ORDER BY data_procedimento, id",
                conn, params=(internacao_id,)
            )
            conn.close()

            # Defaults
            if "procedimento" not in df_proc.columns:
                df_proc["procedimento"] = "Cirurgia / Procedimento"
            df_proc["procedimento"] = df_proc["procedimento"].fillna("Cirurgia / Procedimento")
            df_proc["situacao"] = df_proc.get("situacao", pd.Series(dtype=str)).fillna("Pendente")
            df_proc["observacao"] = df_proc.get("observacao", pd.Series(dtype=str)).fillna("")
            df_proc["aviso"] = df_proc.get("aviso", pd.Series(dtype=str)).fillna("")
            df_proc["grau_participacao"] = df_proc.get("grau_participacao", pd.Series(dtype=str)).fillna("")

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
                    "aviso": st.column_config.Column("Aviso", disabled=True),
                    "grau_participacao": st.column_config.SelectboxColumn(
                        "Grau de Participação", options=[""] + GRAU_PARTICIPACAO_OPCOES, required=False
                    ),
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

            if st.button("💾 Salvar alterações", key="btn_save_proc"):
                cols_chk = ["procedimento", "situacao", "observacao", "grau_participacao"]
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
                            "grau_participacao": row["grau_participacao_new"] if row["grau_participacao_new"] != "" else None,
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
                            grau_participacao=item["grau_participacao"],
                        )
                    st.success(f"{len(alterados)} procedimento(s) atualizado(s).")
                    st.rerun()

            # ----- Excluir cirurgia (procedimento) individual -----
            with st.expander("🗑️ Excluir cirurgia (procedimento)"):
                if df_proc.empty:
                    st.info("Não há procedimentos para excluir.")
                else:
                    for _, r in df_proc.iterrows():
                        c1, c2, c3, c4 = st.columns([3, 3, 3, 2])
                        with c1:
                            st.markdown(f"**ID:** {int(r['id'])}  —  **Data:** {r['data_procedimento']}")
                        with c2:
                            st.markdown(f"**Profissional:** {r['profissional'] or '-'}")
                        with c3:
                            st.markdown(f"**Tipo:** {r['procedimento']}")
                        with c4:
                            if st.button("Excluir", key=f"del_proc_{int(r['id'])}"):
                                deletar_procedimento(int(r["id"]))
                                st.success(f"Procedimento {int(r['id'])} excluído.")
                                st.rerun()

            st.divider()
            st.subheader("➕ Lançar procedimento manual (permite vários no mesmo dia)")

            c1, c2, c3 = st.columns(3)
            with c1:
                data_proc = st.date_input("Data do procedimento", value=date.today())
            with c2:
                profissional = st.text_input("Profissional (opcional)")
            with c3:
                situacao = st.selectbox("Situação", STATUS_OPCOES, index=0)

            colp1, colp2, colp3 = st.columns(3)
            with colp1:
                procedimento_tipo = st.selectbox("Tipo de Procedimento", PROCEDIMENTO_OPCOES, index=0)
            with colp2:
                observacao = st.text_input("Observações (opcional)")
            with colp3:
                grau_part = st.selectbox("Grau de Participação", [""] + GRAU_PARTICIPACAO_OPCOES, index=0)

            if st.button("Adicionar procedimento", key="btn_add_manual"):
                data_str = data_proc.strftime("%d/%m/%Y")
                criar_procedimento(
                    internacao_id,
                    data_str,
                    profissional,
                    procedimento_tipo,
                    situacao=situacao,
                    observacao=(observacao or None),
                    is_manual=1,
                    aviso=None,
                    grau_participacao=(grau_part if grau_part != "" else None),
                )
                st.success("Procedimento (manual) adicionado.")
                st.rerun()  # recarrega a página para exibir o novo item imediatamente

            # ====================================================
            # 🔎 Ver dados de QUITAÇÃO (Finalizados)
            # ====================================================
            st.divider()
            st.subheader("🔎 Quitações desta internação (somente Finalizados)")

            finalizados = df_proc[df_proc["situacao"] == "Finalizado"]
            if finalizados.empty:
                st.info("Não há procedimentos finalizados nesta internação.")
            else:
                for _, r in finalizados.iterrows():
                    colA, colB, colC, colD = st.columns([2, 2, 2, 2])
                    with colA:
                        st.markdown(f"**Data:** {r['data_procedimento']}")
                    with colB:
                        st.markdown(f"**Profissional:** {r['profissional'] or '-'}")
                    with colC:
                        st.markdown(f"**Aviso:** {r['aviso'] or '-'}")
                    with colD:
                        if st.button("Ver quitação", key=f"verquit_{int(r['id'])}"):
                            st.session_state["show_quit_id"] = int(r["id"])

                # Painel de detalhes
                if "show_quit_id" in st.session_state and st.session_state["show_quit_id"]:
                    pid = int(st.session_state["show_quit_id"])
                    df_q = get_quitacao_by_proc_id(pid)
                    if not df_q.empty:
                        q = df_q.iloc[0]
                        total = (q["quitacao_valor_amhptiss"] or 0) + (q["quitacao_valor_complemento"] or 0)

                        st.markdown("---")
                        st.markdown("### 🧾 Detalhes da quitação")
                        c1, c2, c3 = st.columns(3)
                        with c1:
                            st.markdown(f"**Atendimento:** {q['atendimento']}")
                            st.markdown(f"**Hospital:** {q['hospital']}")
                            st.markdown(f"**Convênio:** {q['convenio'] or '-'}")
                        with c2:
                            st.markdown(f"**Paciente:** {q['paciente']}")
                            st.markdown(f"**Data procedimento:** {q['data_procedimento'] or '-'}")
                            st.markdown(f"**Profissional:** {q['profissional'] or '-'}")
                        with c3:
                            st.markdown(f"**Status:** {q['situacao']}")
                            st.markdown(f"**Aviso:** {q['aviso'] or '-'}")
                            st.markdown(f"**Grau participação:** {q['grau_participacao'] or '-'}")

                        st.markdown("#### 💳 Quitação")
                        c4, c5, c6 = st.columns(3)
                        with c4:
                            st.markdown(f"**Data da quitação:** {q['quitacao_data'] or '-'}")
                            st.markdown(f"**Guia AMHPTISS:** {q['quitacao_guia_amhptiss'] or '-'}")
                        with c5:
                            st.markdown(f"**Valor Guia AMHPTISS:** {_format_currency_br(q['quitacao_valor_amhptiss'])}")
                            st.markdown(f"**Guia Complemento:** {q['quitacao_guia_complemento'] or '-'}")
                        with c6:
                            st.markdown(f"**Valor Guia Complemento:** {_format_currency_br(q['quitacao_valor_complemento'])}")
                            st.markdown(f"**Total Quitado:** **{_format_currency_br(total)}**")

                        if st.button("Fechar", key="fechar_quit"):
                            st.session_state["show_quit_id"] = None
                            st.rerun()
                    else:
                        st.warning("Não foi possível carregar os dados da quitação.")
                        if st.button("Fechar", key="fechar_quit_err"):
                            st.session_state["show_quit_id"] = None
                            st.rerun()


# ============================================================
# 📑 ABA 3 — RELATÓRIOS (PDF) — Paisagem, colunas na ordem pedida
# ============================================================

if REPORTLAB_OK:
    def _pdf_cirurgias_por_status(df, filtros):
        """
        Gera PDF (bytes) do relatório 'Cirurgias por Status' em PAISAGEM.
        Colunas (na ordem): Atendimento, Aviso, Convênio, Paciente, Data, Profissional, Hospital.
        """
        buf = io.BytesIO()
        doc = SimpleDocTemplate(
            buf,
            pagesize=landscape(A4),
            leftMargin=18, rightMargin=18, topMargin=18, bottomMargin=18
        )
        styles = getSampleStyleSheet()
        H1 = styles["Heading1"]
        H2 = styles["Heading2"]
        N = styles["BodyText"]

        elems = []

        # Título
        elems.append(Paragraph("Relatório — Cirurgias por Status", H1))
        elems.append(Spacer(1, 6))

        # Filtros
        filtros_txt = (
            f"Período: {filtros['ini']} a {filtros['fim']}  |  "
            f"Hospital: {filtros['hospital']}  |  "
            f"Status: {filtros['status']}"
        )
        elems.append(Paragraph(filtros_txt, N))
        elems.append(Spacer(1, 8))

        # Resumo
        total = len(df)
        elems.append(Paragraph(f"Total de cirurgias: <b>{total}</b>", H2))
        if total > 0 and filtros["status"] == "Todos":
            resumo = (
                df.groupby("situacao")["situacao"]
                .count()
                .sort_values(ascending=False)
                .reset_index(name="qtd")
            )
            data_resumo = [["Situação", "Quantidade"]] + resumo.values.tolist()
            t_res = Table(data_resumo, hAlign="LEFT")
            t_res.setStyle(TableStyle([
                ("BACKGROUND", (0,0), (-1,0), colors.HexColor("#F0F0F0")),
                ("TEXTCOLOR", (0,0), (-1,0), colors.black),
                ("GRID", (0,0), (-1,-1), 0.25, colors.grey),
                ("ALIGN", (1,1), (-1,-1), "RIGHT"),
                ("FONTNAME", (0,0), (-1,0), "Helvetica-Bold"),
                ("BOTTOMPADDING", (0,0), (-1,0), 6),
            ]))
            elems.append(t_res)
            elems.append(Spacer(1, 10))

        # Tabela detalhada
        header = ["Atendimento", "Aviso", "Convênio", "Paciente", "Data", "Profissional", "Hospital"]
        data_rows = []
        for _, r in df.iterrows():
            data_rows.append([
                r.get("atendimento") or "",
                r.get("aviso") or "",
                r.get("convenio") or "",
                r.get("paciente") or "",
                r.get("data_procedimento") or "",
                r.get("profissional") or "",
                r.get("hospital") or "",
            ])

        table = Table([header] + data_rows, repeatRows=1)
        table.setStyle(TableStyle([
            ("BACKGROUND", (0,0), (-1,0), colors.HexColor("#E8EEF7")),
            ("TEXTCOLOR", (0,0), (-1,0), colors.black),
            ("GRID", (0,0), (-1,-1), 0.25, colors.grey),
            ("FONTNAME", (0,0), (-1,0), "Helvetica-Bold"),
            ("FONTSIZE", (0,0), (-1,0), 10),
            ("ALIGN", (0,0), (-1,0), "CENTER"),
            ("VALIGN", (0,0), (-1,-1), "TOP"),
            ("ROWBACKGROUNDS", (0,1), (-1,-1), [colors.white, colors.HexColor("#FAFAFA")]),
        ]))
        elems.append(table)

        doc.build(elems)
        pdf_bytes = buf.getvalue()
        buf.close()
        return pdf_bytes
else:
    def _pdf_cirurgias_por_status(*args, **kwargs):
        raise RuntimeError("ReportLab não está instalado. Adicione 'reportlab' ao requirements.txt.")

with tabs[2]:
    st.header("📑 Relatórios — Central")

    st.subheader("1) Cirurgias por Status (PDF)")

    # Filtros
    hosp_opts = ["Todos"] + get_hospitais()
    colf1, colf2, colf3 = st.columns(3)
    with colf1:
        hosp_sel = st.selectbox("Hospital", hosp_opts, index=0, key="rel_hosp")
    with colf2:
        status_opts = ["Todos"] + STATUS_OPCOES
        status_sel = st.selectbox("Status", status_opts, index=0, key="rel_status")
    with colf3:
        hoje = date.today()
        ini_default = hoje.replace(day=1)
        dt_ini = st.date_input("Data inicial", value=ini_default, key="rel_ini")
        dt_fim = st.date_input("Data final", value=hoje, key="rel_fim")

    # Carregar base (inclui 'convenio' para a tabela)
    conn = get_conn()
    sql_rel = """
        SELECT 
            I.hospital, I.atendimento, I.paciente, I.convenio,
            P.data_procedimento, P.aviso, P.profissional,
            P.procedimento, P.situacao
        FROM Procedimentos P
        INNER JOIN Internacoes I ON I.id = P.internacao_id
        WHERE P.procedimento = 'Cirurgia / Procedimento'
    """
    df_rel = pd.read_sql_query(sql_rel, conn)
    conn.close()

    # Filtragem
    if not df_rel.empty:
        df_rel["_data_dt"] = df_rel["data_procedimento"].apply(_pt_date_to_dt)
        mask = (df_rel["_data_dt"].notna()) & (df_rel["_data_dt"] >= dt_ini) & (df_rel["_data_dt"] <= dt_fim)
        df_rel = df_rel[mask].copy()
        if hosp_sel != "Todos":
            df_rel = df_rel[df_rel["hospital"] == hosp_sel]
        if status_sel != "Todos":
            df_rel = df_rel[df_rel["situacao"] == status_sel]
        df_rel = df_rel.sort_values(by=["_data_dt", "hospital", "paciente", "atendimento"])
        df_rel["data_procedimento"] = df_rel["_data_dt"].apply(lambda d: d.strftime("%d/%m/%Y") if pd.notna(d) else "")
        df_rel = df_rel.drop(columns=["_data_dt"])

    # Ações
    colb1, colb2 = st.columns(2)
    with colb1:
        gerar_pdf = st.button("Gerar PDF")

    with colb2:
        if not df_rel.empty:
            csv_bytes = df_rel.to_csv(index=False).encode("utf-8-sig")
            st.download_button(
                "⬇️ Baixar CSV (fallback)",
                data=csv_bytes,
                file_name=f"cirurgias_por_status_{date.today().strftime('%Y%m%d')}.csv",
                mime="text/csv",
                help="Use este CSV caso o PDF esteja indisponível por falta do reportlab."
            )

    if gerar_pdf:
        if df_rel.empty:
            st.warning("Nenhum registro encontrado para os filtros informados.")
        else:
            if not REPORTLAB_OK:
                st.error(
                    "A biblioteca 'reportlab' não está instalada no ambiente.\n"
                    "→ Solução: adicione `reportlab==3.6.13` ao seu requirements.txt e reimplante a app.\n"
                    "Enquanto isso, use o botão 'Baixar CSV (fallback)'."
                )
            else:
                filtros = {
                    "ini": dt_ini.strftime("%d/%m/%Y"),
                    "fim": dt_fim.strftime("%d/%m/%Y"),
                    "hospital": hosp_sel,
                    "status": status_sel,
                }
                pdf_bytes = _pdf_cirurgias_por_status(df_rel, filtros)
                ts = datetime.now().strftime("%Y%m%d_%H%M%S")
                fname = f"relatorio_cirurgias_por_status_{ts}.pdf"
                st.success(f"Relatório gerado com {len(df_rel)} registro(s).")
                st.download_button(
                    label="⬇️ Baixar PDF",
                    data=pdf_bytes,
                    file_name=fname,
                    mime="application/pdf",
                    use_container_width=True
                )


# ============================================================
# 💼 ABA 4 — QUITAÇÃO
# ============================================================

with tabs[3]:
    st.header("💼 Quitação de Cirurgias")

    # Filtro de hospital
    hosp_opts = ["Todos"] + get_hospitais()
    hosp_sel = st.selectbox("Hospital", hosp_opts, index=0, key="quit_hosp")

    # Carrega somente cirurgias "Enviado para pagamento"
    conn = get_conn()
    base = """
        SELECT 
            P.id, I.hospital, I.atendimento, I.paciente, I.convenio,
            P.data_procedimento, P.profissional, P.aviso, P.situacao,
            P.quitacao_data, P.quitacao_guia_amhptiss, P.quitacao_valor_amhptiss,
            P.quitacao_guia_complemento, P.quitacao_valor_complemento
        FROM Procedimentos P
        INNER JOIN Internacoes I ON I.id = P.internacao_id
        WHERE P.procedimento = 'Cirurgia / Procedimento'
          AND P.situacao = 'Enviado para pagamento'
    """
    if hosp_sel == "Todos":
        sql = base + " ORDER BY P.data_procedimento, I.hospital, I.atendimento"
        df_quit = pd.read_sql_query(sql, conn)
    else:
        sql = base + " AND I.hospital = ? ORDER BY P.data_procedimento, I.hospital, I.atendimento"
        df_quit = pd.read_sql_query(sql, conn, params=(hosp_sel,))
    conn.close()

    if df_quit.empty:
        st.info("Não há cirurgias com status 'Enviado para pagamento' para quitação.")
    else:
        # NORMALIZA TIPOS PARA O data_editor
        df_quit["quitacao_data"] = pd.to_datetime(
            df_quit["quitacao_data"], dayfirst=True, errors="coerce"
        )
        for col in ["quitacao_valor_amhptiss", "quitacao_valor_complemento"]:
            df_quit[col] = pd.to_numeric(df_quit[col], errors="coerce")

        st.markdown(
            "Preencha os dados de quitação e clique em **Gravar quitação(ões)**. "
            "Ao gravar, o status muda automaticamente para **Finalizado**."
        )

        edited = st.data_editor(
            df_quit,
            key="editor_quit",
            use_container_width=True,
            hide_index=True,
            column_config={
                "id": st.column_config.Column("ID", disabled=True),
                "hospital": st.column_config.Column("Hospital", disabled=True),
                "atendimento": st.column_config.Column("Atendimento", disabled=True),
                "paciente": st.column_config.Column("Paciente", disabled=True),
                "convenio": st.column_config.Column("Convênio", disabled=True),
                "data_procedimento": st.column_config.Column("Data Procedimento", disabled=True),
                "profissional": st.column_config.Column("Profissional", disabled=True),
                "aviso": st.column_config.Column("Aviso", disabled=True),
                "situacao": st.column_config.Column("Situação", disabled=True),

                # Campos editáveis de quitação (tipos compatíveis)
                "quitacao_data": st.column_config.DateColumn(
                    "Data da quitação", format="DD/MM/YYYY", help="Obrigatório para quitação"
                ),
                "quitacao_guia_amhptiss": st.column_config.TextColumn("Guia AMHPTISS"),
                "quitacao_valor_amhptiss": st.column_config.NumberColumn(
                    "Valor Guia AMHPTISS", format="R$ %.2f"
                ),
                "quitacao_guia_complemento": st.column_config.TextColumn("Guia Complemento"),
                "quitacao_valor_complemento": st.column_config.NumberColumn(
                    "Valor Guia Complemento", format="R$ %.2f"
                ),
            }
        )

        if st.button("💾 Gravar quitação(ões)"):
            cols_chk = [
                "quitacao_data",
                "quitacao_guia_amhptiss",
                "quitacao_valor_amhptiss",
                "quitacao_guia_complemento",
                "quitacao_valor_complemento",
            ]
            compare = df_quit[["id"] + cols_chk].merge(
                edited[["id"] + cols_chk], on="id", suffixes=("_old", "_new")
            )

            atualizados = 0
            faltando_data = 0
            for _, row in compare.iterrows():
                changed = any(
                    (str(row[c + "_old"] or "") != str(row[c + "_new"] or ""))
                    for c in cols_chk
                )
                if not changed:
                    continue

                # Data da quitação é obrigatória para finalizar
                data_q = _to_ddmmyyyy(row["quitacao_data_new"])
                if not data_q:
                    faltando_data += 1
                    continue

                guia_amhp = row["quitacao_guia_amhptiss_new"] or None
                v_amhp = _to_float_or_none(row["quitacao_valor_amhptiss_new"])
                guia_comp = row["quitacao_guia_complemento_new"] or None
                v_comp = _to_float_or_none(row["quitacao_valor_complemento_new"])

                quitar_procedimento(
                    proc_id=int(row["id"]),
                    data_quitacao=data_q,
                    guia_amhptiss=guia_amhp,
                    valor_amhptiss=v_amhp,
                    guia_complemento=guia_comp,
                    valor_complemento=v_comp,
                )
                atualizados += 1

            if faltando_data > 0 and atualizados == 0:
                st.warning("Nenhuma quitação gravada. Preencha a **Data da quitação** para finalizar.")
            elif faltando_data > 0 and atualizados > 0:
                st.success(f"{atualizados} quitação(ões) gravada(s). "
                           f"Atenção: {faltando_data} linha(s) ignoradas por falta de **Data da quitação**.")
                st.rerun()
            else:
                st.success(f"{atualizados} quitação(ões) gravada(s).")
                st.rerun()


# ============================================================
# ⚙️ ABA 5 — SISTEMA (agrega: Procedimentos, Profissionais, Convênios)
# ============================================================

with tabs[4]:
    st.header("⚙️ Sistema")

    # -------------------------------
    # Seção 1: 📋 Procedimentos (lista)
    # -------------------------------
    st.subheader("📋 Procedimentos — Lista")
    filtro = ["Todos"] + get_hospitais()
    chosen = st.selectbox("Hospital (lista de procedimentos):", filtro, key="sys_proc_hosp")

    if st.button("Carregar procedimentos", key="btn_carregar_proc"):
        conn = get_conn()
        base = """
            SELECT P.id, I.hospital, I.atendimento, I.paciente,
                   P.data_procedimento, P.aviso, P.profissional, P.grau_participacao, P.procedimento,
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

    st.divider()

    # -------------------------------
    # Seção 2: 🧾 Resumo por Profissional
    # -------------------------------
    st.subheader("🧾 Resumo por Profissional")
    filtro_prof = ["Todos"] + get_hospitais()
    chosen_prof = st.selectbox("Hospital (resumo por profissional):", filtro_prof, key="sys_prof_hosp")

    conn = get_conn()
    base_prof = """
        SELECT profissional, COUNT(*) AS total
        FROM Procedimentos P
        INNER JOIN Internacoes I ON I.id = P.internacao_id
        WHERE profissional IS NOT NULL AND profissional <> ''
    """

    if chosen_prof == "Todos":
        sql = base_prof + " GROUP BY profissional ORDER BY total DESC"
        df_prof = pd.read_sql_query(sql, conn)
    else:
        sql = base_prof + " AND I.hospital = ? GROUP BY profissional ORDER BY total DESC"
        df_prof = pd.read_sql_query(sql, conn, params=(chosen_prof,))
    conn.close()

    st.dataframe(df_prof, use_container_width=True)

    st.divider()

    # -------------------------------
    # Seção 3: 💸 Resumo por Convênio
    # -------------------------------
    st.subheader("💸 Resumo por Convênio")
    filtro_conv = ["Todos"] + get_hospitais()
    chosen_conv = st.selectbox("Hospital (resumo por convênio):", filtro_conv, key="sys_conv_hosp")

    conn = get_conn()
    base_conv = """
        SELECT I.convenio, COUNT(*) AS total
        FROM Internacoes I
        INNER JOIN Procedimentos P ON P.internacao_id = I.id
        WHERE I.convenio IS NOT NULL AND I.convenio <> ''
    """

    if chosen_conv == "Todos":
        sql = base_conv + " GROUP BY I.convenio ORDER BY total DESC"
        df_conv = pd.read_sql_query(sql, conn)
    else:
        sql = base_conv + " AND I.hospital = ? GROUP BY I.convenio ORDER BY total DESC"
        df_conv = pd.read_sql_query(sql, conn, params=(chosen_conv,))
    conn.close()

    st.dataframe(df_conv, use_container_width=True)
