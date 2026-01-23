# ============================================================
#  SISTEMA DE INTERNAÇÕES — VERSÃO SUPABASE
# ============================================================

import streamlit as st
import pandas as pd
from datetime import date, datetime
import io
import base64, json
import requests
import re
import streamlit.components.v1 as components
from supabase import create_client, Client # Requer 'supabase' no requirements.txt

# ==== PDF (ReportLab) ====
REPORTLAB_OK = True
try:
    from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer
    from reportlab.lib.pagesizes import A4, landscape
    from reportlab.lib import colors
    from reportlab.lib.styles import getSampleStyleSheet
    from reportlab.lib.units import cm
except ModuleNotFoundError:
    REPORTLAB_OK = False

from parser import parse_tiss_original

# Credenciais (coloque no .streamlit/secrets.toml)
URL = st.secrets["SUPABASE_URL"]
KEY = st.secrets["SUPABASE_KEY"]
supabase: Client = create_client(URL, KEY)

# ---------------------------
# Domínio
# ---------------------------
STATUS_OPCOES = [
    "Pendente",
    "Não Cobrar",
    "Enviado para pagamento",
    "Aguardando Digitação - AMHP",
    "Finalizado",
]
PROCEDIMENTO_OPCOES = ["Cirurgia / Procedimento", "Parecer"]
GRAU_PARTICIPACAO_OPCOES = ["Cirurgião", "1 Auxiliar", "2 Auxiliar", "3 Auxiliar", "Clínico"]

# Médicos sempre marcados na importação
ALWAYS_SELECTED_PROS = {"JOSE.ADORNO", "CASSIO CESAR", "FERNANDO AND", "SIMAO.MATOS"}

# ---------------------------
# Aparência — CSS e componentes auxiliares
# ---------------------------


def inject_css():
    st.markdown("""
    
    <style>
    /* ===== KPIs maiores e centralizados ===== */
    .kpi-wrap.center .kpi{
      text-align:center;
    }
    
    .kpi.big .label{
      font-size: 1.05rem;        /* título maior */
      font-weight: 700;
    }
    
    .kpi.big .value{
      font-size: 2.4rem;         /* número bem visível */
      line-height: 2.6rem;
      font-weight: 800;
      color: var(--text);
    }
    
    .kpi.big .hint{
      font-size: .95rem;         /* subtítulo um pouco maior */
      color: var(--muted);
      margin-top: 4px;
    }
    
    /* opcional: mais destaque ao passar o mouse no card inteiro */
    .kpi.big:hover{
      box-shadow: 0 1px 0 rgba(0,0,0,.03);
    }
    
    /* opcional: botões logo abaixo dos KPIs, com “peso” visual */
    .kpi-action button{
      font-size: 0.95rem !important;
      font-weight: 700 !important;
    }
    </style>

    <style>
    /* ============================
       PALHETA NEUTRA TRADICIONAL
       ============================ */
    :root{
      --bg-main: #F5F6F7;       /* cinza claro de fundo */
      --bg-card: #FFFFFF;       /* cartões */
      --border:  #D0D7DE;       /* borda suave */
      --text:    #24292F;       /* texto principal */
      --muted:   #6B7280;       /* texto secundário */
      --primary: #1F6FEB;       /* azul discreto */
      --primary-hover: #1558B0; /* hover */
      --radius:  8px;
    }

    html, body, .stApp{
      background-color: var(--bg-main) !important;
      color: var(--text) !important;
      font-family: "Segoe UI", Roboto, Arial, sans-serif;
    }

    /* HEADER simples */
    .app-header{
      background: var(--bg-main);
      padding: 10px 12px;
      margin: -1.2rem -1rem 0.8rem -1rem;
      border-bottom: 1px solid var(--border);
    }
    .app-header .title{ font-size:1.2rem; font-weight:700; color: var(--primary); }
    .app-header .sub  { font-size:.9rem;  color: var(--muted); }

    /* CARDS */
    .soft-card{
      background: var(--bg-card);
      border:1px solid var(--border);
      border-radius: var(--radius);
      padding: 14px 16px;
      margin-bottom: 12px;
    }

    /* ============================
       INPUTS (Texto/Number/Date)
       ============================ */
    .stTextInput input,
    .stNumberInput input,
    .stDateInput input,
    .stTextArea textarea{
      background:#FFFFFF !important;
      color: var(--text) !important;
      border:1px solid var(--border) !important;
      border-radius: var(--radius) !important;
      box-shadow:none !important;
    }

    /* Label mais visível, porém discreto */
    label, .st-emotion-cache-1qg05tj p, .stMarkdown p{
      color: var(--muted) !important;
    }

    /* ============================
       SELECTBOX (BaseWeb) CLARO
       ============================ */
    /* Caixa do select (sem fundo escuro) */
    div[data-baseweb="select"]{
      background:#FFFFFF !important;
      border:1px solid var(--border) !important;
      border-radius: var(--radius) !important;
      color: var(--text) !important;
    }
    /* Área de valor */
    div[data-baseweb="select"] div[role="combobox"]{
      background:#FFFFFF !important;
      color: var(--text) !important;
    }
    /* Itens do menu */
    div[data-baseweb="menu"]{
      background:#FFFFFF !important;
      border:1px solid var(--border) !important;
      border-radius: var(--radius) !important;
      color: var(--text) !important;
    }
    div[data-baseweb="option"]{
      background:#FFFFFF !important;
      color: var(--text) !important;
    }
    div[data-baseweb="option"][aria-selected="true"]{
      background:#EEF2FF !important; /* leve */
      color:#111827 !important;
    }
    div[data-baseweb="option"]:hover{
      background:#F3F4F6 !important;
    }

    /* ============================
       FILE UPLOADER CLARO
       ============================ */
    .stFileUploader > section{
      border:1px solid var(--border) !important;
      background:#FFFFFF !important;
      border-radius: var(--radius) !important;
    }
    .stFileUploader div[role="button"]{
      background:#FFFFFF !important;
      color: var(--text) !important;
      border:1px solid var(--border) !important;
      border-radius: var(--radius) !important;
    }

    /* ============================
       BOTÕES
       ============================ */
    .stButton>button{
      background: var(--primary) !important;
      color:#FFFFFF !important;
      border:none !important;
      border-radius: var(--radius) !important;
      padding: 6px 16px !important;
      font-weight:600 !important;
      box-shadow:none !important;
    }
    .stButton>button:hover{ background: var(--primary-hover) !important; }

    /* ============================
       TABELAS/EDITORES
       ============================ */
    .element-container:has(.stDataFrame) .st-emotion-cache-1wmy9hl,
    .element-container:has(.stDataEditor) .st-emotion-cache-1wmy9hl{
      background:#FFFFFF;
      border:1px solid var(--border);
      border-radius: var(--radius);
      padding-top:6px;
    }

    /* ============================
       TABS (linha ativa discreta)
       ============================ */
    button[role="tab"][aria-selected="true"]{
      border-bottom:2px solid var(--primary) !important;
      color: var(--text) !important;
    }

    /* SIDEBAR neutra */
    section[data-testid="stSidebar"] .block-container{
      background: var(--bg-main);
      border-right: 1px solid var(--border);
    }
    </style>
    """, unsafe_allow_html=True)


def pill(situacao: str) -> str:
    """Retorna HTML de um pill colorido por status."""
    s = (situacao or "").strip()
    cls = "pill"
    if s == "Pendente": cls += " pill-pendente"
    elif s == "Não Cobrar": cls += " pill-nc"
    elif s == "Enviado para pagamento": cls += " pill-enviado"
    elif s == "Aguardando Digitação - AMHP": cls += " pill-digitacao"
    elif s == "Finalizado": cls += " pill-ok"
    return f"<span class='{cls}'>{s or '-'}</span>"


def kpi_row(items, extra_class: str = ""):
    """
    items: lista de dicts [{label, value, hint (opcional)}]
    extra_class: classes extras na <div class='kpi-wrap ...'> (ex.: 'center')
    """
    st.markdown(f"<div class='kpi-wrap {extra_class}'>", unsafe_allow_html=True)
    for it in items:
        st.markdown(
            f"""
            <div class='kpi big'>  <!-- adiciona 'big' aqui -->
              <div class='label'>{it.get('label','')}</div>
              <div class='value'>{it.get('value','')}</div>
              {'<div class="hint">'+it.get('hint','')+'</div>' if it.get('hint') else ''}
            </div>
            """,
            unsafe_allow_html=True
        )
    st.markdown("</div>", unsafe_allow_html=True)

def app_header(title: str, subtitle: str = ""):
    st.markdown(
        f"""
        <div class="app-header">
            <div class="title">🏥 {title}</div>
            <div class="sub">{subtitle}</div>
        </div>
        """,
        unsafe_allow_html=True
    )

# ============================================================
# BANCO
# ============================================================

def create_tables():
    """Cria/migra tabelas sem DROP; índice único parcial para auto 1/dia."""
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

    # Procedimentos
    cur.execute("""
    CREATE TABLE IF NOT EXISTS Procedimentos (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        internacao_id INTEGER,
        data_procedimento TEXT,
        profissional TEXT,
        procedimento TEXT,
        situacao TEXT NOT NULL DEFAULT 'Pendente',
        observacao TEXT,
        is_manual INTEGER NOT NULL DEFAULT 0,
        aviso TEXT,
        grau_participacao TEXT,
        quitacao_data TEXT,
        quitacao_guia_amhptiss TEXT,
        quitacao_valor_amhptiss REAL,
        quitacao_guia_complemento TEXT,
        quitacao_valor_complemento REAL,
        quitacao_observacao TEXT,
        FOREIGN KEY(internacao_id) REFERENCES Internacoes(id)
    );
    """)

    # Migrações leves (idempotentes)
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
        "ALTER TABLE Procedimentos ADD COLUMN quitacao_observacao TEXT;",
    ]:
        try:
            cur.execute(alter)
        except sqlite3.OperationalError:
            pass

    # Índice único parcial: 1 automático por (internação, data)
    cur.execute("""
    CREATE UNIQUE INDEX IF NOT EXISTS ux_proc_auto
      ON Procedimentos(internacao_id, data_procedimento)
      WHERE is_manual = 0;
    """)

    conn.commit()
    conn.close()

def seed_hospitais():
    H = [
        "Santa Lucia Sul","Santa Lucia Norte","Maria Auxiliadora",
        "Santa Lucia Taguatinga","Santa Lucia Águas Claras","Santa Lucia Sudoeste"
    ]
    conn = get_conn()
    cur = conn.cursor()
    for nome in H:
        cur.execute("INSERT OR IGNORE INTO Hospitals (name, active) VALUES (?,1)", (nome,))
    conn.commit()
    conn.close()

# ============================================================
# UTIL
# ============================================================

def get_hospitais(include_inactive: bool = False) -> list:
    query = supabase.table("Hospitals").select("name")
    if not include_inactive:
        query = query.eq("active", 1)
    res = query.order("name").execute()
    return [item['name'] for item in res.data]

def _pt_date_to_dt(s):
    try:
        return datetime.strptime(s, "%d/%m/%Y").date()
    except Exception:
        return None

def _to_ddmmyyyy(value):
    if value is None or value == "": return ""
    if isinstance(value, pd.Timestamp): return value.strftime("%d/%m/%Y")
    if isinstance(value, datetime): return value.strftime("%d/%m/%Y")
    if isinstance(value, date): return value.strftime("%d/%m/%Y")
    try:
        dt = datetime.strptime(str(value), "%Y-%m-%d"); return dt.strftime("%d/%m/%Y")
    except Exception:
        pass
    try:
        dt = datetime.strptime(str(value), "%d/%m/%Y"); return dt.strftime("%d/%m/%Y")
    except Exception:
        return str(value)




def _to_float_or_none(v):
    if v is None or v == "": return None
    if isinstance(v, (int,float)): return float(v)
    s = str(v)
    s = re.sub(r"[^\d,.\-]", "", s)  # remove "R$", espaços, etc.
    if "," in s and "." in s: s = s.replace(".", "").replace(",", ".")
    elif "," in s:            s = s.replace(",", ".")
    try: return float(s)
    except: return None




def _format_currency_br(v) -> str:
    if v is None or (isinstance(v, float) and pd.isna(v)): return "R$ 0,00"
    try:
        v = float(v); s = f"{v:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
        return f"R$ {s}"
    except Exception:
        return f"R$ {v}"

# ============================================================
# CRUD
# ============================================================

def apagar_internacoes(lista_at):
    if not lista_at: return
    conn = get_conn(); cur = conn.cursor()
    qmarks = ",".join(["?"] * len(lista_at))
    cur.execute(f"""
        DELETE FROM Procedimentos
         WHERE internacao_id IN (
             SELECT id FROM Internacoes
              WHERE atendimento IN ({qmarks})
         )
    """, lista_at)
    cur.execute(f"DELETE FROM Internacoes WHERE atendimento IN ({qmarks})", lista_at)
    conn.commit(); conn.close()
    mark_db_dirty()


def criar_procedimento(internacao_id, data_proc, profissional, procedimento, **kwargs):
    payload = {
        "internacao_id": internacao_id,
        "data_procedimento": data_proc,
        "profissional": profissional,
        "procedimento": procedimento,
        **kwargs
    }
    supabase.table("Procedimentos").insert(payload).execute()


def deletar_internacao(internacao_id: int):
    conn = get_conn(); cur = conn.cursor()
    cur.execute("DELETE FROM Procedimentos WHERE internacao_id = ?", (internacao_id,))
    cur.execute("DELETE FROM Internacoes WHERE id = ?", (internacao_id,))
    conn.commit(); conn.close()
    mark_db_dirty()

def deletar_procedimento(proc_id: int):
    conn = get_conn(); cur = conn.cursor()
    cur.execute("DELETE FROM Procedimentos WHERE id = ?", (proc_id,))
    conn.commit(); conn.close()
    mark_db_dirty()

def criar_internacao(hospital, atendimento, paciente, data, convenio):
    payload = {
        "hospital": hospital,
        "atendimento": str(atendimento),
        "paciente": paciente,
        "data_internacao": data,
        "convenio": convenio,
        "numero_internacao": float(atendimento)
    }
    res = supabase.table("Internacoes").insert(payload).execute()
    return res.data[0]['id']

def criar_procedimento(internacao_id, data_proc, profissional, procedimento,
                       situacao="Pendente", observacao=None, is_manual=0,
                       aviso=None, grau_participacao=None):
    conn = get_conn(); cur = conn.cursor()
    cur.execute("""
        INSERT OR IGNORE INTO Procedimentos
        (internacao_id, data_procedimento, profissional, procedimento, situacao, observacao, is_manual, aviso, grau_participacao)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (internacao_id, data_proc, profissional, procedimento, situacao, observacao, is_manual, aviso, grau_participacao))
    conn.commit(); conn.close()
    mark_db_dirty()

def existe_procedimento_no_dia(internacao_id, data_proc):
    conn = get_conn(); cur = conn.cursor()
    cur.execute("""
        SELECT 1 FROM Procedimentos
        WHERE internacao_id = ? AND data_procedimento = ? AND is_manual = 0
        LIMIT 1
    """, (internacao_id, data_proc))
    ok = cur.fetchone() is not None
    conn.close(); return ok


def atualizar_procedimento(proc_id, procedimento=None, situacao=None,
                           observacao=None, grau_participacao=None, aviso=None):

    sets, params = [], []

    if procedimento is not None:
        sets.append("procedimento = ?")
        params.append(procedimento)

    if situacao is not None:
        sets.append("situacao = ?")
        params.append(situacao)

    if observacao is not None:
        sets.append("observacao = ?")
        params.append(observacao)

    if grau_participacao is not None:
        sets.append("grau_participacao = ?")
        params.append(grau_participacao)

    if aviso is not None:
        sets.append("aviso = ?")
        params.append(aviso)

    if not sets:
        return

    params.append(proc_id)
    sql = f"UPDATE Procedimentos SET {', '.join(sets)} WHERE id = ?"

    conn = get_conn()
    cur = conn.cursor()
    cur.execute(sql, params)
    conn.commit()
    conn.close()

    mark_db_dirty()


def quitar_procedimento(proc_id, data_quitacao=None, guia_amhptiss=None, valor_amhptiss=None,
                        guia_complemento=None, valor_complemento=None, quitacao_observacao=None):
    conn = get_conn(); cur = conn.cursor()
    cur.execute("""
        UPDATE Procedimentos
           SET quitacao_data = ?,
               quitacao_guia_amhptiss = ?,
               quitacao_valor_amhptiss = ?,
               quitacao_guia_complemento = ?,
               quitacao_valor_complemento = ?,
               quitacao_observacao = ?,
               situacao = 'Finalizado'
         WHERE id = ?
    """, (data_quitacao, guia_amhptiss, valor_amhptiss, guia_complemento, valor_complemento, quitacao_observacao, proc_id))
    conn.commit(); conn.close()
    mark_db_dirty()


def reverter_quitacao(proc_id: int):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        UPDATE Procedimentos
           SET quitacao_data = NULL,
               quitacao_guia_amhptiss = NULL,
               quitacao_valor_amhptiss = NULL,
               quitacao_guia_complemento = NULL,
               quitacao_valor_complemento = NULL,
               quitacao_observacao = NULL,
               situacao = 'Enviado para pagamento'
         WHERE id = ?
    """, (proc_id,))
    conn.commit()
    conn.close()
    mark_db_dirty()


def get_internacao_by_atendimento(att):
    res = supabase.table("Internacoes").select("*").eq("atendimento", str(att)).execute()
    return pd.DataFrame(res.data)

def get_procedimentos(internacao_id):
    res = supabase.table("Procedimentos").select("*").eq("internacao_id", internacao_id).execute()
    return pd.DataFrame(res.data)

def get_quitacao_by_proc_id(proc_id: int):
    conn = get_conn()
    sql = """
        SELECT
            P.id, P.data_procedimento, P.profissional, P.situacao, P.aviso, P.observacao, P.grau_participacao,
            P.quitacao_data, P.quitacao_guia_amhptiss, P.quitacao_valor_amhptiss,
            P.quitacao_guia_complemento, P.quitacao_valor_complemento, P.quitacao_observacao,
            I.hospital, I.atendimento, I.paciente, I.convenio
        FROM Procedimentos P
        INNER JOIN Internacoes I ON I.id = P.internacao_id
        WHERE P.id = ?
    """
    df = pd.read_sql_query(sql, conn, params=(proc_id,))
    conn.close(); return df

# ============================================================
# INICIALIZAÇÃO
# ============================================================

st.set_page_config(page_title="Gestão de Internações", page_icon="🏥", layout="wide")

inject_css()  # <<< estilo global

sync_down_db()      # baixa snapshot do GitHub (se existir)
create_tables()     # garante schema/migrações
seed_hospitais()    # seeds

app_header("Sistema de Internações — Versão Final",
           "Importação, edição, quitação e relatórios com persistência local/GitHub")


def _switch_to_tab_by_label(tab_label: str):
    """
    Clica na aba cujo rótulo visível contém `tab_label` (match por substring
    com normalização de espaços). Faz polling por até 2s.
    """
    js = f"""
    <script>
    (function() {{
      const target = "{tab_label}".trim();
      const norm = (s) => (s || "").replace(/\\s+/g, " ").trim();

      let attempts = 0;
      const maxAttempts = 20;  // 20 * 100ms = 2s
      const timer = setInterval(() => {{
        attempts += 1;
        const tabs = window.parent.document.querySelectorAll('button[role="tab"]');
        for (const t of tabs) {{
          const txt = norm(t.textContent || t.innerText);
          // casa por substring (ex.: emoji + título) com normalização
          if (txt.includes(norm(target))) {{
            t.click();
            clearInterval(timer);
            return;
          }}
        }}
        if (attempts >= maxAttempts) {{
          clearInterval(timer);
          console.warn("Tab não encontrada para:", target);
        }}
      }}, 100);
    }})();
    </script>
    """
    components.html(js, height=0, width=0)


# ============================================================
# ABAS
# ============================================================

tabs = st.tabs([
    "🏠 Início",               # tabs[0]
    "📤 Importar Arquivo",     # tabs[1]
    "🔍 Consultar Internação", # tabs[2]
    "📑 Relatórios",           # tabs[3]
    "💼 Quitação",             # tabs[4]
    "⚙️ Sistema",              # tabs[5]
])


# ============================================================
# 🏠 0) INÍCIO — KPIs (todos os procedimentos) + Filtros opcionais + Listagem
# ============================================================
with tabs[0]:
    st.subheader("🏠 Tela Inicial")

    # Estado
    if "home_status" not in st.session_state:
        st.session_state["home_status"] = None  # status aberto/fechado na lista

    hoje = date.today()
    ini_mes = hoje.replace(day=1)

    # -------------------------
    # Filtros
    # -------------------------
    colf1, colf2 = st.columns([2, 3])
    with colf1:
        filtro_hosp_home = st.selectbox(
            "Hospital",
            ["Todos"] + get_hospitais(),
            index=0,
            key="home_f_hosp"
        )
    with colf2:
        st.write(" ")
        st.caption("Períodos (opcionais)")

    cbox1, cbox2 = st.columns(2)
    with cbox1:
        use_int_range = st.checkbox("Filtrar por data da internação", key="home_use_int_range", value=False)
    with cbox2:
        use_proc_range = st.checkbox("Filtrar por data do procedimento", key="home_use_proc_range", value=False)

    if use_int_range or use_proc_range:
        cold1, cold2, cold3, cold4 = st.columns(4)
        with cold1:
            int_ini = st.date_input("Internação — início", value=st.session_state.get("home_f_int_ini", ini_mes), key="home_f_int_ini")
        with cold2:
            int_fim = st.date_input("Internação — fim", value=st.session_state.get("home_f_int_fim", hoje), key="home_f_int_fim")
        with cold3:
            proc_ini = st.date_input("Procedimento — início", value=st.session_state.get("home_f_proc_ini", ini_mes), key="home_f_proc_ini")
        with cold4:
            proc_fim = st.date_input("Procedimento — fim", value=st.session_state.get("home_f_proc_fim", hoje), key="home_f_proc_fim")

    # -------------------------
    # Base de dados: TODOS os procedimentos
    # -------------------------
    conn = get_conn()
    sql_all = """
        SELECT
            I.id                      AS internacao_id,
            I.atendimento,
            I.paciente,
            I.hospital,
            I.convenio,
            I.data_internacao,
            P.id                      AS procedimento_id,
            P.data_procedimento,
            P.procedimento,
            P.profissional,
            P.situacao,
            P.aviso,
            P.grau_participacao
        FROM Procedimentos P
        INNER JOIN Internacoes I ON I.id = P.internacao_id
    """
    df_all = pd.read_sql_query(sql_all, conn)
    conn.close()

    def _safe_pt_date(s):
        try:
            return datetime.strptime(str(s).strip(), "%d/%m/%Y").date()
        except Exception:
            try:
                return datetime.strptime(str(s).strip(), "%Y-%m-%d").date()
            except Exception:
                return None

    # -------------------------
    # Aplicação de filtros (apenas quando ativados)
    # -------------------------
    if df_all.empty:
        df_f = df_all.copy()
    else:
        df_all["_int_dt"]  = df_all["data_internacao"].apply(_safe_pt_date)
        df_all["_proc_dt"] = df_all["data_procedimento"].apply(_safe_pt_date)

        mask = pd.Series([True]*len(df_all), index=df_all.index)

        # Hospital
        if filtro_hosp_home != "Todos":
            mask &= (df_all["hospital"] == filtro_hosp_home)

        # Período de internação (somente se marcado)
        if use_int_range:
            mask &= df_all["_int_dt"].notna()
            mask &= (df_all["_int_dt"] >= st.session_state["home_f_int_ini"])
            mask &= (df_all["_int_dt"] <= st.session_state["home_f_int_fim"])

        # Período de procedimento (somente se marcado)
        if use_proc_range:
            mask &= df_all["_proc_dt"].notna()
            mask &= (df_all["_proc_dt"] >= st.session_state["home_f_proc_ini"])
            mask &= (df_all["_proc_dt"] <= st.session_state["home_f_proc_fim"])

        df_f = df_all[mask].copy()

    # -------------------------
    # KPIs (todos os procedimentos) — com TOGGLE
    # -------------------------
    tot_pendente   = int((df_f["situacao"] == "Pendente").sum()) if not df_f.empty else 0
    tot_finalizado = int((df_f["situacao"] == "Finalizado").sum()) if not df_f.empty else 0
    tot_nao_cobrar = int((df_f["situacao"] == "Não Cobrar").sum()) if not df_f.empty else 0

    def _toggle_home_status(target: str):
        curr = st.session_state.get("home_status")
        st.session_state["home_status"] = None if curr == target else target
        st.rerun()

    
    active = st.session_state.get("home_status")
    
    c1, c2, c3 = st.columns(3)
    
    with c1:
        # KPI grande e centralizado
        kpi_row(
            [{"label":"Pendentes", "value": f"{tot_pendente}", "hint": "Todos os procedimentos (geral/filtrado)"}],
            extra_class="center"
        )
        # Botão logo abaixo (ocupando a coluna toda)
        lbl = "🔽 Esconder Pendentes" if active == "Pendente" else "👁️ Ver Pendentes"
        with st.container():  # wrapper para permitir classe opcional
            st.markdown("<div class='kpi-action'>", unsafe_allow_html=True)
            if st.button(lbl, key="kpi_btn_pend", use_container_width=True):
                _toggle_home_status("Pendente")
            st.markdown("</div>", unsafe_allow_html=True)
    
    with c2:
        kpi_row(
            [{"label":"Finalizadas", "value": f"{tot_finalizado}", "hint": "Todos os procedimentos (geral/filtrado)"}],
            extra_class="center"
        )
        lbl = "🔽 Esconder Finalizadas" if active == "Finalizado" else "👁️ Ver Finalizadas"
        st.markdown("<div class='kpi-action'>", unsafe_allow_html=True)
        if st.button(lbl, key="kpi_btn_fin", use_container_width=True):
            _toggle_home_status("Finalizado")
        st.markdown("</div>", unsafe_allow_html=True)
    
    with c3:
        kpi_row(
            [{"label":"Não Cobrar", "value": f"{tot_nao_cobrar}", "hint": "Todos os procedimentos (geral/filtrado)"}],
            extra_class="center"
        )
        lbl = "🔽 Esconder Não Cobrar" if active == "Não Cobrar" else "👁️ Ver Não Cobrar"
        st.markdown("<div class='kpi-action'>", unsafe_allow_html=True)
        if st.button(lbl, key="kpi_btn_nc", use_container_width=True):
            _toggle_home_status("Não Cobrar")
        st.markdown("</div>", unsafe_allow_html=True)

  
    # -------------------------
    # Listagem de internações (toggle ON) + fechar lista + abrir na consulta
    # -------------------------
    status_sel_home = st.session_state.get("home_status")
    
    if status_sel_home:
        st.divider()
        st.subheader(f"📋 Internações com ao menos 1 procedimento em: **{status_sel_home}**")
    
        # Botão fechar lista
        cc1, _ = st.columns([1, 6])
        with cc1:
            if st.button("Fechar lista", key="btn_close_list", type="secondary", use_container_width=True):
                st.session_state["home_status"] = None
                st.rerun()
    
        if df_f.empty:
            st.info("Nenhuma internação encontrada com os filtros aplicados.")
        else:
            df_status = df_f[df_f["situacao"] == status_sel_home].copy()
    
            if df_status.empty:
                st.info("Nenhuma internação encontrada para este status com os filtros atuais.")
            else:
                # === ORDENAR POR DATA DA INTERNAÇÃO (mais recentes primeiro) ===
                def _safe_pt_date_int(s):
                    try:
                        return datetime.strptime(str(s).strip(), "%d/%m/%Y").date()
                    except Exception:
                        try:
                            return datetime.strptime(str(s).strip(), "%Y-%m-%d").date()
                        except Exception:
                            return None
    
                cols_show = ["internacao_id","atendimento","paciente","hospital","convenio","data_internacao"]
    
                # Um card por internação
                df_ints = (
                    df_status[cols_show]
                    .drop_duplicates(subset=["internacao_id"])
                    .copy()
                )
    
                # Converte data da internação e ordena (desc)
                df_ints["_int_dt"] = df_ints["data_internacao"].apply(_safe_pt_date_int)
                # Empate estável por hospital/paciente
                df_ints = (
                    df_ints
                    .sort_values(by=["_int_dt", "hospital", "paciente"], ascending=[False, True, True])
                    .drop(columns=["_int_dt"])
                )
    
                # Renderização dos cards
                for _, r in df_ints.iterrows():
                    i1, i2, i3, i4 = st.columns([3, 3, 3, 2])
                    with i1:
                        st.markdown(f"**Atendimento:** {r['atendimento']}  \n**Paciente:** {r.get('paciente') or '-'}")
                    with i2:
                        st.markdown(f"**Hospital:** {r.get('hospital') or '-'}  \n**Convênio:** {r.get('convenio') or '-'}")
                    with i3:
                        st.markdown(f"**Data internação:** {r.get('data_internacao') or '-'}")
                    with i4:
                        if st.button("🔎 Abrir na Consulta", key=f"open_cons_{int(r['internacao_id'])}", use_container_width=True):
                            st.session_state["consulta_codigo"] = str(r["atendimento"])
                            st.session_state["goto_tab_label"] = "🔍 Consultar Internação"


    # Lembrete visual
    if st.session_state.get("consulta_codigo"):
        st.caption(f"🔎 Atendimento **{st.session_state['consulta_codigo']}** pronto para consulta na aba **'🔍 Consultar Internação'**.")


    
    st.markdown("</div>", unsafe_allow_html=True)

# ============================================================
# 📤 1) IMPORTAR (cadastro manual + seleção de médicos)
# ============================================================

with tabs[1]:
    st.subheader("📤 Importar arquivo")

    # Cadastro de internação manual (somente aqui)
    st.markdown("<div class='soft-card'>", unsafe_allow_html=True)
    st.markdown("**➕ Cadastrar internação manualmente (na importação)**")
    cmi1, cmi2, cmi3, cmi4, cmi5 = st.columns(5)
    with cmi1: hosp_new = st.selectbox("Hospital", get_hospitais(), key="imp_new_int_hosp")
    with cmi2: att_new = st.text_input("Atendimento (único)", key="imp_new_int_att")
    with cmi3: pac_new = st.text_input("Paciente", key="imp_new_int_pac")
    with cmi4: data_new = st.date_input("Data de internação", value=date.today(), key="imp_new_int_data")
    with cmi5: conv_new = st.text_input("Convênio", key="imp_new_int_conv")

    col_btn = st.columns(6)[-1]
    with col_btn:
        if st.button("Criar internação", key="imp_btn_criar_int", type="primary"):
            if not att_new:
                st.warning("Informe o atendimento.")
            elif not get_internacao_by_atendimento(att_new).empty:
                st.error("Já existe uma internação com este atendimento.")
            else:
                nid = criar_internacao(hosp_new, att_new, pac_new, data_new.strftime("%d/%m/%Y"), conv_new)
                st.toast(f"Internação criada (ID {nid}).", icon="✅")
                maybe_sync_up_db("chore(db): criação manual de internação (aba Importar)")
    st.markdown("</div>", unsafe_allow_html=True)

    st.divider()

    st.markdown("<div class='soft-card'>", unsafe_allow_html=True)
    hospitais = get_hospitais()
    hospital = st.selectbox("Hospital para esta importação:", hospitais)

    arquivo = st.file_uploader("Selecione o arquivo CSV")

    # Estado de seleção de médicos
    if "import_all_docs" not in st.session_state: st.session_state["import_all_docs"] = True
    if "import_selected_docs" not in st.session_state: st.session_state["import_selected_docs"] = []

    if arquivo:
        raw_bytes = arquivo.getvalue()
        try: csv_text = raw_bytes.decode("latin1")
        except UnicodeDecodeError: csv_text = raw_bytes.decode("utf-8-sig", errors="ignore")

        registros = parse_tiss_original(csv_text)
        st.success(f"{len(registros)} registros interpretados!")

        # KPIs
        pros = sorted({(r.get("profissional") or "").strip() for r in registros if r.get("profissional")})
        pares = sorted({(r.get("atendimento"), r.get("data")) for r in registros if r.get("atendimento") and r.get("data")})
        kpi_row([
            {"label":"Registros no arquivo", "value": f"{len(registros):,}".replace(",", ".")},
            {"label":"Médicos distintos",    "value": f"{len(pros):,}".replace(",", ".")},
            {"label":"Pares (atendimento, data)", "value": f"{len(pares):,}".replace(",", ".")},
        ])

        st.subheader("👨‍⚕️ Seleção de médicos")

        colsel1, colsel2 = st.columns([1, 3])
        with colsel1:
            import_all = st.checkbox("Importar todos os médicos", value=st.session_state["import_all_docs"])
        with colsel2:
            if import_all:
                st.info("Todos os médicos do arquivo serão importados.")
                selected_pros = pros[:]
            else:
                default_pre = sorted([p for p in pros if p in ALWAYS_SELECTED_PROS])
                selected_pros = st.multiselect(
                    "Médicos a importar (os da lista fixa sempre serão incluídos na gravação):",
                    options=pros,
                    default=st.session_state["import_selected_docs"] or default_pre,
                )

        st.session_state["import_all_docs"] = import_all
        st.session_state["import_selected_docs"] = selected_pros

        # Lista final: seleção ∪ (sempre-incluídos presentes no arquivo)
        always_in_file = [p for p in pros if p in ALWAYS_SELECTED_PROS]
        final_pros = sorted(set(selected_pros if not import_all else pros).union(always_in_file))

        st.caption(f"Médicos fixos (sempre incluídos, quando presentes): {', '.join(sorted(ALWAYS_SELECTED_PROS))}")
        st.info(f"Médicos considerados: {', '.join(final_pros) if final_pros else '(nenhum)'}")

        # Filtra registros
        registros_filtrados = registros[:] if import_all else [r for r in registros if (r.get("profissional") or "") in final_pros]

        # Prévia DRY RUN
        df_preview = pd.DataFrame(registros_filtrados)
        st.subheader("Pré-visualização (DRY RUN) — nada foi gravado ainda")
        st.dataframe(df_preview, use_container_width=True, hide_index=True)

        # Pares (att, data) após filtros
        pares = sorted({(r["atendimento"], r["data"]) for r in registros_filtrados if r.get("atendimento") and r.get("data")})
        st.markdown(
            f"<div>🔎 {len(pares)} par(es) (atendimento, data) após filtros. Regra: "
            f"{pill('1 auto por internação/dia')} (manuais podem ser vários).</div>",
            unsafe_allow_html=True
        )

        colg1, colg2 = st.columns([1, 4])
        with colg1:
            if st.button("Gravar no banco", type="primary"):
                total_criados = total_ignorados = total_internacoes = 0

                for (att, data_proc) in pares:
                    if not att: continue
                    df_int = get_internacao_by_atendimento(att)
                    if df_int.empty:
                        itens_att = [r for r in registros_filtrados if r["atendimento"] == att]
                        paciente = next((x.get("paciente") for x in itens_att if x.get("paciente")), "") if itens_att else ""
                        conv_total = next((x.get("convenio") for x in itens_att if x.get("convenio")), "") if itens_att else ""
                        data_int = next((x.get("data") for x in itens_att if x.get("data")), data_proc)
                        internacao_id = criar_internacao(hospital, att, paciente, data_int, conv_total)
                        total_internacoes += 1
                    else:
                        internacao_id = int(df_int["id"].iloc[0])

                    prof_dia = ""; aviso_dia = ""
                    for it in registros_filtrados:
                        if it["atendimento"] == att and it["data"] == data_proc:
                            if not prof_dia and it.get("profissional"): prof_dia = it["profissional"]
                            if not aviso_dia and it.get("aviso"): aviso_dia = it["aviso"]
                            if prof_dia and aviso_dia: break

                    if not prof_dia:
                        total_ignorados += 1; continue
                    if existe_procedimento_no_dia(internacao_id, data_proc):
                        total_ignorados += 1; continue

                    criar_procedimento(
                        internacao_id, data_proc, prof_dia,
                        procedimento="Cirurgia / Procedimento", situacao="Pendente",
                        observacao=None, is_manual=0, aviso=aviso_dia or None, grau_participacao=None
                    )
                    total_criados += 1

                st.success(f"Concluído! Internações criadas: {total_internacoes} | Automáticos criados: {total_criados} | Ignorados: {total_ignorados}")
                st.toast("✅ Importação concluída.", icon="✅")
                maybe_sync_up_db("chore(db): importação")

    st.markdown("</div>", unsafe_allow_html=True)

# ============================================================
# 🔍 2) CONSULTAR (sem cadastro manual) + edição + exclusões + ver quitação
# ============================================================

with tabs[2]:
    st.subheader("🔍 Consultar Internação")

    st.markdown("<div class='soft-card'>", unsafe_allow_html=True)
    hlist = ["Todos"] + get_hospitais()
    filtro_hosp = st.selectbox("Filtrar hospital (consulta):", hlist)            
    codigo = st.text_input(
        "Digite o atendimento para consultar:",
        key="consulta_codigo",
        placeholder="Ex.: 123456",
        label_visibility="visible",
    )
    st.markdown("</div>", unsafe_allow_html=True)

    if codigo:
        df_int = get_internacao_by_atendimento(codigo)
        if filtro_hosp != "Todos":
            df_int = df_int[df_int["hospital"] == filtro_hosp]

        if df_int.empty:
            st.warning("Nenhuma internação encontrada.")
        else:
            st.subheader("Dados da internação")
            st.dataframe(df_int, use_container_width=True, hide_index=True)
            internacao_id = int(df_int["id"].iloc[0])

            
            # ============================
            # ✏️ Edição dos dados da internação
            # ============================
            
            st.subheader("✏️ Editar dados da internação")
            
            with st.container():
                c1, c2, c3, c4 = st.columns(4)
            
                with c1:
                    novo_paciente = st.text_input("Paciente:", value=df_int["paciente"].iloc[0])
            
                with c2:
                    novo_convenio = st.text_input("Convênio:", value=df_int["convenio"].iloc[0])
            
                with c3:
                    data_atual = df_int["data_internacao"].iloc[0]
                    try:
                        dt_atual = datetime.strptime(data_atual, "%d/%m/%Y").date()
                    except:
                        dt_atual = date.today()
            
                    nova_data = st.date_input("Data da internação:", value=dt_atual)
            
                with c4:
                    todos_hospitais = get_hospitais(include_inactive=True)
                    novo_hospital = st.selectbox("Hospital:", todos_hospitais, index=todos_hospitais.index(df_int["hospital"].iloc[0]))
            
                col_save_int = st.columns(6)[-1]
                with col_save_int:
                    if st.button("💾 Salvar alterações da internação", type="primary"):
                        atualizar_internacao(
                            internacao_id,
                            paciente=novo_paciente,
                            convenio=novo_convenio,
                            data_internacao=nova_data.strftime("%d/%m/%Y"),
                            hospital=novo_hospital
                        )
            
                        st.toast("Dados da internação atualizados!", icon="✅")
                        maybe_sync_up_db("chore(db): edição de internação")
                        st.rerun()

            

            # Excluir internação
            with st.expander("🗑️ Excluir esta internação"):
                st.warning("Esta ação apagará a internação e TODOS os procedimentos vinculados.")
                confirm_txt = st.text_input("Digite APAGAR para confirmar", key="confirm_del_int")
                col_del = st.columns(6)[-1]
                with col_del:
                    if st.button("Excluir internação", key="btn_del_int"):
                        if confirm_txt.strip().upper() == "APAGAR":
                            deletar_internacao(internacao_id)
                            st.toast("🗑️ Internação excluída.", icon="✅")
                            maybe_sync_up_db("chore(db): exclusão de internação")
                            st.rerun()
                        else:
                            st.info("Confirmação inválida. Digite APAGAR.")

            # Procedimentos
            conn = get_conn()
            df_proc = pd.read_sql_query(
                """
                SELECT id, data_procedimento, profissional, procedimento, situacao, observacao, aviso, grau_participacao
                FROM Procedimentos
                WHERE internacao_id = ?
                """,
                conn, params=(internacao_id,)
            )
            conn.close()
            
            # Garantias de colunas e preenchimentos
            if "procedimento" not in df_proc.columns:
                df_proc["procedimento"] = "Cirurgia / Procedimento"
            
            df_proc["procedimento"]       = df_proc["procedimento"].fillna("Cirurgia / Procedimento")
            df_proc["situacao"]           = df_proc.get("situacao", pd.Series(dtype=str)).fillna("Pendente")
            df_proc["observacao"]         = df_proc.get("observacao", pd.Series(dtype=str)).fillna("")
            df_proc["aviso"]              = df_proc.get("aviso", pd.Series(dtype=str)).fillna("")
            df_proc["grau_participacao"]  = df_proc.get("grau_participacao", pd.Series(dtype=str)).fillna("")
            
            # === ORDENAR POR DATA DO ATENDIMENTO (data_procedimento) ===
            # 1) Converte a data em datetime.date (formato pt-BR)
            def _safe_pt_date(s):
                try:
                    return datetime.strptime(str(s).strip(), "%d/%m/%Y").date()
                except Exception:
                    try:
                        # fallback caso venha em ISO
                        return datetime.strptime(str(s).strip(), "%Y-%m-%d").date()
                    except Exception:
                        return None
            
            df_proc["_data_dt"] = df_proc["data_procedimento"].apply(_safe_pt_date)
            
            # 2) Ordena por data (e por id para estabilizar)
            #    -> ascending=True para mais antigos primeiro (mude para False se quiser mais recentes no topo)
            df_proc = df_proc.sort_values(by=["_data_dt", "id"], ascending=[True, True]).reset_index(drop=True)
            
            # 3) Reformatar de volta para dd/mm/yyyy (somente para exibição)
            df_proc["data_procedimento"] = df_proc["_data_dt"].apply(lambda d: d.strftime("%d/%m/%Y") if pd.notna(d) else "")
            
            # 4) Remove auxiliar
            df_proc = df_proc.drop(columns=["_data_dt"])
            
            # (daqui pra baixo, mantenha o que você já tem:)
            st.subheader("Procedimentos — Editáveis")
            edited = st.data_editor(
                df_proc,
                key="editor_proc",
                use_container_width=True, hide_index=True,
                column_config={
                    "id": st.column_config.Column("ID", disabled=True),
                    "data_procedimento": st.column_config.Column("Data", disabled=True),
                    "profissional": st.column_config.Column("Profissional", disabled=True),
            
                    "aviso": st.column_config.TextColumn("Aviso"),
                    "grau_participacao": st.column_config.SelectboxColumn(
                        "Grau de Participação",
                        options=[""] + GRAU_PARTICIPACAO_OPCOES,
                        required=False
                    ),
                    "procedimento": st.column_config.SelectboxColumn(
                        "Tipo de Procedimento",
                        options=PROCEDIMENTO_OPCOES,
                        required=True
                    ),
                    "situacao": st.column_config.SelectboxColumn(
                        "Situação",
                        options=STATUS_OPCOES,
                        required=True
                    ),
                    "observacao": st.column_config.TextColumn("Observações"),
                },
            )



            col_save = st.columns(6)[-1]
            with col_save:
                if st.button("💾 Salvar alterações", key="btn_save_proc", type="primary"):
                    cols_chk = ["procedimento", "situacao", "observacao", "grau_participacao", "aviso"]
                    df_compare = df_proc[["id"] + cols_chk].merge(edited[["id"] + cols_chk], on="id", suffixes=("_old", "_new"))
                    alterados = []
                    for _, row in df_compare.iterrows():
                        changed = any((str(row[c + "_old"] or "") != str(row[c + "_new"] or "")) for c in cols_chk)
                        if changed:                            
                            alterados.append({
                                "id": int(row["id"]),
                                "procedimento": row["procedimento_new"],
                                "situacao": row["situacao_new"],
                                "observacao": row["observacao_new"],
                                "grau_participacao": (
                                    row["grau_participacao_new"] if row["grau_participacao_new"] != "" else None
                                ),
                                "aviso": row["aviso_new"],   #  <<<<<< ADICIONADO
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
                                aviso=item.get("aviso"),   #  <<<<<< USAR .get
                            )
                            
                        st.toast(f"{len(alterados)} procedimento(s) atualizado(s).", icon="✅")
                        maybe_sync_up_db("chore(db): edição de procedimentos")
                        st.rerun()

            # Excluir cirurgia/procedimento
            with st.expander("🗑️ Excluir cirurgia (procedimento)"):
                if df_proc.empty:
                    st.info("Não há procedimentos para excluir.")
                else:
                    for _, r in df_proc.iterrows():
                        c1, c2, c3, c4 = st.columns([3, 3, 3, 2])
                        with c1: st.markdown(f"**ID:** {int(r['id'])}  —  **Data:** {r['data_procedimento']}")
                        with c2: st.markdown(f"**Profissional:** {r['profissional'] or '-'}")
                        with c3: st.markdown(f"**Tipo:** {r['procedimento']}<br>{pill(r['situacao'])}", unsafe_allow_html=True)
                        with c4:
                            if st.button("Excluir", key=f"del_proc_{int(r['id'])}", help="Apagar este procedimento"):
                                deletar_procedimento(int(r["id"]))
                                st.toast(f"Procedimento {int(r['id'])} excluído.", icon="🗑️")
                                maybe_sync_up_db("chore(db): exclusão de procedimento")
                                st.rerun()

            # Lançar manual
            st.divider()
            st.subheader("➕ Lançar procedimento manual (permite vários no mesmo dia)")
            c1, c2, c3 = st.columns(3)
            with c1: data_proc = st.date_input("Data do procedimento", value=date.today())            
            with c2:
                # Buscar lista de profissionais existentes no sistema
                conn = get_conn()
                df_pros = pd.read_sql_query(
                    "SELECT DISTINCT profissional FROM Procedimentos WHERE profissional IS NOT NULL AND TRIM(profissional) <> '' ORDER BY profissional",
                    conn
                )
                conn.close()
                lista_profissionais = df_pros["profissional"].tolist()
            
                # Campo agora é um selectbox, não mais texto livre
                profissional = st.selectbox(
                    "Profissional",
                    ["(selecione)"] + lista_profissionais,
                    index=0
                )
            with c3: situacao = st.selectbox("Situação", STATUS_OPCOES, index=0)

            colp1, colp2, colp3 = st.columns(3)
            with colp1: procedimento_tipo = st.selectbox("Tipo de Procedimento", PROCEDIMENTO_OPCOES, index=0)
            with colp2: observacao = st.text_input("Observações (opcional)")
            with colp3: grau_part = st.selectbox("Grau de Participação", [""] + GRAU_PARTICIPACAO_OPCOES, index=0)

            col_add = st.columns(6)[-1]
            with col_add:    
                
                # Data da internação já carregada do banco
                data_internacao_str = df_int["data_internacao"].iloc[0]
                try:
                    dt_internacao = datetime.strptime(data_internacao_str, "%d/%m/%Y").date()
                except:
                    dt_internacao = date.today()   # fallback seguro
                
                if st.button("Adicionar procedimento", key="btn_add_manual", type="primary"):
                
                    # ⚠️ VALIDAÇÃO (sem st.stop)
                    if data_proc < dt_internacao:
                        st.error("❌ A data do procedimento não pode ser anterior à data da internação.")
                    else:
                        data_str = data_proc.strftime("%d/%m/%Y")
                                        
                        if profissional == "(selecione)":
                            st.error("Selecione um profissional.")
                        else:
                            criar_procedimento(
                                internacao_id, data_str, profissional, procedimento_tipo,
                                situacao=situacao,
                                observacao=(observacao or None),
                                is_manual=1,
                                aviso=None,
                                grau_participacao=(grau_part if grau_part != "" else None),
                            )

                
                        st.toast("Procedimento (manual) adicionado.", icon="✅")
                        maybe_sync_up_db("chore(db): novo procedimento manual")
                        st.rerun()

            # Ver quitação (Finalizados)
            st.divider()
            st.subheader("🔎 Quitações desta internação (somente Finalizados)")
            finalizados = df_proc[df_proc["situacao"] == "Finalizado"]
            if finalizados.empty:
                st.info("Não há procedimentos finalizados nesta internação.")
            else:
                for _, r in finalizados.iterrows():
                    colA, colB, colC, colD = st.columns([2, 2, 2, 2])
                    with colA: st.markdown(f"**Data:** {r['data_procedimento']}")
                    with colB: st.markdown(f"**Profissional:** {r['profissional'] or '-'}")
                    with colC: st.markdown(f"**Aviso:** {r['aviso'] or '-'}")
                    with colD:
                        if st.button("Ver quitação", key=f"verquit_{int(r['id'])}"):
                            st.session_state["show_quit_id"] = int(r["id"])

                if "show_quit_id" in st.session_state and st.session_state["show_quit_id"]:
                    pid = int(st.session_state["show_quit_id"]); df_q = get_quitacao_by_proc_id(pid)
                    if not df_q.empty:
                        q = df_q.iloc[0]
                        total = (q["quitacao_valor_amhptiss"] or 0) + (q["quitacao_valor_complemento"] or 0)
                        st.markdown("---"); st.markdown("### 🧾 Detalhes da quitação")
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
                            st.markdown(f"**Status:** {pill(q['situacao'])}", unsafe_allow_html=True)
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

                        st.markdown("**Observações da quitação:**")
                        st.write(q["quitacao_observacao"] or "-")

                        
                        # ============================================
                        # BOTÕES — FECHAR e REVERTER QUITAÇÃO
                        # ============================================
                        
                        cbot1, cbot2 = st.columns(2)
                        
                        with cbot1:
                            if st.button("Fechar", key="fechar_quit"):
                                st.session_state["show_quit_id"] = None
                                st.rerun()
                        
                        with cbot2:
                            if st.button("↩️ Reverter quitação", key=f"rev_{pid}", type="secondary"):
                                reverter_quitacao(pid)
                                st.toast(
                                    "Quitação revertida. Status voltou para 'Enviado para pagamento'.",
                                    icon="↩️"
                                )
                                maybe_sync_up_db("chore(db): revertido quitação")
                                st.session_state["show_quit_id"] = None
                                st.rerun()




# ============================================================
# 📑 3) RELATÓRIOS (PDF)
# ============================================================

# --- PDF: Cirurgias por Status (paisagem) ---
if REPORTLAB_OK:   
    
    def _pdf_cirurgias_por_status(df, filtros):
        buf = io.BytesIO()
        doc = SimpleDocTemplate(
            buf,
            pagesize=landscape(A4),
            leftMargin=18, rightMargin=18, topMargin=18, bottomMargin=18
        )
    
        styles = getSampleStyleSheet()
        H1 = styles["Heading1"]; H2 = styles["Heading2"]; N = styles["BodyText"]
    
        # Estilos de célula com quebra de linha
        from reportlab.lib.styles import ParagraphStyle
        from reportlab.platypus import Paragraph
    
        TH = ParagraphStyle(
            "TH",
            parent=styles["Normal"],
            fontName="Helvetica-Bold",
            fontSize=9,
            leading=11,
            alignment=1,          # CENTER
            spaceBefore=0,
            spaceAfter=0,
        )
        TD = ParagraphStyle(
            "TD",
            parent=styles["Normal"],
            fontName="Helvetica",
            fontSize=8,
            leading=10,
            wordWrap="LTR",       # permite quebra de linha dentro da célula
        )
        TD_CENTER = ParagraphStyle(**{**TD.__dict__, "alignment":1})
        TD_RIGHT  = ParagraphStyle(**{**TD.__dict__, "alignment":2})
    
        elems = []
        elems.append(Paragraph("Relatório — Cirurgias por Status", H1))
        elems.append(Spacer(1, 6))
    
        filtros_txt = (f"Período: {filtros['ini']} a {filtros['fim']}  |  "
                       f"Hospital: {filtros['hospital']}  |  "
                       f"Status: {filtros['status']}")
        elems.append(Paragraph(filtros_txt, N))
        elems.append(Spacer(1, 8))
    
        total = len(df)
        elems.append(Paragraph(f"Total de cirurgias: <b>{total}</b>", H2))
    
        # Resumo por situação (opcional)
        if total > 0 and filtros["status"] == "Todos":
            resumo = (df.groupby("situacao")["situacao"]
                        .count()
                        .sort_values(ascending=False)
                        .reset_index(name="qtd"))
            data_resumo = [["Situação", "Quantidade"]] + resumo.values.tolist()
            t_res = Table(data_resumo, hAlign="LEFT")
            t_res.setStyle(TableStyle([
                ("BACKGROUND", (0,0), (-1,0), colors.HexColor("#F0F0F0")),
                ("GRID", (0,0), (-1,-1), 0.25, colors.grey),
                ("ALIGN", (1,1), (-1,-1), "RIGHT"),
                ("FONTNAME", (0,0), (-1,0), "Helvetica-Bold"),
                ("FONTSIZE", (0,0), (-1,0), 9),
            ]))
            elems.append(t_res)
            elems.append(Spacer(1, 10))
    
        # ======= TABELA PRINCIPAL =======
        # Nova ordem no final: ... "Hospital", "Situação"
        header_labels = [
            "Atendimento", "Aviso", "Convênio", "Paciente",
            "Data", "Tipo", "Profissional", "Grau de Participação", "Hospital", "Situação"
        ]
        header = [Paragraph(h, TH) for h in header_labels]
    
        # Larguras balanceadas p/ A4 paisagem (26,1 cm úteis aprox. com suas margens):
        col_widths = [
            2.6*cm,  # Atendimento
            2.0*cm,  # Aviso
            2.8*cm,  # Convênio
            5.0*cm,  # Paciente
            2.2*cm,  # Data
            2.4*cm,  # Tipo
            2.8*cm,  # Profissional
            3.0*cm,  # Grau de Participação
            2.6*cm,  # Hospital
            2.1*cm,  # Situação (curto, cabe nomes definidos)
        ]
    
        def _p(v, style=TD):
            txt = "" if v is None else str(v)
            return Paragraph(txt, style)
    
        data_rows = []
        for _, r in df.iterrows():
            data_rows.append([
                _p(r.get("atendimento"), TD_CENTER),
                _p(r.get("aviso"), TD_CENTER),
                _p(r.get("convenio")),
                _p(r.get("paciente")),
                _p(r.get("data_procedimento"), TD_CENTER),
                _p(r.get("procedimento")),
                _p(r.get("profissional")),
                _p(r.get("grau_participacao"), TD_CENTER),
                _p(r.get("hospital")),
                _p(r.get("situacao"), TD_CENTER),
            ])
    
        table = Table([header] + data_rows, repeatRows=1, colWidths=col_widths)
        table.setStyle(TableStyle([
            ("BACKGROUND", (0,0), (-1,0), colors.HexColor("#E8EEF7")),
            ("GRID", (0,0), (-1,-1), 0.25, colors.grey),
            ("FONTNAME", (0,0), (-1,0), "Helvetica-Bold"),
            ("FONTSIZE", (0,0), (-1,0), 9),
            ("VALIGN", (0,0), (-1,-1), "TOP"),
            ("ROWBACKGROUNDS", (0,1), (-1,-1), [colors.white, colors.HexColor("#FAFAFA")]),
            ("ALIGN", (0,0), (-1,0), "CENTER"),  # cabeçalho centralizado
        ]))
        elems.append(table)
    
        doc.build(elems)
        pdf_bytes = buf.getvalue()
        buf.close()
        return pdf_bytes

else:
    def _pdf_cirurgias_por_status(*args, **kwargs):
        raise RuntimeError("ReportLab não está instalado no ambiente.")

# --- PDF: Quitações (paisagem, com PROFISSIONAL, larguras e totais) ---
if REPORTLAB_OK:
    def _pdf_quitacoes(df, filtros):
        # Totais
        v_amhp = pd.to_numeric(df.get("quitacao_valor_amhptiss", 0), errors="coerce").fillna(0.0)
        v_comp = pd.to_numeric(df.get("quitacao_valor_complemento", 0), errors="coerce").fillna(0.0)
        total_amhp = float(v_amhp.sum())
        total_comp = float(v_comp.sum())
        total_geral = total_amhp + total_comp

        buf = io.BytesIO()
        doc = SimpleDocTemplate(
            buf, pagesize=landscape(A4), leftMargin=18, rightMargin=18, topMargin=18, bottomMargin=18
        )
        styles = getSampleStyleSheet()
        H1 = styles["Heading1"]; N = styles["BodyText"]

        elems = []
        elems.append(Paragraph("Relatório — Quitações", H1))
        filtros_txt = (f"Período da quitação: {filtros['ini']} a {filtros['fim']}  |  "
                       f"Hospital: {filtros['hospital']}")
        elems.append(Paragraph(filtros_txt, N)); elems.append(Spacer(1, 8))

        header = [
            "Convênio", "Paciente", "Profissional", "Data", "Atendimento",
            "Guia AMHP", "Guia Complemento",
            "Valor AMHP", "Valor Complemento", "Data da quitação"
        ]
        col_widths = [
            3.2*cm, 6.0*cm, 6.0*cm, 2.4*cm, 2.8*cm,
            3.2*cm, 3.6*cm, 3.2*cm, 3.6*cm, 2.8*cm
        ]
        numeric_cols = [7, 8]
        center_cols  = [3, 4, 9]

        data_rows = []
        for _, r in df.iterrows():
            data_rows.append([
                r.get("convenio") or "",
                r.get("paciente") or "",
                r.get("profissional") or "",
                r.get("data_procedimento") or "",
                r.get("atendimento") or "",
                r.get("quitacao_guia_amhptiss") or "",
                r.get("quitacao_guia_complemento") or "",
                _format_currency_br(r.get("quitacao_valor_amhptiss")),
                _format_currency_br(r.get("quitacao_valor_complemento")),
                r.get("quitacao_data") or "",
            ])

        table = Table([header] + data_rows, repeatRows=1, colWidths=col_widths)
        style_cmds = [
            ("BACKGROUND", (0,0), (-1,0), colors.HexColor("#E8EEF7")),
            ("GRID", (0,0), (-1,-1), 0.25, colors.grey),
            ("FONTNAME", (0,0), (-1,0), "Helvetica-Bold"),
            ("FONTSIZE", (0,0), (-1,0), 10),
            ("VALIGN", (0,0), (-1,-1), "TOP"),
            ("ROWBACKGROUNDS", (0,1), (-1,-1), [colors.white, colors.HexColor("#FAFAFA")]),
            ("ALIGN", (0,0), (-1,0), "CENTER"),
        ]
        for c in numeric_cols:
            style_cmds.append(("ALIGN", (c,1), (c,-1), "RIGHT"))
        for c in center_cols:
            style_cmds.append(("ALIGN", (c,1), (c,-1), "CENTER"))

        table.setStyle(TableStyle(style_cmds))
        elems.append(table); elems.append(Spacer(1, 8))

        totals_data = [
            ["Total AMHP:", _format_currency_br(total_amhp)],
            ["Total Complemento:", _format_currency_br(total_comp)],
            ["Total Geral:", _format_currency_br(total_geral)],
        ]
        totals_tbl = Table(totals_data, colWidths=[4.5*cm, 3.5*cm], hAlign="RIGHT")
        totals_tbl.setStyle(TableStyle([
            ("FONTNAME", (0,0), (-1,-1), "Helvetica"),
            ("FONTSIZE", (0,0), (-1,-1), 10),
            ("ALIGN", (0,0), (0,-1), "RIGHT"),
            ("ALIGN", (1,0), (1,-1), "RIGHT"),
            ("BOTTOMPADDING", (0,0), (-1,-1), 4),
            ("TOPPADDING", (0,0), (-1,-1), 2),
        ]))
        elems.append(totals_tbl)

        doc.build(elems)
        pdf_bytes = buf.getvalue(); buf.close()
        return pdf_bytes
else:
    def _pdf_quitacoes(*args, **kwargs):
        raise RuntimeError("ReportLab não está instalado no ambiente.")

with tabs[3]:
    st.subheader("📑 Relatórios — Central")

    # 1) Cirurgias por Status
    st.markdown("**1) Cirurgias por Status (PDF)**")
    hosp_opts = ["Todos"] + get_hospitais()
    colf1, colf2, colf3 = st.columns(3)
    with colf1: hosp_sel = st.selectbox("Hospital", hosp_opts, index=0, key="rel_hosp")
    with colf2: status_sel = st.selectbox("Status", ["Todos"] + STATUS_OPCOES, index=0, key="rel_status")
    with colf3:
        hoje = date.today(); ini_default = hoje.replace(day=1)
        dt_ini = st.date_input("Data inicial", value=ini_default, key="rel_ini")
        dt_fim = st.date_input("Data final", value=hoje, key="rel_fim")

    conn = get_conn()    
    sql_rel = """
        SELECT 
            I.hospital, I.atendimento, I.paciente, I.convenio,
            P.data_procedimento, P.aviso, P.profissional,
            P.procedimento, P.grau_participacao, P.situacao
        FROM Procedimentos P
        INNER JOIN Internacoes I ON I.id = P.internacao_id
        WHERE P.procedimento = 'Cirurgia / Procedimento'
    """

    df_rel = pd.read_sql_query(sql_rel, conn); conn.close()

    if not df_rel.empty:
        df_rel["_data_dt"] = df_rel["data_procedimento"].apply(_pt_date_to_dt)
        mask = (df_rel["_data_dt"].notna()) & (df_rel["_data_dt"] >= dt_ini) & (df_rel["_data_dt"] <= dt_fim)
        df_rel = df_rel[mask].copy()
        if hosp_sel != "Todos": df_rel = df_rel[df_rel["hospital"] == hosp_sel]
        if status_sel != "Todos": df_rel = df_rel[df_rel["situacao"] == status_sel]
        df_rel = df_rel.sort_values(by=["_data_dt", "hospital", "paciente", "atendimento"])
        df_rel["data_procedimento"] = df_rel["_data_dt"].apply(lambda d: d.strftime("%d/%m/%Y") if pd.notna(d) else "")
        df_rel = df_rel.drop(columns=["_data_dt"])

    colb1, colb2 = st.columns(2)
    with colb1:
        if st.button("Gerar PDF", type="primary"):
            if df_rel.empty:
                st.warning("Nenhum registro encontrado para os filtros informados.")
            elif not REPORTLAB_OK:
                st.error("A biblioteca 'reportlab' não está instalada no ambiente.")
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
                st.download_button(label="⬇️ Baixar PDF", data=pdf_bytes, file_name=fname,
                                   mime="application/pdf", use_container_width=True)
    with colb2:
        if not df_rel.empty:
            csv_bytes = df_rel.to_csv(index=False).encode("utf-8-sig")
            st.download_button("⬇️ Baixar CSV (fallback)", data=csv_bytes,
                               file_name=f"cirurgias_por_status_{date.today().strftime('%Y%m%d')}.csv",
                               mime="text/csv")

    st.divider()

    # 2) Quitações
    st.markdown("**2) Quitações (PDF)**")
    hosp_opts_q = ["Todos"] + get_hospitais()
    colq1, colq2 = st.columns(2)
    with colq1:
        hosp_sel_q = st.selectbox("Hospital", hosp_opts_q, index=0, key="rel_q_hosp")
    with colq2:
        hoje = date.today()
        ini_default_q = hoje.replace(day=1)
        dt_ini_q = st.date_input("Data inicial da quitação", value=ini_default_q, key="rel_q_ini")
        dt_fim_q = st.date_input("Data final da quitação", value=hoje, key="rel_q_fim")

    conn = get_conn()
    sql_quit = """
        SELECT 
            I.hospital, I.atendimento, I.paciente, I.convenio,
            P.data_procedimento, P.profissional,
            P.quitacao_data, P.quitacao_guia_amhptiss, P.quitacao_guia_complemento,
            P.quitacao_valor_amhptiss, P.quitacao_valor_complemento
        FROM Procedimentos P
        INNER JOIN Internacoes I ON I.id = P.internacao_id
        WHERE P.procedimento = 'Cirurgia / Procedimento'
          AND P.quitacao_data IS NOT NULL
          AND TRIM(P.quitacao_data) <> ''
    """
    df_quit = pd.read_sql_query(sql_quit, conn)
    conn.close()

    if not df_quit.empty:
        df_quit["_quit_dt"] = df_quit["quitacao_data"].apply(_pt_date_to_dt)
        mask_q = (df_quit["_quit_dt"].notna()) & (df_quit["_quit_dt"] >= dt_ini_q) & (df_quit["_quit_dt"] <= dt_fim_q)
        df_quit = df_quit[mask_q].copy()
        if hosp_sel_q != "Todos":
            df_quit = df_quit[df_quit["hospital"] == hosp_sel_q]

        df_quit = df_quit.sort_values(by=["_quit_dt", "convenio", "paciente"])
        df_quit["data_procedimento"] = df_quit["data_procedimento"].apply(
            lambda s: _pt_date_to_dt(s).strftime("%d/%m/%Y") if pd.notna(_pt_date_to_dt(s)) else (s or "")
        )
        df_quit["quitacao_data"] = df_quit["_quit_dt"].apply(lambda d: d.strftime("%d/%m/%Y") if pd.notna(d) else "")
        df_quit = df_quit.drop(columns=["_quit_dt"])

        cols_pdf = [
            "convenio", "paciente", "profissional", "data_procedimento", "atendimento",
            "quitacao_guia_amhptiss", "quitacao_guia_complemento",
            "quitacao_valor_amhptiss", "quitacao_valor_complemento",
            "quitacao_data"
        ]
        for c in cols_pdf:
            if c not in df_quit.columns: df_quit[c] = ""
        df_quit = df_quit[cols_pdf]

    colqb1, colqb2 = st.columns(2)
    with colqb1:
        if st.button("Gerar PDF (Quitações)", type="primary"):
            if df_quit.empty:
                st.warning("Nenhum registro de quitação encontrado para os filtros informados.")
            else:
                if not REPORTLAB_OK:
                    st.error("A biblioteca 'reportlab' não está instalada no ambiente.")
                else:
                    filtros_q = {
                        "ini": dt_ini_q.strftime("%d/%m/%Y"),
                        "fim": dt_fim_q.strftime("%d/%m/%Y"),
                        "hospital": hosp_sel_q,
                    }
                    pdf_bytes_q = _pdf_quitacoes(df_quit, filtros_q)
                    ts_q = datetime.now().strftime("%Y%m%d_%H%M%S")
                    fname_q = f"relatorio_quitacoes_{ts_q}.pdf"
                    st.success(f"Relatório de Quitações gerado com {len(df_quit)} registro(s).")
                    st.download_button(
                        label="⬇️ Baixar PDF (Quitações)",
                        data=pdf_bytes_q,
                        file_name=fname_q,
                        mime="application/pdf",
                        use_container_width=True
                    )
    with colqb2:
        if not df_quit.empty:
            csv_quit = df_quit.to_csv(index=False).encode("utf-8-sig")
            st.download_button(
                "⬇️ Baixar CSV (Quitações)",
                data=csv_quit,
                file_name=f"quitacoes_{date.today().strftime('%Y%m%d')}.csv",
                mime="text/csv",
            )

# ============================================================
# 💼 4) QUITAÇÃO (com observações)
# ============================================================

with tabs[4]:
    st.subheader("💼 Quitação de Cirurgias")

    st.markdown("<div class='soft-card'>", unsafe_allow_html=True)
    hosp_opts = ["Todos"] + get_hospitais()
    hosp_sel = st.selectbox("Hospital", hosp_opts, index=0, key="quit_hosp")
    st.markdown("</div>", unsafe_allow_html=True)

    conn = get_conn()
    base = """
        SELECT 
            P.id, I.hospital, I.atendimento, I.paciente, I.convenio,
            P.data_procedimento, P.profissional, P.aviso, P.situacao,
            P.quitacao_data, P.quitacao_guia_amhptiss, P.quitacao_valor_amhptiss,
            P.quitacao_guia_complemento, P.quitacao_valor_complemento, P.quitacao_observacao
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
        df_quit["quitacao_data"] = pd.to_datetime(df_quit["quitacao_data"], dayfirst=True, errors="coerce")
        for col in ["quitacao_valor_amhptiss", "quitacao_valor_complemento"]:
            df_quit[col] = pd.to_numeric(df_quit[col], errors="coerce")

        st.markdown("Preencha os dados e clique em **Gravar quitação(ões)**. Ao gravar, o status muda para **Finalizado**.")
        
        edited = st.data_editor(
            df_quit, key="editor_quit", use_container_width=True, hide_index=True,
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

                "quitacao_data": st.column_config.DateColumn("Data da quitação", format="DD/MM/YYYY"),
                "quitacao_guia_amhptiss": st.column_config.TextColumn("Guia AMHPTISS"),
                "quitacao_valor_amhptiss": st.column_config.NumberColumn("Valor Guia AMHPTISS", format="R$ %.2f"),
                "quitacao_guia_complemento": st.column_config.TextColumn("Guia Complemento"),
                "quitacao_valor_complemento": st.column_config.NumberColumn("Valor Guia Complemento", format="R$ %.2f"),
                "quitacao_observacao": st.column_config.TextColumn("Observações da quitação"),
            }
        )

        col_quit = st.columns(6)[-1]
        with col_quit:
            if st.button("💾 Gravar quitação(ões)", type="primary"):
                cols_chk = [
                    "quitacao_data","quitacao_guia_amhptiss","quitacao_valor_amhptiss",
                    "quitacao_guia_complemento","quitacao_valor_complemento","quitacao_observacao",
                ]
                compare = df_quit[["id"] + cols_chk].merge(edited[["id"] + cols_chk], on="id", suffixes=("_old", "_new"))
                atualizados = faltando_data = 0
                for _, row in compare.iterrows():
                    changed = any((str(row[c + "_old"] or "") != str(row[c + "_new"] or "")) for c in cols_chk)
                    if not changed: continue
                    data_q = _to_ddmmyyyy(row["quitacao_data_new"])
                    if not data_q:
                        faltando_data += 1; continue
                    guia_amhp = row["quitacao_guia_amhptiss_new"] or None
                    v_amhp = _to_float_or_none(row["quitacao_valor_amhptiss_new"])
                    guia_comp = row["quitacao_guia_complemento_new"] or None
                    v_comp = _to_float_or_none(row["quitacao_valor_complemento_new"])
                    obs_q = (row["quitacao_observacao_new"] or None)

                    quitar_procedimento(
                        proc_id=int(row["id"]),
                        data_quitacao=data_q, guia_amhptiss=guia_amhp, valor_amhptiss=v_amhp,
                        guia_complemento=guia_comp, valor_complemento=v_comp, quitacao_observacao=obs_q
                    )
                    atualizados += 1

                if faltando_data > 0 and atualizados == 0:
                    st.warning("Nenhuma quitação gravada. Preencha a **Data da quitação** para finalizar.")
                elif faltando_data > 0 and atualizados > 0:
                    st.toast(f"{atualizados} quitação(ões) gravada(s). {faltando_data} linha(s) ignoradas sem **Data da quitação**.", icon="✅")
                    maybe_sync_up_db("chore(db): quitação (lote)")
                    st.rerun()
                else:
                    st.toast(f"{atualizados} quitação(ões) gravada(s).", icon="✅")
                    maybe_sync_up_db("chore(db): quitação")
                    st.rerun()

# ============================================================
# ⚙️ 5) SISTEMA (listas e resumos)
# ============================================================

with tabs[5]:
    st.subheader("⚙️ Sistema")
    
    st.markdown("<div class='soft-card'>", unsafe_allow_html=True)
    st.markdown("**🔒 Persistência de Dados**")
    
    if github_config_ok():
        # Linha com branch/path/repo — exatamente como você quer
        st.caption(
            f"🔗 Persistência **GitHub** ativa — "
            f"branch: `{GH_BRANCH}` • path: `{GH_DB_PATH}` • repo: `{GH_REPO}`"
        )
    else:
        st.caption("💾 Persistência **local** — configure `GH_TOKEN`, `GH_REPO` e `GH_DB_PATH` em *Secrets* para sincronizar com o GitHub.")
    
    # Último status de sync_down_db()
    msg = st.session_state.get("gh_sync_status")
    ts  = st.session_state.get("gh_sync_time")
    if msg:
        st.info(f"{msg}" + (f" (última verificação: {ts})" if ts else ""))
    else:
        # fallback amigável
        st.caption("ℹ️ Ainda não há registro de sincronização nesta sessão.")
    
    st.markdown("</div>", unsafe_allow_html=True)


    st.markdown("**📋 Procedimentos — Lista**")
    filtro = ["Todos"] + get_hospitais()
    chosen = st.selectbox("Hospital (lista de procedimentos):", filtro, key="sys_proc_hosp")

    if st.button("Carregar procedimentos", key="btn_carregar_proc", type="primary"):
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
        st.dataframe(df, use_container_width=True, hide_index=True)

    st.divider()

    st.markdown("**🧾 Resumo por Profissional**")
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
    st.dataframe(df_prof, use_container_width=True, hide_index=True)

    st.divider()

    st.markdown("**💸 Resumo por Convênio**")
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
    st.dataframe(df_conv, use_container_width=True, hide_index=True)

    
# ---- Troca de aba programática (DELAYED — EXECUTA POR ÚLTIMO) ----
if st.session_state.get("goto_tab_label"):
    _switch_to_tab_by_label(st.session_state["goto_tab_label"])
    st.session_state["goto_tab_label"] = None
