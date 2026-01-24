# ============================================================
#  SISTEMA DE INTERNAÇÕES — VERSÃO SUPABASE (Cloud)
#  Visual e fluxo do app "Versão Final" — DB: Supabase
# ============================================================

import streamlit as st
import pandas as pd
from datetime import date, datetime
import io
import json
import re
import streamlit.components.v1 as components

# ==== Supabase ====
from supabase import create_client, Client
from postgrest import APIError

# ==== PDF (ReportLab) - opcional ====
REPORTLAB_OK = True
try:
    from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer
    from reportlab.lib.pagesizes import A4, landscape
    from reportlab.lib import colors
    from reportlab.lib.styles import getSampleStyleSheet
    from reportlab.lib.units import cm
except ModuleNotFoundError:
    REPORTLAB_OK = False

# Parser (seu módulo)
#  -&gt; mantenha o arquivo parser.py no projeto com parse_tiss_original(csv_text) definido.
try:
    from parser import parse_tiss_original
except Exception:
    parse_tiss_original = None

# ============================================================
#  SUPABASE — Conexão
# ============================================================
URL = st.secrets.get("SUPABASE_URL", "")
KEY = st.secrets.get("SUPABASE_KEY", "")
if not URL or not KEY:
    st.error("Configure SUPABASE_URL e SUPABASE_KEY em Secrets para iniciar o app.")
    st.stop()
supabase: Client = create_client(URL, KEY)

def _sb_debug_error(e: APIError, prefix="Erro Supabase"):
    st.error(prefix)
    with st.expander("Detalhes técnicos"):
        st.code(
            f"code: {getattr(e, 'code', None)}\n"
            f"message: {getattr(e, 'message', None)}\n"
            f"details: {getattr(e, 'details', None)}\n"
            f"hint: {getattr(e, 'hint', None)}",
            language="text",
        )

# ============================================================
#  Domínio e Aparência
# ============================================================
STATUS_OPCOES = [
    "Pendente",
    "Não Cobrar",
    "Enviado para pagamento",
    "Aguardando Digitação - AMHP",
    "Finalizado",
]
PROCEDIMENTO_OPCOES = ["Cirurgia / Procedimento", "Parecer"]
GRAU_PARTICIPACAO_OPCOES = ["Cirurgião", "1 Auxiliar", "2 Auxiliar", "3 Auxiliar", "Clínico"]
ALWAYS_SELECTED_PROS = {"JOSE.ADORNO", "CASSIO CESAR", "FERNANDO AND", "SIMAO.MATOS"}

def inject_css():
    st.markdown("""
    &lt;style&gt;
    /* ===== KPIs maiores e centralizados ===== */
    .kpi-wrap.center .kpi{ text-align:center; }
    .kpi.big .label{ font-size: 1.05rem; font-weight: 700; }
    .kpi.big .value{ font-size: 2.4rem; line-height: 2.6rem; font-weight: 800; color: var(--text); }
    .kpi.big .hint{ font-size: .95rem; color: var(--muted); margin-top: 4px;}
    .kpi.big:hover{ box-shadow: 0 1px 0 rgba(0,0,0,.03); }
    .kpi-action button{ font-size: 0.95rem !important; font-weight: 700 !important; }

    :root{ --bg-main:#F5F6F7; --bg-card:#FFFFFF; --border:#D0D7DE; --text:#24292F; --muted:#6B7280; --primary:#1F6FEB; --primary-hover:#1558B0; --radius:8px; }
    html, body, .stApp{ background-color:var(--bg-main)!important; color:var(--text)!important; font-family:"Segoe UI", Roboto, Arial, sans-serif;}
    .app-header{ background:var(--bg-main); padding:10px 12px; margin:-1.2rem -1rem .8rem -1rem; border-bottom:1px solid var(--border); }
    .app-header .title{ font-size:1.2rem; font-weight:700; color:var(--primary); }
    .app-header .sub{ font-size:.9rem; color:var(--muted); }
    .soft-card{ background:var(--bg-card); border:1px solid var(--border); border-radius:var(--radius); padding:14px 16px; margin-bottom:12px; }
    .stTextInput input,.stNumberInput input,.stDateInput input,.stTextArea textarea{ background:#FFF!important;color:var(--text)!important;border:1px solid var(--border)!important;border-radius:var(--radius)!important; box-shadow:none!important;}
    label, .st-emotion-cache-1qg05tj p, .stMarkdown p{ color:var(--muted)!important; }
    div[data-baseweb="select"]{ background:#FFF!important;border:1px solid var(--border)!important;border-radius:var(--radius)!important;color:var(--text)!important;}
    div[data-baseweb="select"] div[role="combobox"]{ background:#FFF!important;color:var(--text)!important;}
    div[data-baseweb="menu"]{ background:#FFF!important;border:1px solid var(--border)!important;border-radius:var(--radius)!important;color:var(--text)!important;}
    div[data-baseweb="option"]{ background:#FFF!important;color:var(--text)!important;}
    div[data-baseweb="option"][aria-selected="true"]{ background:#EEF2FF!important;color:#111827!important;}
    div[data-baseweb="option"]:hover{ background:#F3F4F6!important;}
    .stFileUploader &gt; section{ border:1px solid var(--border)!important; background:#FFF!important;border-radius:var(--radius)!important;}
    .stFileUploader div[role="button"]{ background:#FFF!important;color:var(--text)!important;border:1px solid var(--border)!important;border-radius:var(--radius)!important;}
    .stButton&gt;button{ background:var(--primary)!important;color:#FFF!important;border:none!important;border-radius:var(--radius)!important;padding:6px 16px!important;font-weight:600!important;box-shadow:none!important;}
    .stButton&gt;button:hover{ background:var(--primary-hover)!important; }
    .element-container:has(.stDataFrame) .st-emotion-cache-1wmy9hl,
    .element-container:has(.stDataEditor) .st-emotion-cache-1wmy9hl{
      background:#FFF;border:1px solid var(--border);border-radius:var(--radius);padding-top:6px;
    }
    button[role="tab"][aria-selected="true"]{ border-bottom:2px solid var(--primary)!important; color:var(--text)!important; }
    section[data-testid="stSidebar"] .block-container{ background:var(--bg-main); border-right:1px solid var(--border); }
    .pill{display:inline-block; padding:2px 8px; border-radius:999px; font-size:.8rem; border:1px solid #DDD; background:#F8FAFC}
    .pill-pendente{ background:#FFF7ED; border-color:#FDBA74;}
    .pill-nc{ background:#F3F4F6; border-color:#D1D5DB;}
    .pill-enviado{ background:#EEF2FF; border-color:#C7D2FE;}
    .pill-digitacao{ background:#ECFEFF; border-color:#BAE6FD;}
    .pill-ok{ background:#ECFDF5; border-color:#A7F3D0;}
    &lt;/style&gt;
    """, unsafe_allow_html=True)

def pill(situacao: str) -&gt; str:
    s = (situacao or "").strip()
    cls = "pill"
    if s == "Pendente": cls += " pill-pendente"
    elif s == "Não Cobrar": cls += " pill-nc"
    elif s == "Enviado para pagamento": cls += " pill-enviado"
    elif s == "Aguardando Digitação - AMHP": cls += " pill-digitacao"
    elif s == "Finalizado": cls += " pill-ok"
    return f"&lt;span class='{cls}'&gt;{s or '-'}&lt;/span&gt;"

def kpi_row(items, extra_class: str = ""):
    st.markdown(f"&lt;div class='kpi-wrap {extra_class}'&gt;", unsafe_allow_html=True)
    for it in items:
        st.markdown(
            f"""
            &lt;div class='kpi big'&gt;
              &lt;div class='label'&gt;{it.get('label','')}&lt;/div&gt;
              &lt;div class='value'&gt;{it.get('value','')}&lt;/div&gt;
              { '&lt;div class="hint"&gt;'+it.get('hint','')+'&lt;/div&gt;' if it.get('hint') else '' }
            &lt;/div&gt;
            """,
            unsafe_allow_html=True
        )
    st.markdown("&lt;/div&gt;", unsafe_allow_html=True)

def app_header(title: str, subtitle: str = ""):
    st.markdown(
        f"""
        &lt;div class="app-header"&gt;
            &lt;div class="title"&gt;🏥 {title}&lt;/div&gt;
            &lt;div class="sub"&gt;{subtitle}&lt;/div&gt;
        &lt;/div&gt;
        """,
        unsafe_allow_html=True
    )

# ============================================================
# UTIL (datas, moeda)
# ============================================================
def _pt_date_to_dt(s):
    try:
        return datetime.strptime(str(s), "%d/%m/%Y").date()
    except Exception:
        try:
            return datetime.strptime(str(s), "%Y-%m-%d").date()
        except Exception:
            return None

def _to_ddmmyyyy(value):
    if value is None or value == "": return ""
    if isinstance(value, pd.Timestamp): return value.strftime("%d/%m/%Y")
    if isinstance(value, datetime): return value.strftime("%d/%m/%Y")
    if isinstance(value, date): return value.strftime("%d/%m/%Y")
    for fmt in ("%Y-%m-%d", "%d/%m/%Y"):
        try:
            return datetime.strptime(str(value), fmt).strftime("%d/%m/%Y")
        except Exception:
            pass
    return str(value)

def _to_float_or_none(v):
    if v is None or v == "": return None
    if isinstance(v, (int,float)): return float(v)
    s = re.sub(r"[^\d,.\-]", "", str(v))
    if "," in s and "." in s: s = s.replace(".", "").replace(",", ".")
    elif "," in s:            s = s.replace(",", ".")
    try: return float(s)
    except: return None

def _format_currency_br(v) -&gt; str:
    if v is None or (isinstance(v, float) and pd.isna(v)): return "R$ 0,00"
    try:
        v = float(v); s = f"{v:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
        return f"R$ {s}"
    except Exception:
        return f"R$ {v}"

# ============================================================
# Helper de merge tolerante (evita KeyError com DF/coluna vazios)
# ============================================================
def safe_merge(
    left: pd.DataFrame,
    right: pd.DataFrame,
    left_on: str,
    right_on: str,
    how: str = "left",
    suffixes=("", "_right"),
) -&gt; pd.DataFrame:
    """
    Faz merge sem estourar KeyError quando o 'right' está vazio ou sem a coluna-chave.
    Retorna 'left' intacto se a chave do 'left' não existir.
    """
    if not isinstance(left, pd.DataFrame) or left.empty:
        return left if isinstance(left, pd.DataFrame) else pd.DataFrame()

    if not isinstance(right, pd.DataFrame) or right.empty or (right_on not in right.columns):
        right = pd.DataFrame(columns=[right_on])

    if left_on not in left.columns:
        return left

    try:
        return left.merge(right, left_on=left_on, right_on=right_on, how=how, suffixes=suffixes)
    except KeyError:
        return left

# ============================================================
# CRUD — Supabase (tabelas minúsculas)
# ============================================================
def get_hospitais(include_inactive: bool = False) -&gt; list:
    try:
        query = supabase.table("hospitals").select("name, active")
        if not include_inactive:
            query = query.eq("active", 1)
        res = query.order("name").execute()
        return [r["name"] for r in (res.data or [])]
    except APIError as e:
        _sb_debug_error(e, "Falha ao buscar hospitais.")
        return []

def get_internacao_by_atendimento(att):
    try:
        res = supabase.table("internacoes").select("*").eq("atendimento", str(att)).execute()
        return pd.DataFrame(res.data or [])
    except APIError as e:
        _sb_debug_error(e, "Falha ao consultar internação.")
        return pd.DataFrame()

def criar_internacao(hospital, atendimento, paciente, data, convenio):
    payload = {
        "hospital": hospital,
        "atendimento": str(atendimento),
        "paciente": paciente,
        "data_internacao": _to_ddmmyyyy(data),
        "convenio": convenio,
        "numero_internacao": float(atendimento) if str(atendimento).replace(".","").isdigit() else None
    }
    try:
        res = supabase.table("internacoes").insert(payload).execute()
        row = (res.data or [{}])[0]
        return int(row.get("id"))
    except APIError as e:
        _sb_debug_error(e, "Falha ao criar internação.")
        return None

def atualizar_internacao(internacao_id, **kwargs):
    update_data = {k: v for k, v in kwargs.items() if v is not None}
    if "data_internacao" in update_data:
        update_data["data_internacao"] = _to_ddmmyyyy(update_data["data_internacao"])
    try:
        supabase.table("internacoes").update(update_data).eq("id", int(internacao_id)).execute()
    except APIError as e:
        _sb_debug_error(e, "Falha ao atualizar internação.")

def deletar_internacao(internacao_id: int):
    try:
        supabase.table("procedimentos").delete().eq("internacao_id", int(internacao_id)).execute()
        supabase.table("internacoes").delete().eq("id", int(internacao_id)).execute()
    except APIError as e:
        _sb_debug_error(e, "Falha ao deletar internação.")

def criar_procedimento(internacao_id, data_proc, profissional, procedimento,
                       situacao="Pendente", observacao=None, is_manual=0,
                       aviso=None, grau_participacao=None):
    payload = {
        "internacao_id": int(internacao_id),
        "data_procedimento": _to_ddmmyyyy(data_proc),
        "profissional": profissional,
        "procedimento": procedimento,
        "situacao": situacao or "Pendente",
        "observacao": observacao,
        "is_manual": int(is_manual or 0),
        "aviso": aviso,
        "grau_participacao": grau_participacao,
    }
    try:
        supabase.table("procedimentos").insert(payload).execute()
    except APIError as e:
        _sb_debug_error(e, "Falha ao criar procedimento.")

def existe_procedimento_no_dia(internacao_id, data_proc):
    try:
        res = (
            supabase.table("procedimentos")
            .select("id")
            .eq("internacao_id", int(internacao_id))
            .eq("data_procedimento", _to_ddmmyyyy(data_proc))
            .eq("is_manual", 0)
            .limit(1)
            .execute()
        )
        return len(res.data or []) &gt; 0
    except APIError as e:
        _sb_debug_error(e, "Falha ao verificar existência de procedimento no dia.")
        return False

def atualizar_procedimento(proc_id, procedimento=None, situacao=None,
                           observacao=None, grau_participacao=None, aviso=None):
    update_data = {}
    if procedimento is not None: update_data["procedimento"] = procedimento
    if situacao is not None: update_data["situacao"] = situacao
    if observacao is not None: update_data["observacao"] = observacao
    if grau_participacao is not None: update_data["grau_participacao"] = grau_participacao
    if aviso is not None: update_data["aviso"] = aviso
    if not update_data: return
    try:
        supabase.table("procedimentos").update(update_data).eq("id", int(proc_id)).execute()
    except APIError as e:
        _sb_debug_error(e, "Falha ao atualizar procedimento.")

def deletar_procedimento(proc_id: int):
    try:
        supabase.table("procedimentos").delete().eq("id", int(proc_id)).execute()
    except APIError as e:
        _sb_debug_error(e, "Falha ao deletar procedimento.")

def quitar_procedimento(proc_id, data_quitacao=None, guia_amhptiss=None, valor_amhptiss=None,
                        guia_complemento=None, valor_complemento=None, quitacao_observacao=None):
    update_data = {
        "quitacao_data": _to_ddmmyyyy(data_quitacao) if data_quitacao else None,
        "quitacao_guia_amhptiss": guia_amhptiss,
        "quitacao_valor_amhptiss": valor_amhptiss,
        "quitacao_guia_complemento": guia_complemento,
        "quitacao_valor_complemento": valor_complemento,
        "quitacao_observacao": quitacao_observacao,
        "situacao": "Finalizado",
    }
    update_data = {k:v for k,v in update_data.items() if v is not None or k=="situacao"}
    try:
        supabase.table("procedimentos").update(update_data).eq("id", int(proc_id)).execute()
    except APIError as e:
        _sb_debug_error(e, "Falha ao quitar procedimento.")

def reverter_quitacao(proc_id: int):
    update_data = {
        "quitacao_data": None,
        "quitacao_guia_amhptiss": None,
        "quitacao_valor_amhptiss": None,
        "quitacao_guia_complemento": None,
        "quitacao_valor_complemento": None,
        "quitacao_observacao": None,
        "situacao": "Enviado para pagamento",
    }
    try:
        supabase.table("procedimentos").update(update_data).eq("id", int(proc_id)).execute()
    except APIError as e:
        _sb_debug_error(e, "Falha ao reverter quitação.")

def get_procedimentos(internacao_id):
    try:
        res = supabase.table("procedimentos").select("*").eq("internacao_id", int(internacao_id)).execute()
        return pd.DataFrame(res.data or [])
    except APIError as e:
        _sb_debug_error(e, "Falha ao listar procedimentos.")
        return pd.DataFrame()

def get_quitacao_by_proc_id(proc_id: int):
    """Retorna Procedimento + Internação (merge em pandas, sem embed)."""
    try:
        r1 = supabase.table("procedimentos").select("*").eq("id", int(proc_id)).limit(1).execute()
        dfp = pd.DataFrame(r1.data or [])
        if dfp.empty: return dfp
        iid = int(dfp["internacao_id"].iloc[0])
        r2 = supabase.table("internacoes").select("*").eq("id", iid).limit(1).execute()
        dfi = pd.DataFrame(r2.data or [])
        if dfi.empty: return dfp
        df = safe_merge(dfp, dfi, left_on="internacao_id", right_on="id", how="left", suffixes=("", "_int"))
        return df
    except APIError as e:
        _sb_debug_error(e, "Falha ao consultar quitação.")
        return pd.DataFrame()

# ============================================================
# INICIALIZAÇÃO UI
# ============================================================
st.set_page_config(page_title="Gestão de Internações", page_icon="🏥", layout="wide")
inject_css()
app_header("Sistema de Internações — Supabase",
           "Importação, edição, quitação e relatórios (banco em nuvem)")

def _switch_to_tab_by_label(tab_label: str):
    """
    Clica na aba cujo rótulo visível contém `tab_label` (match por substring).
    Usa JSON para injetar a string com segurança e evita f-string no JS.
    """
    js = """
    &lt;script&gt;
    (function(){
      const target = __TAB_LABEL__;
      const norm = (s) =&gt; (s || "").replace(/\\s+/g, " ").trim();

      let attempts = 0;
      const maxAttempts = 20;  // 20 * 100ms = 2s
      const timer = setInterval(() =&gt; {
        attempts++;
        const tabs = window.parent.document.querySelectorAll('button[role="tab"]');
        for (const t of tabs) {
          const txt = norm(t.textContent || t.innerText);
          if (txt.includes(norm(target))) {
            t.click();
            clearInterval(timer);
            return;
          }
        }
        if (attempts &gt;= maxAttempts) {
          clearInterval(timer);
          console.warn("Tab não encontrada para:", target);
        }
      }, 100);
    })();
    &lt;/script&gt;
    """
    js = js.replace("__TAB_LABEL__", json.dumps(tab_label))
    components.html(js, height=0, width=0)

tabs = st.tabs([
    "🏠 Início",
    "📤 Importar Arquivo",
    "🔍 Consultar Internação",
    "📑 Relatórios",
    "💼 Quitação",
    "⚙️ Sistema",
])

# ============================================================
# 🏠 0) INÍCIO
# ============================================================
with tabs[0]:
    st.subheader("🏠 Tela Inicial")

    if "home_status" not in st.session_state:
        st.session_state["home_status"] = None

    hoje = date.today()
    ini_mes = hoje.replace(day=1)

    colf1, colf2 = st.columns([2,3])
    with colf1:
        filtro_hosp_home = st.selectbox("Hospital", ["Todos"] + get_hospitais(), index=0, key="home_f_hosp")
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

    # ------ Carrega Procedimentos + Internações (2 passos, sem embed) ------
    try:
        res_p = supabase.table("procedimentos").select(
            "id, internacao_id, data_procedimento, procedimento, profissional, situacao, aviso, grau_participacao"
        ).execute()
        df_p = pd.DataFrame(res_p.data or [])
        if df_p.empty:
            df_all = pd.DataFrame(columns=[
                "internacao_id","atendimento","paciente","hospital","convenio","data_internacao",
                "id","data_procedimento","procedimento","profissional","situacao","aviso","grau_participacao"
            ])
        else:
            ids = sorted(set(int(x) for x in df_p["internacao_id"].dropna().tolist()))
            if ids:
                res_i = supabase.table("internacoes").select(
                    "id, atendimento, paciente, hospital, convenio, data_internacao"
                ).in_("id", ids).execute()
                df_i = pd.DataFrame(res_i.data or [])
            else:
                df_i = pd.DataFrame(columns=["id","atendimento","paciente","hospital","convenio","data_internacao"])
            df_all = safe_merge(
                df_p,
                df_i[["id", "atendimento", "paciente", "hospital", "convenio", "data_internacao"]] if not df_i.empty else df_i,
                left_on="internacao_id",
                right_on="id",
                how="left",
                suffixes=("", "_int"),
            )
    except APIError as e:
        _sb_debug_error(e, "Falha ao carregar dados para a Home.")
        df_all = pd.DataFrame()

    # Filtros
    if df_all.empty:
        df_f = df_all.copy()
    else:
        def _safe_pt_date(s):
            try:
                return datetime.strptime(str(s).strip(), "%d/%m/%Y").date()
            except Exception:
                try:
                    return datetime.strptime(str(s).strip(), "%Y-%m-%d").date()
                except Exception:
                    return None

        df_all["_int_dt"]  = df_all["data_internacao"].apply(_safe_pt_date)
        df_all["_proc_dt"] = df_all["data_procedimento"].apply(_safe_pt_date)

        mask = pd.Series([True]*len(df_all), index=df_all.index)

        if filtro_hosp_home != "Todos":
            mask &amp;= (df_all["hospital"] == filtro_hosp_home)

        if use_int_range:
            mask &amp;= df_all["_int_dt"].notna()
            mask &amp;= (df_all["_int_dt"] &gt;= st.session_state["home_f_int_ini"])
            mask &amp;= (df_all["_int_dt"] &lt;= st.session_state["home_f_int_fim"])

        if use_proc_range:
            mask &amp;= df_all["_proc_dt"].notna()
            mask &amp;= (df_all["_proc_dt"] &gt;= st.session_state["home_f_proc_ini"])
            mask &amp;= (df_all["_proc_dt"] &lt;= st.session_state["home_f_proc_fim"])

        df_f = df_all[mask].copy()

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
        kpi_row([{"label":"Pendentes", "value": f"{tot_pendente}", "hint": "Todos os procedimentos"}], extra_class="center")
        lbl = "🔽 Esconder Pendentes" if active == "Pendente" else "👁️ Ver Pendentes"
        st.markdown("&lt;div class='kpi-action'&gt;", unsafe_allow_html=True)
        if st.button(lbl, key="kpi_btn_pend", use_container_width=True):
            _toggle_home_status("Pendente")
        st.markdown("&lt;/div&gt;", unsafe_allow_html=True)
    with c2:
        kpi_row([{"label":"Finalizadas", "value": f"{tot_finalizado}", "hint": "Todos os procedimentos"}], extra_class="center")
        lbl = "🔽 Esconder Finalizadas" if active == "Finalizado" else "👁️ Ver Finalizadas"
        st.markdown("&lt;div class='kpi-action'&gt;", unsafe_allow_html=True)
        if st.button(lbl, key="kpi_btn_fin", use_container_width=True):
            _toggle_home_status("Finalizado")
        st.markdown("&lt;/div&gt;", unsafe_allow_html=True)
    with c3:
        kpi_row([{"label":"Não Cobrar", "value": f"{tot_nao_cobrar}", "hint": "Todos os procedimentos"}], extra_class="center")
        lbl = "🔽 Esconder Não Cobrar" if active == "Não Cobrar" else "👁️ Ver Não Cobrar"
        st.markdown("&lt;div class='kpi-action'&gt;", unsafe_allow_html=True)
        if st.button(lbl, key="kpi_btn_nc", use_container_width=True):
            _toggle_home_status("Não Cobrar")
        st.markdown("&lt;/div&gt;", unsafe_allow_html=True)

    status_sel_home = st.session_state.get("home_status")
    if status_sel_home:
        st.divider()
        st.subheader(f"📋 Internações com ao menos 1 procedimento em: **{status_sel_home}**")

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
                cols_show = ["internacao_id","atendimento","paciente","hospital","convenio","data_internacao"]
                df_ints = df_status[cols_show].drop_duplicates(subset=["internacao_id"]).copy()

                def _safe_pt_date_int(s):
                    try:
                        return datetime.strptime(str(s).strip(), "%d/%m/%Y").date()
                    except Exception:
                        try:
                            return datetime.strptime(str(s).strip(), "%Y-%m-%d").date()
                        except Exception:
                            return None

                df_ints["_int_dt"] = df_ints["data_internacao"].apply(_safe_pt_date_int)
                df_ints = (
                    df_ints.sort_values(by=["_int_dt","hospital","paciente"], ascending=[False,True,True])
                          .drop(columns=["_int_dt"])
                )

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

    if st.session_state.get("consulta_codigo"):
        st.caption(f"🔎 Atendimento **{st.session_state['consulta_codigo']}** pronto para consulta na aba **'🔍 Consultar Internação'**.")

# ============================================================
# 📤 1) IMPORTAR
# ============================================================
with tabs[1]:
    st.subheader("📤 Importar arquivo")
    st.markdown("&lt;div class='soft-card'&gt;", unsafe_allow_html=True)

    # Cadastro manual de internação
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
                if nid:
                    st.toast(f"Internação criada (ID {nid}).", icon="✅")

    st.markdown("&lt;/div&gt;", unsafe_allow_html=True)
    st.divider()

    st.markdown("&lt;div class='soft-card'&gt;", unsafe_allow_html=True)
    hospitais = get_hospitais()
    hospital = st.selectbox("Hospital para esta importação:", hospitais)
    arquivo = st.file_uploader("Selecione o arquivo CSV")

    if parse_tiss_original is None:
        st.info("Adicione o arquivo parser.py com a função parse_tiss_original() para habilitar a importação.")
    elif arquivo:
        raw_bytes = arquivo.getvalue()
        try: csv_text = raw_bytes.decode("latin1")
        except UnicodeDecodeError: csv_text = raw_bytes.decode("utf-8-sig", errors="ignore")

        registros = parse_tiss_original(csv_text)
        st.success(f"{len(registros)} registros interpretados!")

        pros = sorted({(r.get("profissional") or "").strip() for r in registros if r.get("profissional")})
        pares = sorted({(r.get("atendimento"), r.get("data")) for r in registros if r.get("atendimento") and r.get("data")})
        kpi_row([
            {"label":"Registros no arquivo", "value": f"{len(registros):,}".replace(",", ".")},
            {"label":"Médicos distintos",    "value": f"{len(pros):,}".replace(",", ".")},
            {"label":"Pares (atendimento, data)", "value": f"{len(pares):,}".replace(",", ".")},
        ])

        st.subheader("👨‍⚕️ Seleção de médicos")
        if "import_all_docs" not in st.session_state: st.session_state["import_all_docs"] = True
        if "import_selected_docs" not in st.session_state: st.session_state["import_selected_docs"] = []

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

        always_in_file = [p for p in pros if p in ALWAYS_SELECTED_PROS]
        final_pros = sorted(set(selected_pros if not import_all else pros).union(always_in_file))

        st.caption(f"Médicos fixos (sempre incluídos, quando presentes): {', '.join(sorted(ALWAYS_SELECTED_PROS))}")
        st.info(f"Médicos considerados: {', '.join(final_pros) if final_pros else '(nenhum)'}")

        registros_filtrados = registros[:] if import_all else [r for r in registros if (r.get("profissional") or "") in final_pros]

        df_preview = pd.DataFrame(registros_filtrados)
        st.subheader("Pré-visualização (DRY RUN) — nada foi gravado ainda")
        st.dataframe(df_preview, use_container_width=True, hide_index=True)

        pares = sorted({(r["atendimento"], r["data"]) for r in registros_filtrados if r.get("atendimento") and r.get("data")})
        st.markdown(
            f"&lt;div&gt;🔎 {len(pares)} par(es) (atendimento, data) após filtros. Regra: "
            f"{pill('1 auto por internação/dia')} (manuais podem ser vários).&lt;/div&gt;",
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
                        if internacao_id: total_internacoes += 1
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

    st.markdown("&lt;/div&gt;", unsafe_allow_html=True)

# ============================================================
# 🔍 2) CONSULTAR
# ============================================================
with tabs[2]:
    st.subheader("🔍 Consultar Internação")

    st.markdown("&lt;div class='soft-card'&gt;", unsafe_allow_html=True)
    hlist = ["Todos"] + get_hospitais()
    filtro_hosp = st.selectbox("Filtrar hospital (consulta):", hlist)
    codigo = st.text_input("Digite o atendimento para consultar:", key="consulta_codigo", placeholder="Ex.: 123456")
    st.markdown("&lt;/div&gt;", unsafe_allow_html=True)

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

            # ===== Edição da internação =====
            st.subheader("✏️ Editar dados da internação")
            with st.container():
                c1, c2, c3, c4 = st.columns(4)
                with c1:
                    novo_paciente = st.text_input("Paciente:", value=df_int["paciente"].iloc[0] or "")
                with c2:
                    novo_convenio = st.text_input("Convênio:", value=df_int["convenio"].iloc[0] or "")
                with c3:
                    data_atual = df_int["data_internacao"].iloc[0]
                    try:
                        dt_atual = datetime.strptime(str(data_atual), "%d/%m/%Y").date()
                    except:
                        dt_atual = date.today()
                    nova_data = st.date_input("Data da internação:", value=dt_atual)
                with c4:
                    todos_hospitais = get_hospitais(include_inactive=True)
                    try:
                        idx_h = todos_hospitais.index(df_int["hospital"].iloc[0])
                    except Exception:
                        idx_h = 0 if todos_hospitais else None
                    novo_hospital = st.selectbox("Hospital:", todos_hospitais, index=idx_h if idx_h is not None else 0)

                col_save_int = st.columns(6)[-1]
                with col_save_int:
                    if st.button("💾 Salvar alterações da internação", type="primary"):
                        atualizar_internacao(
                            internacao_id,
                            paciente=novo_paciente,
                            convenio=novo_convenio,
                            data_internacao=nova_data,
                            hospital=novo_hospital
                        )
                        st.toast("Dados da internação atualizados!", icon="✅")
                        st.rerun()

            # ===== Excluir internação =====
            with st.expander("🗑️ Excluir esta internação"):
                st.warning("Esta ação apagará a internação e TODOS os procedimentos vinculados.")
                confirm_txt = st.text_input("Digite APAGAR para confirmar", key="confirm_del_int")
                col_del = st.columns(6)[-1]
                with col_del:
                    if st.button("Excluir internação", key="btn_del_int"):
                        if confirm_txt.strip().upper() == "APAGAR":
                            deletar_internacao(internacao_id)
                            st.toast("🗑️ Internação excluída.", icon="✅")
                            st.rerun()
                        else:
                            st.info("Confirmação inválida. Digite APAGAR.")

            # ===== Procedimentos (edição) =====
            try:
                res_p = supabase.table("procedimentos").select(
                    "id, data_procedimento, profissional, procedimento, situacao, observacao, aviso, grau_participacao"
                ).eq("internacao_id", internacao_id).execute()
                df_proc = pd.DataFrame(res_p.data or [])
            except APIError as e:
                _sb_debug_error(e, "Falha ao carregar procedimentos.")
                df_proc = pd.DataFrame()

            if "procedimento" not in df_proc.columns:
                df_proc["procedimento"] = "Cirurgia / Procedimento"

            for c, default in [
                ("procedimento", "Cirurgia / Procedimento"),
                ("situacao", "Pendente"),
                ("observacao", ""),
                ("aviso", ""),
                ("grau_participacao", ""),
            ]:
                if c not in df_proc.columns: df_proc[c] = default
                df_proc[c] = df_proc[c].fillna(default)

            def _safe_pt_date(s):
                try:
                    return datetime.strptime(str(s).strip(), "%d/%m/%Y").date()
                except Exception:
                    try:
                        return datetime.strptime(str(s).strip(), "%Y-%m-%d").date()
                    except Exception:
                        return None

            df_proc["_data_dt"] = df_proc["data_procedimento"].apply(_safe_pt_date)
            df_proc = df_proc.sort_values(by=["_data_dt","id"], ascending=[True, True]).reset_index(drop=True)
            df_proc["data_procedimento"] = df_proc["_data_dt"].apply(lambda d: d.strftime("%d/%m/%Y") if pd.notna(d) else "")
            df_proc = df_proc.drop(columns=["_data_dt"])

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
                                "grau_participacao": (row["grau_participacao_new"] if row["grau_participacao_new"] != "" else None),
                                "aviso": row["aviso_new"],
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
                                aviso=item.get("aviso"),
                            )
                        st.toast(f"{len(alterados)} procedimento(s) atualizado(s).", icon="✅")
                        st.rerun()

            # ===== Excluir procedimento =====
            with st.expander("🗑️ Excluir cirurgia (procedimento)"):
                if df_proc.empty:
                    st.info("Não há procedimentos para excluir.")
                else:
                    for _, r in df_proc.iterrows():
                        c1, c2, c3, c4 = st.columns([3, 3, 3, 2])
                        with c1: st.markdown(f"**ID:** {int(r['id'])}  —  **Data:** {r['data_procedimento']}")
                        with c2: st.markdown(f"**Profissional:** {r['profissional'] or '-'}")
                        with c3: st.markdown(f"**Tipo:** {r['procedimento']}&lt;br&gt;{pill(r['situacao'])}", unsafe_allow_html=True)
                        with c4:
                            if st.button("Excluir", key=f"del_proc_{int(r['id'])}", help="Apagar este procedimento"):
                                deletar_procedimento(int(r["id"]))
                                st.toast(f"Procedimento {int(r['id'])} excluído.", icon="🗑️")
                                st.rerun()

            # ===== Lançar manual =====
            st.divider()
            st.subheader("➕ Lançar procedimento manual (permite vários no mesmo dia)")
            c1, c2, c3 = st.columns(3)
            with c1: data_proc = st.date_input("Data do procedimento", value=date.today())
            with c2:               
                Profissionais distintos existentes (lado cliente, sem DISTINCT no PostgREST)
                try:
                    res_dist = supabase.table("procedimentos").select("profissional").execute()
                    df_pros = pd.DataFrame(res_dist.data or [])
                    if "profissional" in df_pros.columns:
                        lista_profissionais = sorted({
                            str(x).strip() for x in df_pros["profissional"].dropna()
                            if str(x).strip()  # remove vazios
                        })
                    else:
                        lista_profissionais = []
                except APIError:
                    lista_profissionais = []
                    
                profissional = st.selectbox("Profissional", ["(selecione)"] + lista_profissionais, index=0)
            with c3: situacao = st.selectbox("Situação", STATUS_OPCOES, index=0)

            colp1, colp2, colp3 = st.columns(3)
            with colp1: procedimento_tipo = st.selectbox("Tipo de Procedimento", PROCEDIMENTO_OPCOES, index=0)
            with colp2: observacao = st.text_input("Observações (opcional)")
            with colp3: grau_part = st.selectbox("Grau de Participação", [""] + GRAU_PARTICIPACAO_OPCOES, index=0)

            col_add = st.columns(6)[-1]
            with col_add:
                data_internacao_str = df_int["data_internacao"].iloc[0]
                try:
                    dt_internacao = datetime.strptime(str(data_internacao_str), "%d/%m/%Y").date()
                except:
                    dt_internacao = date.today()
                if st.button("Adicionar procedimento", key="btn_add_manual", type="primary"):
                    if data_proc &lt; dt_internacao:
                        st.error("❌ A data do procedimento não pode ser anterior à data da internação.")
                    else:
                        if profissional == "(selecione)":
                            st.error("Selecione um profissional.")
                        else:
                            criar_procedimento(
                                internacao_id, data_proc, profissional, procedimento_tipo,
                                situacao=situacao,
                                observacao=(observacao or None),
                                is_manual=1,
                                aviso=None,
                                grau_participacao=(grau_part if grau_part != "" else None),
                            )
                            st.toast("Procedimento (manual) adicionado.", icon="✅")
                            st.rerun()

            # ===== Ver quitação (Finalizados) =====
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
                        total = float(q.get("quitacao_valor_amhptiss") or 0) + float(q.get("quitacao_valor_complemento") or 0)
                        st.markdown("---"); st.markdown("### 🧾 Detalhes da quitação")
                        c1, c2, c3 = st.columns(3)
                        with c1:
                            st.markdown(f"**Atendimento:** {q.get('atendimento','')}")
                            st.markdown(f"**Hospital:** {q.get('hospital','')}")
                            st.markdown(f"**Convênio:** {q.get('convenio') or '-'}")
                        with c2:
                            st.markdown(f"**Paciente:** {q.get('paciente','')}")
                            st.markdown(f"**Data procedimento:** {q.get('data_procedimento') or '-'}")
                            st.markdown(f"**Profissional:** {q.get('profissional') or '-'}")
                        with c3:
                            st.markdown(f"**Status:** {pill(q.get('situacao'))}", unsafe_allow_html=True)
                            st.markdown(f"**Aviso:** {q.get('aviso') or '-'}")
                            st.markdown(f"**Grau participação:** {q.get('grau_participacao') or '-'}")

                        st.markdown("#### 💳 Quitação")
                        c4, c5, c6 = st.columns(3)
                        with c4:
                            st.markdown(f"**Data da quitação:** {q.get('quitacao_data') or '-'}")
                            st.markdown(f"**Guia AMHPTISS:** {q.get('quitacao_guia_amhptiss') or '-'}")
                        with c5:
                            st.markdown(f"**Valor Guia AMHPTISS:** {_format_currency_br(q.get('quitacao_valor_amhptiss'))}")
                            st.markdown(f"**Guia Complemento:** {q.get('quitacao_guia_complemento') or '-'}")
                        with c6:
                            st.markdown(f"**Valor Guia Complemento:** {_format_currency_br(q.get('quitacao_valor_complemento'))}")
                            st.markdown(f"**Total Quitado:** **{_format_currency_br(total)}**")

                        st.markdown("**Observações da quitação:**")
                        st.write(q.get("quitacao_observacao") or "-")

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
                                st.session_state["show_quit_id"] = None
                                st.rerun()

# ============================================================
# 📑 3) RELATÓRIOS
# ============================================================
# --- PDF: Cirurgias por Status ---
if REPORTLAB_OK:
    def _pdf_cirurgias_por_status(df, filtros):
        buf = io.BytesIO()
        doc = SimpleDocTemplate(buf, pagesize=landscape(A4), leftMargin=18, rightMargin=18, topMargin=18, bottomMargin=18)
        styles = getSampleStyleSheet()
        H1 = styles["Heading1"]; H2 = styles["Heading2"]; N = styles["BodyText"]
        from reportlab.lib.styles import ParagraphStyle
        from reportlab.platypus import Paragraph

        TH = ParagraphStyle("TH", parent=styles["Normal"], fontName="Helvetica-Bold", fontSize=9, leading=11, alignment=1)
        TD = ParagraphStyle("TD", parent=styles["Normal"], fontName="Helvetica", fontSize=8, leading=10, wordWrap="LTR")
        TD_CENTER = ParagraphStyle(**{**TD.__dict__, "alignment":1})
        elems = []
        elems.append(Paragraph("Relatório — Cirurgias por Status", H1)); elems.append(Spacer(1,6))
        filtros_txt = (f"Período: {filtros['ini']} a {filtros['fim']}  |  Hospital: {filtros['hospital']}  |  Status: {filtros['status']}")
        elems.append(Paragraph(filtros_txt, N)); elems.append(Spacer(1,8))
        total = len(df); elems.append(Paragraph(f"Total de cirurgias: &lt;b&gt;{total}&lt;/b&gt;", H2))

        if total &gt; 0 and filtros["status"] == "Todos":
            resumo = (df.groupby("situacao")["situacao"].count().sort_values(ascending=False).reset_index(name="qtd"))
            data_resumo = [["Situação", "Quantidade"]] + resumo.values.tolist()
            t_res = Table(data_resumo, hAlign="LEFT")
            t_res.setStyle(TableStyle([
                ("BACKGROUND", (0,0), (-1,0), colors.HexColor("#F0F0F0")),
                ("GRID", (0,0), (-1,-1), 0.25, colors.grey),
                ("ALIGN", (1,1), (-1,-1), "RIGHT"),
                ("FONTNAME", (0,0), (-1,0), "Helvetica-Bold"),
                ("FONTSIZE", (0,0), (-1,0), 9),
            ]))
            elems.append(t_res); elems.append(Spacer(1,10))

        header_labels = ["Atendimento","Aviso","Convênio","Paciente","Data","Tipo","Profissional","Grau de Participação","Hospital","Situação"]
        header = [Paragraph(h, TH) for h in header_labels]
        col_widths = [2.6*cm,2.0*cm,2.8*cm,5.0*cm,2.2*cm,2.4*cm,2.8*cm,3.0*cm,2.6*cm,2.1*cm]

        def _p(v, style=TD): return Paragraph("" if v is None else str(v), style)
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
            ("ALIGN", (0,0), (-1,0), "CENTER"),
        ]))
        elems.append(table)
        doc.build(elems)
        pdf_bytes = buf.getvalue(); buf.close()
        return pdf_bytes
else:
    def _pdf_cirurgias_por_status(*args, **kwargs):
        raise RuntimeError("ReportLab não está instalado no ambiente.")

# --- PDF: Quitações ---
if REPORTLAB_OK:
    def _pdf_quitacoes(df, filtros):
        v_amhp = pd.to_numeric(df.get("quitacao_valor_amhptiss", 0), errors="coerce").fillna(0.0)
        v_comp = pd.to_numeric(df.get("quitacao_valor_complemento", 0), errors="coerce").fillna(0.0)
        total_amhp = float(v_amhp.sum()); total_comp = float(v_comp.sum()); total_geral = total_amhp + total_comp

        buf = io.BytesIO()
        doc = SimpleDocTemplate(buf, pagesize=landscape(A4), leftMargin=18, rightMargin=18, topMargin=18, bottomMargin=18)
        styles = getSampleStyleSheet()
        H1 = styles["Heading1"]; N = styles["BodyText"]
        elems = []
        elems.append(Paragraph("Relatório — Quitações", H1))
        filtros_txt = (f"Período da quitação: {filtros['ini']} a {filtros['fim']}  |  Hospital: {filtros['hospital']}")
        elems.append(Paragraph(filtros_txt, N)); elems.append(Spacer(1, 8))

        header = ["Convênio","Paciente","Profissional","Data","Atendimento","Guia AMHP","Guia Complemento","Valor AMHP","Valor Complemento","Data da quitação"]
        col_widths = [3.2*cm,6.0*cm,6.0*cm,2.4*cm,2.8*cm,3.2*cm,3.6*cm,3.2*cm,3.6*cm,2.8*cm]
        data_rows = []
        for _, r in df.iterrows():
            data_rows.append([
                r.get("convenio") or "", r.get("paciente") or "", r.get("profissional") or "",
                r.get("data_procedimento") or "", r.get("atendimento") or "",
                r.get("quitacao_guia_amhptiss") or "", r.get("quitacao_guia_complemento") or "",
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
            ("ALIGN", (7,1), (8,-1), "RIGHT"),
            ("ALIGN", (3,1), (3,-1), "CENTER"),
            ("ALIGN", (4,1), (4,-1), "CENTER"),
            ("ALIGN", (9,1), (9,-1), "CENTER"),
        ]
        table.setStyle(TableStyle(style_cmds)); elems.append(table); elems.append(Spacer(1,8))

        totals_data = [
            ["Total AMHP:", _format_currency_br(total_amhp)],
            ["Total Complemento:", _format_currency_br(total_comp)],
            ["Total Geral:", _format_currency_br(total_geral)],
        ]
        totals_tbl = Table(totals_data, colWidths=[4.5*cm, 3.5*cm], hAlign="RIGHT")
        totals_tbl.setStyle(TableStyle([
            ("FONTNAME", (0,0), (-1,-1), "Helvetica"), ("FONTSIZE", (0,0), (-1,-1), 10),
            ("ALIGN", (0,0), (0,-1), "RIGHT"), ("ALIGN", (1,0), (1,-1), "RIGHT"),
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

    # Carrega base (procedimentos Cirurgia/Proc + merge com internacoes)
    try:
        resp = supabase.table("procedimentos").select(
            "internacao_id, data_procedimento, aviso, profissional, procedimento, grau_participacao, situacao"
        ).eq("procedimento", "Cirurgia / Procedimento").execute()
        dfp = pd.DataFrame(resp.data or [])
        if dfp.empty:
            df_rel = pd.DataFrame()
        else:
            ids = sorted(set(int(x) for x in dfp["internacao_id"].dropna().tolist()))
            if ids:
                resi = supabase.table("internacoes").select(
                    "id, hospital, atendimento, paciente, convenio"
                ).in_("id", ids).execute()
                dfi = pd.DataFrame(resi.data or [])
            else:
                dfi = pd.DataFrame(columns=["id","hospital","atendimento","paciente","convenio"])
            df_rel = safe_merge(dfp, dfi, left_on="internacao_id", right_on="id", how="left")
    except APIError as e:
        _sb_debug_error(e, "Falha ao carregar dados para Relatório.")
        df_rel = pd.DataFrame()

    if not df_rel.empty:
        df_rel["_data_dt"] = df_rel["data_procedimento"].apply(_pt_date_to_dt)
        mask = (df_rel["_data_dt"].notna()) &amp; (df_rel["_data_dt"] &gt;= dt_ini) &amp; (df_rel["_data_dt"] &lt;= dt_fim)
        df_rel = df_rel[mask].copy()
        if hosp_sel != "Todos": df_rel = df_rel[df_rel["hospital"] == hosp_sel]
        if status_sel != "Todos": df_rel = df_rel[df_rel["situacao"] == status_sel]
        df_rel = df_rel.sort_values(by=["_data_dt","hospital","paciente","atendimento"])
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

    # Carrega procedimentos finalizados (com data de quitação) + merge
    try:
        resp = supabase.table("procedimentos").select(
            "internacao_id, data_procedimento, profissional, quitacao_data, quitacao_guia_amhptiss, quitacao_guia_complemento, quitacao_valor_amhptiss, quitacao_valor_complemento"
        ).eq("procedimento", "Cirurgia / Procedimento").not_.is_("quitacao_data", None).execute()
        dfp = pd.DataFrame(resp.data or [])
        if dfp.empty:
            df_quit = pd.DataFrame()
        else:
            ids = sorted(set(int(x) for x in dfp["internacao_id"].dropna().tolist()))
            if ids:
                resi = supabase.table("internacoes").select("id, hospital, atendimento, paciente, convenio").in_("id", ids).execute()
                dfi = pd.DataFrame(resi.data or [])
            else:
                dfi = pd.DataFrame()
            df_quit = safe_merge(dfp, dfi, left_on="internacao_id", right_on="id", how="left")
    except APIError as e:
        _sb_debug_error(e, "Falha ao carregar dados de quitações.")
        df_quit = pd.DataFrame()

    if not df_quit.empty:
        df_quit["_quit_dt"] = df_quit["quitacao_data"].apply(_pt_date_to_dt)
        mask_q = (df_quit["_quit_dt"].notna()) &amp; (df_quit["_quit_dt"] &gt;= dt_ini_q) &amp; (df_quit["_quit_dt"] &lt;= dt_fim_q)
        df_quit = df_quit[mask_q].copy()
        if hosp_sel_q != "Todos":
            df_quit = df_quit[df_quit["hospital"] == hosp_sel_q]

        df_quit = df_quit.sort_values(by=["_quit_dt","convenio","paciente"])
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
# 💼 4) QUITAÇÃO (edição em lote)
# ============================================================
with tabs[4]:
    st.subheader("💼 Quitação de Cirurgias")

    st.markdown("&lt;div class='soft-card'&gt;", unsafe_allow_html=True)
    hosp_opts = ["Todos"] + get_hospitais()
    hosp_sel = st.selectbox("Hospital", hosp_opts, index=0, key="quit_hosp")
    st.markdown("&lt;/div&gt;", unsafe_allow_html=True)

    # Carrega pendentes de envio + merge (sem embed)
    try:
        resp = supabase.table("procedimentos").select(
            "id, internacao_id, data_procedimento, profissional, aviso, situacao, "
            "quitacao_data, quitacao_guia_amhptiss, quitacao_valor_amhptiss, "
            "quitacao_guia_complemento, quitacao_valor_complemento, quitacao_observacao"
        ).eq("procedimento", "Cirurgia / Procedimento").eq("situacao", "Enviado para pagamento").execute()
        dfp = pd.DataFrame(resp.data or [])
        if dfp.empty:
            df_quit = pd.DataFrame()
        else:
            ids = sorted(set(int(x) for x in dfp["internacao_id"].dropna().tolist()))
            if ids:
                resi = supabase.table("internacoes").select("id, hospital, atendimento, paciente, convenio").in_("id", ids).execute()
                dfi = pd.DataFrame(resi.data or [])
            else:
                dfi = pd.DataFrame()
            df_quit = safe_merge(dfp, dfi, left_on="internacao_id", right_on="id", how="left", suffixes=("", "_int"))
    except APIError as e:
        _sb_debug_error(e, "Falha ao carregar pendências de quitação.")
        df_quit = pd.DataFrame()

    if hosp_sel != "Todos" and not df_quit.empty:
        df_quit = df_quit[df_quit["hospital"] == hosp_sel]

    if df_quit.empty:
        st.info("Não há cirurgias com status 'Enviado para pagamento' para quitação.")
    else:
        # normalizações de tipos
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

                if faltando_data &gt; 0 and atualizados == 0:
                    st.warning("Nenhuma quitação gravada. Preencha a **Data da quitação** para finalizar.")
                elif faltando_data &gt; 0 and atualizados &gt; 0:
                    st.toast(f"{atualizados} quitação(ões) gravada(s). {faltando_data} linha(s) ignoradas sem **Data da quitação**.", icon="✅")
                    st.rerun()
                else:
                    st.toast(f"{atualizados} quitação(ões) gravada(s).", icon="✅")
                    st.rerun()

# ============================================================
# ⚙️ 5) SISTEMA — Diagnósticos simples
# ============================================================
with tabs[5]:
    st.subheader("⚙️ Sistema")
    st.markdown("&lt;div class='soft-card'&gt;", unsafe_allow_html=True)
    st.markdown("**🔌 Conexão Supabase**")
    ok = True
    try:
        _ = supabase.table("hospitals").select("id", count="exact").limit(1).execute()
        st.success("Conexão OK.")
    except APIError as e:
        ok = False
        _sb_debug_error(e, "Falha ao conectar/consultar Supabase.")
    st.markdown("&lt;/div&gt;", unsafe_allow_html=True)

    st.markdown("**📋 Procedimentos — Lista**")
    filtro = ["Todos"] + get_hospitais()
    chosen = st.selectbox("Hospital (lista de procedimentos):", filtro, key="sys_proc_hosp")

    if st.button("Carregar procedimentos", key="btn_carregar_proc", type="primary"):
        try:
            resp = supabase.table("procedimentos").select(
                "id, internacao_id, data_procedimento, aviso, profissional, grau_participacao, procedimento, situacao, observacao"
            ).execute()
            dfp = pd.DataFrame(resp.data or [])
            if dfp.empty:
                st.info("Sem procedimentos.")
            else:
                ids = sorted(set(int(x) for x in dfp["internacao_id"].dropna().tolist()))
                resi = supabase.table("internacoes").select("id, hospital, atendimento, paciente").in_("id", ids).execute() if ids else None
                dfi = pd.DataFrame(resi.data or []) if resi else pd.DataFrame()
                df = safe_merge(dfp, dfi, left_on="internacao_id", right_on="id", how="left", suffixes=("", "_i"))
                if chosen != "Todos":
                    df = df[df["hospital"] == chosen]
                df = df.sort_values(by=["data_procedimento","id"], ascending=[False, False])
                st.dataframe(df, use_container_width=True, hide_index=True)
        except APIError as e:
            _sb_debug_error(e, "Falha ao carregar procedimentos.")

    st.divider()
    st.markdown("**🧾 Resumo por Profissional**")
    filtro_prof = ["Todos"] + get_hospitais()
    chosen_prof = st.selectbox("Hospital (resumo por profissional):", filtro_prof, key="sys_prof_hosp")
    try:
        resp = supabase.table("procedimentos").select("internacao_id, profissional").not_.is_("profissional", None).execute()
        dfp = pd.DataFrame(resp.data or [])
        if dfp.empty:
            st.info("Sem dados.")
        else:
            ids = sorted(set(int(x) for x in dfp["internacao_id"].dropna().tolist()))
            resi = supabase.table("internacoes").select("id, hospital").in_("id", ids).execute() if ids else None
            dfi = pd.DataFrame(resi.data or []) if resi else pd.DataFrame()
            dfm = safe_merge(dfp, dfi, left_on="internacao_id", right_on="id", how="left")
            if chosen_prof != "Todos":
                dfm = dfm[dfm["hospital"] == chosen_prof]
            df_prof = dfm.groupby("profissional")["profissional"].count().reset_index(name="total").sort_values("total", ascending=False)
            st.dataframe(df_prof, use_container_width=True, hide_index=True)
    except APIError as e:
        _sb_debug_error(e, "Falha no resumo por profissional.")

    st.divider()
    st.markdown("**💸 Resumo por Convênio**")
    filtro_conv = ["Todos"] + get_hospitais()
    chosen_conv = st.selectbox("Hospital (resumo por convênio):", filtro_conv, key="sys_conv_hosp")

    try:
        # Internações (lado direito do merge), traz convenio e hospital
        resi = supabase.table("internacoes").select("id, convenio, hospital").execute()
        dfi = pd.DataFrame(resi.data or [])

        if dfi.empty:
            st.info("Sem dados de internações.")
        else:
            if chosen_conv != "Todos":
                dfi = dfi[dfi["hospital"] == chosen_conv]

            # Procedimentos (lado esquerdo do merge), traz internacao_id
            resp = supabase.table("procedimentos").select("internacao_id").execute()
            dfp = pd.DataFrame(resp.data or [])

            if dfp.empty:
                st.info("Sem procedimentos.")
            else:
                # Garante internacao_id válidos
                dfp = dfp[dfp["internacao_id"].notna()]
                ids = sorted(set(int(x) for x in dfp["internacao_id"].tolist() if pd.notna(x)))

                if not ids:
                    st.info("Sem vínculos de procedimentos com internações.")
                else:
                    # Reduz o universo de internações para as usadas
                    dfi_ids = dfi[dfi["id"].isin(ids)].copy()

                    # Merge seguro
                    dfm = safe_merge(dfp, dfi_ids, left_on="internacao_id", right_on="id", how="left")

                    # Agrega por convênio (ignora nulos/vazios)
                    df_conv = (
                        dfm[dfm["convenio"].notna() &amp; (dfm["convenio"].astype(str).str.strip() != "")]
                        .groupby("convenio")["convenio"]
                        .count()
                        .reset_index(name="total")
                        .sort_values("total", ascending=False)
                    )

                    if df_conv.empty:
                        st.info("Sem dados para o resumo por convênio.")
                    else:
                        st.dataframe(df_conv, use_container_width=True, hide_index=True)

    except APIError as e:
        _sb_debug_error(e, "Falha no resumo por convênio.")

# ---- Troca de aba programática ----
if st.session_state.get("goto_tab_label"):
    _switch_to_tab_by_label(st.session_state["goto_tab_label"])
    st.session_state["goto_tab_label"] = None
