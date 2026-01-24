# ============================================================
#  SISTEMA DE INTERNAÇÕES — VERSÃO SUPABASE (GABMA)
# ============================================================

import streamlit as st
import pandas as pd
from datetime import date, datetime
import io
import base64, json
import requests
import re
import streamlit.components.v1 as components
from supabase import create_client, Client

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

# Parser (mantenha o arquivo parser.py no projeto)
try:
    from parser import parse_tiss_original
except ImportError:
    st.error("Arquivo parser.py não encontrado no diretório.")

# ---------------------------
# CONFIGURAÇÃO SUPABASE
# ---------------------------
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

ALWAYS_SELECTED_PROS = {"JOSE.ADORNO", "CASSIO CESAR", "FERNANDO AND", "SIMAO.MATOS"}

# ---------------------------
# CRUD SUPABASE
# ---------------------------

def get_hospitais(include_inactive: bool = False) -> list:
    query = supabase.table("Hospitals").select("name")
    if not include_inactive:
        query = query.eq("active", 1)
    res = query.order("name").execute()
    return [item['name'] for item in res.data]

def get_internacao_by_atendimento(att):
    res = supabase.table("Internacoes").select("*").eq("atendimento", str(att)).execute()
    return pd.DataFrame(res.data)

def criar_internacao(hospital, atendimento, paciente, data, convenio):
    payload = {
        "hospital": hospital,
        "atendimento": str(atendimento),
        "paciente": paciente,
        "data_internacao": data,
        "convenio": convenio,
        "numero_internacao": float(atendimento) if str(atendimento).replace('.','').isdigit() else 0
    }
    res = supabase.table("Internacoes").insert(payload).execute()
    return res.data[0]['id']

def atualizar_internacao(internacao_id, **kwargs):
    update_data = {k: v for k, v in kwargs.items() if v is not None}
    supabase.table("Internacoes").update(update_data).eq("id", internacao_id).execute()

def criar_procedimento(internacao_id, data_proc, profissional, procedimento, **kwargs):
    payload = {
        "internacao_id": internacao_id,
        "data_procedimento": data_proc,
        "profissional": profissional,
        "procedimento": procedimento,
        "situacao": kwargs.get("situacao", "Pendente"),
        "is_manual": kwargs.get("is_manual", 0),
        "aviso": kwargs.get("aviso"),
        "observacao": kwargs.get("observacao"),
        "grau_participacao": kwargs.get("grau_participacao")
    }
    supabase.table("Procedimentos").insert(payload).execute()

def atualizar_procedimento(proc_id, **kwargs):
    update_data = {k: v for k, v in kwargs.items() if v is not None}
    supabase.table("Procedimentos").update(update_data).eq("id", proc_id).execute()

def deletar_internacao(internacao_id: int):
    supabase.table("Procedimentos").delete().eq("internacao_id", internacao_id).execute()
    supabase.table("Internacoes").delete().eq("id", internacao_id).execute()

def deletar_procedimento(proc_id: int):
    supabase.table("Procedimentos").delete().eq("id", proc_id).execute()

def existe_procedimento_no_dia(internacao_id, data_proc):
    res = supabase.table("Procedimentos").select("id").eq("internacao_id", internacao_id).eq("data_procedimento", data_proc).eq("is_manual", 0).execute()
    return len(res.data) > 0

def quitar_procedimento(proc_id, **kwargs):
    update_data = {
        "quitacao_data": kwargs.get("data_quitacao"),
        "quitacao_guia_amhptiss": kwargs.get("guia_amhptiss"),
        "quitacao_valor_amhptiss": kwargs.get("valor_amhptiss"),
        "quitacao_guia_complemento": kwargs.get("guia_complemento"),
        "quitacao_valor_complemento": kwargs.get("valor_complemento"),
        "quitacao_observacao": kwargs.get("quitacao_observacao"),
        "situacao": "Finalizado"
    }
    update_data = {k: v for k, v in update_data.items() if v is not None}
    supabase.table("Procedimentos").update(update_data).eq("id", proc_id).execute()

def reverter_quitacao(proc_id: int):
    update_data = {
        "quitacao_data": None, "quitacao_guia_amhptiss": None, "quitacao_valor_amhptiss": None,
        "quitacao_guia_complemento": None, "quitacao_valor_complemento": None,
        "quitacao_observacao": None, "situacao": "Enviado para pagamento"
    }
    supabase.table("Procedimentos").update(update_data).eq("id", proc_id).execute()

def get_quitacao_by_proc_id(proc_id: int):
    res = supabase.table("Procedimentos").select("*, Internacoes(*)").eq("id", proc_id).execute()
    df = pd.json_normalize(res.data)
    df.columns = [c.replace('Internacoes.', '') for c in df.columns]
    return df

# ---------------------------
# UTILITÁRIOS
# ---------------------------
def _format_currency_br(v) -> str:
    if v is None or (isinstance(v, float) and pd.isna(v)): return "R$ 0,00"
    try:
        v = float(v)
        return f"R$ {v:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    except: return "R$ 0,00"

def _to_ddmmyyyy(value):
    if not value: return ""
    if isinstance(value, (datetime, date)): return value.strftime("%d/%m/%Y")
    return str(value)

# ---------------------------
# INTERFACE E CSS
# ---------------------------
def inject_css():
    st.markdown("""
    <style>
    :root{ --bg-main: #F5F6F7; --bg-card: #FFFFFF; --border: #D0D7DE; --primary: #1F6FEB; --radius: 8px; }
    .soft-card{ background: var(--bg-card); border:1px solid var(--border); border-radius: var(--radius); padding: 14px; margin-bottom: 12px; }
    .kpi{ text-align: center; padding: 10px; border: 1px solid var(--border); border-radius: 8px; background: white; }
    .kpi .value{ font-size: 2rem; font-weight: bold; color: var(--primary); }
    </style>
    """, unsafe_allow_html=True)

# ---------------------------
# INICIALIZAÇÃO APP
# ---------------------------
st.set_page_config(page_title="GABMA - Gestão Médica", page_icon="🏥", layout="wide")
inject_css()

st.title("🏥 GABMA — Gestão Médica")
st.caption("Faturamento e Controle de Internações (Versão Cloud)")

tabs = st.tabs(["🏠 Início", "📤 Importar", "🔍 Consultar", "📑 Relatórios", "💼 Quitação"])

# --- ABA INÍCIO ---
with tabs[0]:
    st.subheader("Resumo Geral")
    res = supabase.table("Procedimentos").select("situacao, data_procedimento").execute()
    df_home = pd.DataFrame(res.data)
    
    if not df_home.empty:
        c1, c2, c3 = st.columns(3)
        c1.metric("Pendentes", len(df_home[df_home['situacao'] == 'Pendente']))
        c2.metric("Finalizados", len(df_home[df_home['situacao'] == 'Finalizado']))
        c3.metric("Enviados", len(df_home[df_home['situacao'] == 'Enviado para pagamento']))
    else:
        st.info("Nenhum dado encontrado no banco de dados.")

# --- ABA IMPORTAR ---
with tabs[1]:
    st.subheader("Importação de CSV TISS")
    hospitais = get_hospitais()
    h_sel = st.selectbox("Hospital", hospitais)
    file = st.file_uploader("Arquivo CSV", type="csv")
    
    if file:
        raw = file.getvalue().decode("latin1")
        regs = parse_tiss_original(raw)
        st.write(f"Interpretados {len(regs)} registros.")
        if st.button("Gravar no Banco"):
            for r in regs:
                df_i = get_internacao_by_atendimento(r['atendimento'])
                if df_i.empty:
                    int_id = criar_internacao(h_sel, r['atendimento'], r['paciente'], r['data'], r['convenio'])
                else:
                    int_id = int(df_i['id'].iloc[0])
                
                if not existe_procedimento_no_dia(int_id, r['data']):
                    criar_procedimento(int_id, r['data'], r['profissional'], "Cirurgia / Procedimento", aviso=r.get('aviso'))
            st.success("Importação concluída!")

# --- ABA CONSULTAR ---
with tabs[2]:
    st.subheader("Consulta de Atendimento")
    cod = st.text_input("Número do Atendimento")
    if cod:
        df_i = get_internacao_by_atendimento(cod)
        if not df_i.empty:
            st.write(df_i)
            int_id = int(df_i['id'].iloc[0])
            res_p = supabase.table("Procedimentos").select("*").eq("internacao_id", int_id).execute()
            df_p = pd.DataFrame(res_p.data)
            st.data_editor(df_p, use_container_width=True)
            
            if st.button("Excluir Internação"):
                deletar_internacao(int_id)
                st.warning("Deletado.")
                st.rerun()
        else:
            st.error("Não encontrado.")

# --- ABA QUITAÇÃO ---
with tabs[4]:
    st.subheader("Processar Quitações")
    res_q = supabase.table("Procedimentos").select("*, Internacoes(paciente, hospital)").eq("situacao", "Enviado para pagamento").execute()
    df_q = pd.json_normalize(res_q.data)
    if not df_q.empty:
        st.data_editor(df_q, key="edit_quit")
        if st.button("Salvar Quitações"):
            # Lógica de salvar lote aqui
            st.success("Salvo!")
    else:
        st.info("Nada pendente para envio.")
