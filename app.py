
import streamlit as st
from models import create_tables
from parser import parse_csv
from database import (
    get_internacao_by_atendimento,
    criar_internacao,
    criar_procedimento
)

st.set_page_config(page_title="Importador Hospitalar", layout="wide")

create_tables()

st.title("🏥 Importador de Produção Cirúrgica")

hospital = st.selectbox("Selecione o hospital:", ["HSL", "HBDF", "HMIB", "Outro"])
arquivo = st.file_uploader("Importe o arquivo CSV")

if arquivo:
    registros = parse_csv(arquivo)
    st.write(f"Registros encontrados: {len(registros)}")

    for r in registros:
        atendimento = r["atendimento"]
        paciente = r["paciente"]
        convenio = r["convenio"]
        data = r["data"]
        procedimento = r["procedimento"]
        profissional = r["profissional"]

        # VERIFICA INTERNAÇÃO EXISTENTE
        existente = get_internacao_by_atendimento(atendimento)

        if not existente:
            numero_internacao = float(atendimento)
            internacao_id = criar_internacao(
                numero_internacao,
                hospital,
                atendimento,
                paciente,
                data,
                convenio
            )
        else:
            internacao_id = existente[0]

        # CRIA PROCEDIMENTO
        criar_procedimento(
            internacao_id,
            data,
            profissional,
            procedimento
        )

    st.success("Importação concluída com sucesso!")
