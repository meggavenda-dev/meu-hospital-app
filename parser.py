
import csv
import re

def parse_csv(file):
    linhas = file.read().decode("latin1", errors="ignore").splitlines()

    registros = []
    atendimento_atual = None
    paciente_atual = None
    convenio_atual = None
    data_atual = None
    profissional_atual = None

    for linha in linhas:

        # Remove caracteres duplicados/ruins
        linha = linha.replace("\x00", "").strip()

        # Se contiver atendimento (começa com número grande)
        if re.match(r"^\s*\d{6,}", linha):
            partes = linha.split(",")

            atendimento_atual = partes[0].strip()
            paciente_atual = partes[1].strip() if len(partes) > 1 else ""
            convenio_atual = ""
            profissional_atual = ""
            data_atual = ""

        # Detecta convênio
        if "CBM" in linha or "CASSI" in linha or "UNIMED" in linha or "BRADESCO" in linha:
            partes = linha.split(",")
            if len(partes) > 10:
                convenio_atual = partes[10].strip()

        # Detecta profissional
        if "PRESTADOR" in linha.upper() or "MÉD" in linha.upper():
            partes = linha.split(",")
            profissional_atual = partes[-1].strip()

        # Detecta data de realização
        if "/12/2025" in linha:
            data_atual = linha.strip().replace("Data de Realização :", "").replace(",", "").strip()

        # Detecta descrição de cirurgia
        if any(x in linha.upper() for x in ["ECTOMIA", "PLASTIA", "VIDEO", "HERNIA", "SINOV", "ARTRO", "TENÓ", "CURATIVO", "RETALHO", "DESBRIDAMENTO"]):
            procedimento = linha.strip()

            if atendimento_atual:
                registros.append({
                    "atendimento": atendimento_atual,
                    "paciente": paciente_atual,
                    "convenio": convenio_atual,
                    "data": data_atual,
                    "profissional": profissional_atual,
                    "procedimento": procedimento
                })

    return registros
