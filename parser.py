
import re

def parse_csv(file):
    # Lê o arquivo inteiro
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

        # Detecta a linha da data
        if "Data de Realização" in linha:
            partes = linha.split(",")
            for p in partes:
                p = p.strip()
                if re.match(r"\d{2}/\d{2}/\d{4}", p):
                    data_atual = p
            continue

        # Detecta linha mestre: começa com vírgula + atendimento numérico
        if re.match(r"^,\s*\d{7,12}", original):
            partes = original.split(",")

            atendimento = partes[1].strip()

            paciente = partes[2].strip() if len(partes) > 2 else ""

            # O procedimento principal fica em partes[10]
            procedimento = partes[10].strip() if len(partes) > 10 else ""

            convenio = partes[11].strip() if len(partes) > 11 else ""
            profissional = partes[12].strip() if len(partes) > 12 else ""

            # Registra o primeiro procedimento do atendimento
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

        # Detecta linha filha (procedimentos adicionais)
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
