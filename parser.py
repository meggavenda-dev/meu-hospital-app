
import pandas as pd

def parse_csv(file):
    df = pd.read_csv(file, dtype=str, encoding="latin1")
    df = df.fillna("")
    registros = []

    for _, row in df.iterrows():
        atendimento = row["Atendimento"].strip()
        paciente = row["Paciente"].strip()
        convenio = row["Convênio"].strip()
        proc = row["Cirurgia"].strip()
        inicio = row["Hora Início - Fim"].split(" ")[0] if "Hora" in row else ""
        profissional = row["Prestador"].strip()
        
        if atendimento:
            registros.append({
                "atendimento": atendimento,
                "paciente": paciente,
                "convenio": convenio,
                "procedimento": proc,
                "data": row.get("Data", ""),
                "profissional": profissional
            })

    return registros
