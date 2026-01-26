def parse_tiss_original(csv_text):
    import re, csv, io

    def clean(s: str) -> str:
        return (s or "").replace("\x00", "").strip().strip('"').strip()

    reader = csv.reader(io.StringIO(csv_text), delimiter=",", quotechar='"')
    registros = []
    data_atual = ""
    # Contexto para herança de dados das linhas filhas
    ctx = {"atendimento": "", "paciente": "", "aviso": "", "convenio": ""}

    for cols in reader:
        # Normaliza a linha para ter pelo menos 12 colunas para evitar IndexError
        cols = [clean(c) for c in cols]
        while len(cols) < 12: cols.append("")
        
        line_txt = " ".join(cols)

        # 1. Detecta Mudança de Data
        if "Data de Realiza" in line_txt:
            for c in cols:
                if re.fullmatch(r"\d{2}/\d{2}/\d{4}", c):
                    data_atual = c
                    break
            continue

        # 2. Identifica Linha-Mestre (Onde nasce o atendimento)
        # Verifica se a coluna 1 (índice 1) tem um número de 7 a 12 dígitos
        if re.fullmatch(r"\d{7,12}", cols[1]):
            ctx = {
                "atendimento": cols[1],
                "paciente": cols[2],
                "aviso": cols[3],
                "convenio": cols[7] if cols[7] else ""
            }
            
            # Se a linha mestre já tem médico (índice 8)
            if cols[8]:
                registros.append({
                    "atendimento": ctx["atendimento"], "paciente": ctx["paciente"],
                    "data": data_atual, "aviso": ctx["aviso"],
                    "procedimento": cols[6], "convenio": ctx["convenio"],
                    "profissional": cols[8], "hora_ini": cols[4], "hora_fim": cols[5]
                })
            continue

        # 3. Identifica Linha-Filha (Procedimentos extras ou médicos auxiliares)
        # Se a coluna de atendimento está vazia, mas a de procedimento (6) e médico (8) têm dados
        if not cols[1] and cols[6] and cols[8] and ctx["atendimento"]:
            registros.append({
                "atendimento": ctx["atendimento"], "paciente": ctx["paciente"],
                "data": data_atual, "aviso": ctx["aviso"],
                "procedimento": cols[6], 
                "convenio": cols[7] if cols[7] else ctx["convenio"],
                "profissional": cols[8], "hora_ini": "", "hora_fim": ""
            })

    return registros
