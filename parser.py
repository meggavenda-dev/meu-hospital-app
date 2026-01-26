def parse_tiss_original(csv_text):
    import re, csv, io

    def clean(s: str) -> str:
        # Remove caracteres nulos e espaços extras
        return (s or "").replace("\x00", "").strip().strip('"').strip()

    f = io.StringIO(csv_text)
    reader = csv.reader(f, delimiter=",", quotechar='"')
    
    registros = []
    data_atual = ""
    # Armazena os dados da "Linha Mestre" para aplicar às "Linhas Filhas"
    ctx = {"atendimento": "", "paciente": "", "aviso": "", "convenio": ""}

    for cols in reader:
        cols = [clean(c) for c in cols]
        if not any(cols): continue # Pula linhas totalmente vazias
        
        line_txt = " ".join(cols)

        # 1. Identifica a Data (Ex: 12/11/2025)
        if "Data de Realiza" in line_txt:
            for c in cols:
                if re.fullmatch(r"\d{2}/\d{2}/\d{4}", c):
                    data_atual = c
                    break
            continue

        # Pula cabeçalhos do relatório
        if "Atendimento" in line_txt and "Paciente" in line_txt:
            continue

        # 2. Identifica Linha-Mestre (Início de um atendimento)
        # O número do atendimento está sempre no índice 1 (segunda coluna)
        if len(cols) > 1 and re.fullmatch(r"\d{7,12}", cols[1]):
            ctx = {
                "atendimento": cols[1],
                "paciente": cols[2], # Se estiver vazio, o contexto guarda vazio (mas o atendimento existe!)
                "aviso": cols[3] if len(cols) > 3 else "",
                "convenio": cols[7] if len(cols) > 7 else ""
            }
            
            # Se a linha mestre já tiver um profissional (índice 8)
            prof = cols[8] if len(cols) > 8 else ""
            if prof and data_atual:
                registros.append({
                    "atendimento": ctx["atendimento"],
                    "paciente": ctx["paciente"],
                    "data": data_atual,
                    "aviso": ctx["aviso"],
                    "procedimento": cols[6] if len(cols) > 6 else "",
                    "convenio": ctx["convenio"],
                    "profissional": prof,
                    "hora_ini": cols[4] if len(cols) > 4 else "",
                    "hora_fim": cols[5] if len(cols) > 5 else ""
                })
            continue

        # 3. Identifica Linha-Filha (Procedimentos adicionais ou outros médicos)
        # Atendimento (index 1) é vazio, mas Profissional (index 8) existe
        if len(cols) > 8 and not cols[1] and cols[8] and ctx["atendimento"]:
            registros.append({
                "atendimento": ctx["atendimento"],
                "paciente": ctx["paciente"],
                "data": data_atual,
                "aviso": ctx["aviso"],
                "procedimento": cols[6] if len(cols) > 6 else "",
                "convenio": cols[7] if len(cols) > 7 else ctx["convenio"],
                "profissional": cols[8],
                "hora_ini": "",
                "hora_fim": ""
            })

    return registros
