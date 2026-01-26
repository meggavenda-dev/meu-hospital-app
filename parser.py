def parse_tiss_original(csv_text):
    import re, csv, io

    def clean(s: str) -> str:
        return (s or "").replace("\x00", "").strip().strip('"').strip()

    reader = csv.reader(io.StringIO(csv_text), delimiter=",", quotechar='"')
    registros = []
    data_atual = ""
    # Armazena o contexto da última linha-mestre lida para aplicar às linhas-filhas
    ctx = {"atendimento": "", "paciente": "", "aviso": "", "hora_ini": "", "hora_fim": ""}

    for cols in reader:
        cols = [clean(c) for c in cols]
        if not any(cols): continue
        line_txt = " ".join(cols)

        # 1. Identifica a Data do Bloco
        if "Data de Realiza" in line_txt:
            for c in cols:
                if re.fullmatch(r"\d{2}/\d{2}/\d{4}", c):
                    data_atual = c
                    break
            continue

        # Pula cabeçalhos e linhas de total
        if any(k in line_txt for k in ["Atendimento", "Convênio", "Total de"]):
            continue

        # 2. Verifica se é Linha-Mestre (Atendimento na Coluna 1)
        is_master = (len(cols) > 1 and re.fullmatch(r"\d{7,12}", cols[1]))
        
        if is_master:
            atendimento = cols[1]
            paciente = cols[2] if len(cols) > 2 else ""
            aviso = cols[3] if len(cols) > 3 else ""
            hora_ini = cols[4] if len(cols) > 4 else ""
            hora_fim = cols[5] if len(cols) > 5 else ""
            
            ctx = {"atendimento": atendimento, "paciente": paciente, "aviso": aviso, "hora_ini": hora_ini, "hora_fim": hora_fim}
            
            # Profissional está sempre no índice 8
            prof = cols[8] if len(cols) > 8 else ""
            if prof:
                registros.append({
                    "atendimento": atendimento, "paciente": paciente, "data": data_atual,
                    "aviso": aviso, "procedimento": cols[6] if len(cols) > 6 else "",
                    "convenio": cols[7] if len(cols) > 7 else "", "profissional": prof,
                    "anestesista": cols[9] if len(cols) > 9 else "", 
                    "tipo": cols[10] if len(cols) > 10 else "", "quarto": cols[11] if len(cols) > 11 else "",
                    "hora_ini": hora_ini, "hora_fim": hora_fim
                })
            continue

        # 3. Verifica se é Linha-Filha (Atendimento vazio, mas tem procedimento no índice 6)
        if len(cols) > 8 and not cols[1] and cols[6] and ctx["atendimento"]:
            prof = cols[8]
            if prof:
                registros.append({
                    "atendimento": ctx["atendimento"], "paciente": ctx["paciente"], "data": data_atual,
                    "aviso": ctx["aviso"], "procedimento": cols[6],
                    "convenio": cols[7], "profissional": prof,
                    "anestesista": cols[9], "tipo": cols[10], "quarto": cols[11],
                    "hora_ini": ctx["hora_ini"], "hora_fim": ctx["hora_fim"]
                })

    return registros
