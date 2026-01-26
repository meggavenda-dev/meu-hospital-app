def parse_tiss_original(csv_text):
    import re, csv, io

    def clean(s: str) -> str:
        return (s or "").replace("\x00", "").strip().strip('"').strip()

    def is_time(s: str) -> bool:
        return bool(re.fullmatch(r"\d{1,2}:\d{2}", s or ""))

    def is_digits(s: str) -> bool:
        return bool(re.fullmatch(r"\d{3,}", s or ""))

    def last_n_nonempty(seq, n):
        out = [clean(v) for v in reversed(seq) if clean(v) != ""]
        out = out[:n]
        out.reverse()
        if len(out) < n: out = [""] * (n - len(out)) + out
        return out

    reader = csv.reader(io.StringIO(csv_text), delimiter=",", quotechar='"')
    registros = []
    data_atual = ""
    contexto = {"atendimento": "", "paciente": "", "hora_ini": "", "hora_fim": "", "aviso": ""}
    row_idx = 0

    for cols in reader:
        cols = [clean(c) for c in cols]
        if all(c == "" for c in cols): continue
        line_txt = " ".join(cols)

        # 1. DATA DO BLOCO (Busca flexível para evitar erro de acentuação)
        if "Data de Realiza" in line_txt:
            for c in cols:
                if re.fullmatch(r"\d{2}/\d{2}/\d{4}", c):
                    data_atual = c
                    break
            continue

        # Ignorar cabeçalhos
        if ("Hora" in line_txt and "Início" in line_txt) or any(k in line_txt for k in ["Atendimento", "Convênio", "Total de Avisos"]):
            continue

        # 2. LINHA-MESTRE (Identifica pelo atendimento na col 1)
        is_master = (len(cols) >= 2 and re.fullmatch(r"\d{7,12}", cols[1]))
        if is_master:
            atendimento = cols[1]
            paciente = cols[2] if len(cols) > 2 else ""
            
            # Localizar aviso e horas
            aviso_idx = None
            for k in range(3, len(cols) - 2):
                if is_digits(cols[k]) and is_time(cols[k+1]) and is_time(cols[k+2]):
                    aviso_idx = k
                    break
            
            if aviso_idx is not None:
                aviso = cols[aviso_idx]
                hora_ini, hora_fim = cols[aviso_idx+1], cols[aviso_idx+2]
                procedimento = cols[aviso_idx+3] if aviso_idx+3 < len(cols) else ""
                
                # Dados de Convênio/Profissional (Âncora pela direita)
                tail5 = last_n_nonempty(cols, 5)
                contexto = {"atendimento": atendimento, "paciente": paciente, "hora_ini": hora_ini, "hora_fim": hora_fim, "aviso": aviso}
                
                registros.append({
                    "atendimento": atendimento, "paciente": paciente, "data": data_atual, "aviso": aviso,
                    "procedimento": procedimento, "convenio": tail5[0], "profissional": tail5[1],
                    "anestesista": tail5[2], "tipo": tail5[3], "quarto": tail5[4],
                    "hora_ini": hora_ini, "hora_fim": hora_fim, "_row_idx": row_idx
                })
                row_idx += 1
                continue

        # 3. LINHA-FILHA (Onde geralmente estão os outros médicos)
        # Ajustado para aceitar linhas que começam vazias (atendimento oculto)
        first_idx = next((i for i, c in enumerate(cols) if c != ""), None)
        if first_idx is not None and first_idx >= 5: # Reduzido de 10 para 5
            procedimento = cols[first_idx]
            # No seu arquivo, Profissional costuma estar 2 colunas após o Convênio
            tail5 = last_n_nonempty(cols, 5)
            
            if contexto["atendimento"]:
                registros.append({
                    "atendimento": contexto["atendimento"], "paciente": contexto["paciente"], 
                    "data": data_atual, "aviso": contexto["aviso"],
                    "procedimento": procedimento, "convenio": tail5[0], "profissional": tail5[1],
                    "anestesista": tail5[2], "tipo": tail5[3], "quarto": tail5[4],
                    "hora_ini": contexto["hora_ini"], "hora_fim": contexto["hora_fim"], "_row_idx": row_idx
                })
                row_idx += 1
    return registros
