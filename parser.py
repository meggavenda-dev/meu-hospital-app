
def parse_tiss_original(csv_text):
    import re

    linhas = csv_text.splitlines()
    registros = []

    regex_mestre = re.compile(r"^,\s*\d{7,12},")
    regex_filha  = re.compile(r"^,{10,}")

    data_atual = ""
    atual_atendimento = None
    atual_paciente = None
    atual_hora_ini = None
    atual_hora_fim = None

    for raw in linhas:
        ln = raw.replace("\x00","").rstrip("\n")

        # DATA
        if "Data de Realização" in ln:
            partes = ln.split(",")
            for p in partes:
                p = p.strip()
                if re.fullmatch(r"\d{2}/\d{2}/\d{4}", p):
                    data_atual = p
            continue

        # ignorar cabeçalhos e totais
        if (
            "Hora" in ln and "Início" in ln
            or ln.startswith("Atendimento")
            or "Convênio" in ln
            or "Centro Cirurgico" in ln
            or "Total" in ln
        ):
            continue

        # ---------------------------
        # LINHA-MESTRE
        # ---------------------------
        if regex_mestre.match(ln):
            cols = [c.strip() for c in ln.split(",")]

            atendimento = cols[1]
            paciente    = cols[2]

            aviso       = cols[8]
            hora_ini    = cols[9]
            hora_fim    = cols[10]

            procedimento = cols[11]
            convenio     = cols[12]
            prestador    = cols[13]
            anestesista  = cols[14]
            tipo         = cols[15]
            quarto       = cols[16]

            atual_atendimento = atendimento
            atual_paciente    = paciente
            atual_hora_ini    = hora_ini
            atual_hora_fim    = hora_fim

            registros.append({
                "atendimento": atendimento,
                "paciente": paciente,
                "data": data_atual,
                "procedimento": procedimento,
                "convenio": convenio,
                "profissional": prestador,
                "anestesista": anestesista,
                "tipo": tipo,
                "quarto": quarto,
                "hora_ini": hora_ini,
                "hora_fim": hora_fim
            })
            continue

        # ---------------------------
        # LINHA-FILHA (procedimento extra)
        # ---------------------------
        if regex_filha.match(ln):
            cols = [c.strip() for c in ln.split(",")]

            procedimento = cols[10]
            convenio     = cols[11]
            prestador    = cols[12]
            anestesista  = cols[13]
            tipo         = cols[14]
            quarto       = cols[15]

            registros.append({
                "atendimento": atual_atendimento,
                "paciente": atual_paciente,
                "data": data_atual,
                "procedimento": procedimento,
                "convenio": convenio,
                "profissional": prestador,
                "anestesista": anestesista,
                "tipo": tipo,
                "quarto": quarto,
                "hora_ini": atual_hora_ini,
                "hora_fim": atual_hora_fim
            })
            continue

    return registros
