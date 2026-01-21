
def parse_tiss_original(csv_text):
    import re

    linhas = csv_text.splitlines()
    registros = []

    data_atual = ""
    atual_atendimento = None
    atual_paciente = None
    atual_hora_ini = None
    atual_hora_fim = None

    # detecta linhas-mestre
    regex_mestre = re.compile(r"^,\s*\d{7,12},")
    # detecta filhas
    regex_filha = re.compile(r"^,{10,}")

    for raw in linhas:
        ln = raw.replace("\x00","").rstrip("\n")

        # detector de data
        if "Data de Realização" in ln:
            partes = ln.split(",")
            for p in partes:
                if re.fullmatch(r"\d{2}/\d{2}/\d{4}", p.strip()):
                    data_atual = p.strip()
            continue

        # ignorar cabeçalhos internos
        if (
            "Hora" in ln and "Início" in ln
            or ln.startswith("Atendimento")
            or "Convênio" in ln and "Prestador" in ln
            or "Centro Cirurgico" in ln
            or "Total" in ln
        ):
            continue

        # =============================
        # LINHA MESTRE
        # =============================
        if regex_mestre.match(ln):
            cols = [c.strip() for c in ln.split(",")]

            # atendimento
            atendimento = cols[1]

            # paciente (pode estar vazio)
            paciente = cols[2] if cols[2] else ""

            # aviso = cols[7] mas não utilizamos no sistema
            hora_ini = cols[8]
            hora_fim = cols[9]

            # procedimento mestre
            procedimento = cols[10]

            convenio   = cols[11]
            prestador  = cols[12]
            anest      = cols[13]
            tipo       = cols[14]
            quarto     = cols[15]

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
                "anestesista": anest,
                "tipo": tipo,
                "quarto": quarto,
                "hora_ini": hora_ini,
                "hora_fim": hora_fim
            })
            continue

        # =============================
        # LINHA-FILHA (procedimentos adicionais)
        # =============================
        if regex_filha.match(ln):
            cols = [c.strip() for c in ln.split(",")]

            procedimento = cols[10]
            convenio   = cols[11]
            prestador  = cols[12]
            anest      = cols[13]
            tipo       = cols[14]
            quarto     = cols[15]

            registros.append({
                "atendimento": atual_atendimento,
                "paciente": atual_paciente,
                "data": data_atual,
                "procedimento": procedimento,
                "convenio": convenio,
                "profissional": prestador,
                "anestesista": anest,
                "tipo": tipo,
                "quarto": quarto,
                "hora_ini": atual_hora_ini,
                "hora_fim": atual_hora_fim
            })
            continue

    return registros
