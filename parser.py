
def parse_real(csv_text):

    internacoes = []
    data_atual = None
    atual = None  # internação corrente

    for raw in csv_text.splitlines():
        linha = raw.strip()

        if not linha:
            continue

        # 1) Detecta nova data
        if "Data de Realização" in linha:
            partes = linha.split(",")
            for p in partes:
                p = p.strip()
                if re.match(r"\d{2}/\d{2}/\d{4}", p):
                    data_atual = p
            continue

        # 2) Linha mestre
        if re.match(r"^,\s*\d{7,12},", raw):
            cols = raw.split(",")

            atendimento = cols[1].strip()
            paciente = cols[2].strip()

            aviso = cols[7].strip()
            hora_ini = cols[8].strip()
            hora_fim = cols[9].strip()

            procedimento = cols[10].strip()
            convenio = cols[11].strip()
            prestador = cols[12].strip() if len(cols) > 12 else ""
            anest = cols[13].strip() if len(cols) > 13 else ""
            tipo = cols[14].strip() if len(cols) > 14 else ""
            quarto = cols[15].strip() if len(cols) > 15 else ""

            # cria nova internação
            atual = {
                "data": data_atual,
                "atendimento": atendimento,
                "paciente": paciente,
                "hora_inicio": hora_ini,
                "hora_fim": hora_fim,
                "procedimentos": []
            }

            atual["procedimentos"].append({
                "procedimento": procedimento,
                "convenio": convenio,
                "prestador": prestador,
                "anestesista": anest,
                "tipo": tipo,
                "quarto": quarto
            })

            internacoes.append(atual)
            continue

        # 3) Linhas filhas — procedimentos extras
        if re.match(r"^,{10,}", raw):
            cols = raw.split(",")

            procedimento = cols[10].strip()
            convenio = cols[11].strip()
            prestador = cols[12].strip() if len(cols) > 12 else ""
            anest = cols[13].strip() if len(cols) > 13 else ""
            tipo = cols[14].strip() if len(cols) > 14 else ""
            quarto = cols[15].strip() if len(cols) > 15 else ""

            if atual:
                atual["procedimentos"].append({
                    "procedimento": procedimento,
                    "convenio": convenio,
                    "prestador": prestador,
                    "anestesista": anest,
                    "tipo": tipo,
                    "quarto": quarto
                })
            continue

        # 4) Totais — ignorar
        if "Total de Avisos" in linha or "Total de Cirurgias" in linha:
            continue

    return internacoes
