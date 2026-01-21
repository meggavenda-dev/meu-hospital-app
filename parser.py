
import re

def _tail(cols, n):
    """Retorna as n últimas colunas (com padding à esquerda se faltar), sem perder vazios do meio."""
    pad = [""] * max(0, n - len(cols))
    return (pad + cols)[-n:]

def _clean(s):
    return s.strip().strip('"').strip()

def parse_csv_text(csv_text: str):
    """
    Lê o relatório 'CSV-like' real e devolve uma lista de dicts normalizados:
    atendimento, paciente, data, procedimento, convenio, profissional, anestesista, tipo, quarto, hora_ini, hora_fim
    """
    internacoes = []
    data_atual = None
    atual = None  # último bloco (internação) ativo
    hora_ini_mestre = ""
    hora_fim_mestre = ""

    for raw in csv_text.splitlines():
        linha = raw.replace("\x00", "").rstrip("\n")
        if not linha or linha.strip() == "":
            continue

        # 0) Data do bloco
        if "Data de Realização" in linha:
            partes = [p.strip() for p in linha.split(",")]
            for p in partes:
                if re.fullmatch(r"\d{2}/\d{2}/\d{4}", p):
                    data_atual = p
            continue

        # 1) Linha-mestre: começa com vírgula + atendimento numérico
        if re.match(r"^,\s*\d{7,12},", raw):
            cols = [c for c in raw.split(",")]  # preserva vazios intermediários

            # Lado direito (sempre ancorado)
            quarto, tipo, anest, prest, conv = map(_clean, _tail(cols, 1+2+3+4+5)[:5][::-1])  # vamos re-mapear já já
            # O _tail acima não está na ordem; para ficar explícito:
            # Pegue as 5 últimas de uma vez, na ordem correta:
            conv, prest, anest, tipo, quarto = map(_clean, _tail(cols, 5))

            # Cirurgia é a 6ª a partir do fim
            procedimento = _clean(_tail(cols, 6)[0])

            # Horas (8 e 7 a partir do fim)
            hora_ini = _clean(_tail(cols, 8)[2])  # posição -8
            hora_fim = _clean(_tail(cols, 7)[1])  # posição -7

            # Aviso (9 a partir do fim)
            aviso = _clean(_tail(cols, 9)[0])

            # Do lado esquerdo: primeiro não-vazio é atendimento; o próximo não-vazio é paciente (se houver)
            # (há um vazio inicial por causa da vírgula inicial)
            esquerda = [c.strip() for c in cols]
            # ignore leading empties
            i = 0
            while i < len(esquerda) and esquerda[i] == "":
                i += 1
            atendimento = esquerda[i] if i < len(esquerda) else ""
            # próximo não-vazio (pode não existir)
            j = i + 1
            while j < len(esquerda) and esquerda[j] == "":
                j += 1
            paciente = esquerda[j] if j < len(esquerda) else ""

            # guarda horas da linha-mestre para usar nas filhas
            hora_ini_mestre, hora_fim_mestre = hora_ini, hora_fim

            # inicia nova internação
            atual = {
                "data": data_atual or "",
                "atendimento": atendimento,
                "paciente": paciente,
                "hora_ini": hora_ini,
                "hora_fim": hora_fim,
                "procedimentos": []
            }
            # primeiro procedimento
            if procedimento or conv or prest or anest or tipo or quarto:
                atual["procedimentos"].append({
                    "procedimento": procedimento,
                    "convenio": conv,
                    "profissional": prest,       # prestador = cirurgião/equipe
                    "anestesista": anest,
                    "tipo": tipo,
                    "quarto": quarto,
                    "hora_ini": hora_ini,
                    "hora_fim": hora_fim
                })

            internacoes.append(atual)
            continue

        # 2) Linhas-filhas: começam com >=10 vírgulas
        if re.match(r"^,{10,}", raw):
            cols = [c for c in raw.split(",")]

            conv, prest, anest, tipo, quarto = map(_clean, _tail(cols, 5))
            procedimento = _clean(_tail(cols, 6)[0])

            if atual:  # herda hora da mestre
                atual["procedimentos"].append({
                    "procedimento": procedimento,
                    "convenio": conv,
                    "profissional": prest,
                    "anestesista": anest,
                    "tipo": tipo,
                    "quarto": quarto,
                    "hora_ini": hora_ini_mestre,
                    "hora_fim": hora_fim_mestre
                })
            continue

        # 3) Totais: ignorar
        if "Total de Avisos" in linha or "Total de Cirurgias" in linha:
            continue

        # 4) Demais linhas: ignorar silenciosamente (cabeçalhos intermediários etc.)
        continue

    # Expande em registros "flat" (um por procedimento)
    registros = []
    for it in internacoes:
        for p in it["procedimentos"]:
            registros.append({
                "atendimento": it["atendimento"],
                "paciente": it["paciente"],
                "data": it["data"],
                "procedimento": p["procedimento"],
                "convenio": p["convenio"],
                "profissional": p["profissional"],
                "anestesista": p["anestesista"],
                "tipo": p["tipo"],
                "quarto": p["quarto"],
                "hora_ini": p["hora_ini"],
                "hora_fim": p["hora_fim"]
            })
    return registros

