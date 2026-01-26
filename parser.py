
def parse_tiss_original(csv_text, prefer_prestador: str = None, todos_prestadores_marcados: bool = True):
    """
    Parser robusto para o relatório do Centro Cirúrgico/Hemodinâmica/Obstétrico.
    - Usa csv.reader para respeitar campos com vírgulas entre aspas.
    - Linha-mestre: detecta atendimento (7-12 dígitos), acha 'aviso' (número) e
      logo depois duas horas HH:MM; o campo seguinte é o 'procedimento'.
    - Convênio/Prestador/Anestesista: preferimos os campos logo após 'procedimento'
      quando existirem; 'tipo' e 'quarto' são ancorados pelos 2 últimos campos não vazios.
    - Linha-filha: 10+ vazios à esquerda; herda hora_ini/hora_fim e 'aviso' da mestre.

    NOVO:
    - parametros prefer_prestador (str) e todos_prestadores_marcados (bool).
      Quando todos_prestadores_marcados=False e prefer_prestador aparece em
      QUALQUER linha do bloco (mestre + filhas), sobrescrevemos o 'profissional'
      de TODO o bloco para o prefer_prestador.
    """
    import re, csv, io, unicodedata

    def clean(s: str) -> str:
        return (s or "").replace("\x00", "").strip().strip('"').strip()

    def is_time(s: str) -> bool:
        return bool(re.fullmatch(r"\d{1,2}:\d{2}", s or ""))

    def is_digits(s: str) -> bool:
        return bool(re.fullmatch(r"\d{3,}", s or ""))

    def last_n_nonempty(seq, n):
        out = []
        for v in reversed(seq):
            v2 = clean(v)
            if v2 != "":
                out.append(v2)
                if len(out) == n:
                    break
        out.reverse()
        if len(out) < n:
            out = [""] * (n - len(out)) + out
        return out

    # normalização robusta para comparar nomes de prestadores
    def _strip_accents(txt: str) -> str:
        txt = txt or ""
        nfkd = unicodedata.normalize("NFKD", txt)
        return "".join(c for c in nfkd if not unicodedata.combining(c))

    def norm_name(s: str) -> str:
        s = _strip_accents(s)
        s = re.sub(r"\s+", " ", s).strip()
        return s.casefold()

    prefer_norm = norm_name(prefer_prestador) if prefer_prestador else None

    reader = csv.reader(io.StringIO(csv_text), delimiter=",", quotechar='"')
    registros = []

    data_atual = ""
    # contexto do bloco corrente (mestre + filhas)
    contexto = {
        "atendimento": "", "paciente": "", "hora_ini": "", "hora_fim": "", "aviso": "",
        # NOVO: controle do bloco para eventual sobrescrita
        "idxs_bloco": [],               # índices no vetor 'registros' que pertencem ao bloco
        "prestadores_vistos": set(),    # nomes de prestadores já vistos no bloco
        "sobrescrito": False            # já aplicamos override neste bloco?
    }

    def inicia_bloco():
        # reset do contexto de bloco
        contexto["idxs_bloco"].clear()
        contexto["prestadores_vistos"].clear()
        contexto["sobrescrito"] = False

    inicia_bloco()

    def talvez_sobrescrever_prestador(prestador_encontrado: str):
        """
        Se a UI não está com 'Todos os prestadores' marcados, e o prestador preferido
        apareceu em alguma linha do bloco, sobrescreve 'profissional' em TODAS as
        linhas do bloco para o preferido (com a grafia exata que encontramos agora).
        """
        if todos_prestadores_marcados or not prefer_norm or contexto["sobrescrito"]:
            return
        if norm_name(prestador_encontrado) == prefer_norm:
            # aplica override no bloco
            for i in contexto["idxs_bloco"]:
                registros[i]["profissional"] = prestador_encontrado
            contexto["sobrescrito"] = True  # evita reaplicar

    for cols in reader:
        cols = [clean(c) for c in cols]
        if all(c == "" for c in cols):
            continue

        line_txt = " ".join(cols)

        # DATA DO BLOCO
        if any("Data de Realização" in c for c in cols):
            for c in cols:
                if re.fullmatch(r"\d{2}/\d{2}/\d{4}", c):
                    data_atual = c
                    break
            # inicia um novo bloco lógico de data (não interfere no bloco mestre/filha)
            continue

        # Ignorar cabeçalhos/totais/seções
        if (
            ("Hora" in line_txt and "Início" in line_txt)
            or any(k in line_txt for k in [
                "Atendimento", "Convênio", "Centro Cirurgico", "HEMODINAMICA", "OBSTETRICO",
                "Total de Avisos", "Total de Cirurgias", "Total Geral"
            ])
        ):
            continue

        # ---------------------------
        # LINHA-MESTRE
        # ---------------------------
        is_master = (len(cols) >= 2 and re.fullmatch(r"\d{7,12}", cols[1] or ""))
        if is_master:
            # iniciar novo bloco de mestre+filhas
            inicia_bloco()

            atendimento = cols[1]
            paciente    = cols[2] if len(cols) > 2 else ""

            # achar 'aviso' (número) seguido de duas horas
            aviso_idx = None
            for k in range(3, len(cols) - 2):
                if is_digits(cols[k]) and is_time(cols[k+1]) and is_time(cols[k+2]):
                    aviso_idx = k
                    break
            if aviso_idx is None:
                # fallback: primeiro horário cujo anterior é um número (aviso)
                for k in range(3, len(cols)):
                    if is_time(cols[k]) and k - 1 >= 0 and is_digits(cols[k-1]):
                        aviso_idx = k - 1
                        break
            if aviso_idx is None:
                # linha inconsistente
                continue

            aviso    = cols[aviso_idx]
            hora_ini = cols[aviso_idx + 1] if aviso_idx + 1 < len(cols) else ""
            hora_fim = cols[aviso_idx + 2] if aviso_idx + 2 < len(cols) else ""
            proc_idx = aviso_idx + 3
            procedimento = cols[proc_idx] if proc_idx < len(cols) else ""

            # ÂNCORA PELA DIREITA: 5 últimos não vazios tendem a ser [conv, prest, anest, tipo, quarto]
            conv = prest = anest = tipo = quarto = ""
            tail5 = last_n_nonempty(cols, 5)

            # Preferir os campos imediatamente após o procedimento, se existirem
            if proc_idx + 1 < len(cols) and cols[proc_idx + 1] != "":
                conv = cols[proc_idx + 1]
            else:
                conv = tail5[0]

            if proc_idx + 2 < len(cols) and cols[proc_idx + 2] != "":
                prest = cols[proc_idx + 2]
            else:
                prest = tail5[1]

            if proc_idx + 3 < len(cols) and cols[proc_idx + 3] != "":
                anest = cols[proc_idx + 3]
            else:
                anest = tail5[2]

            # tipo e quarto — normalmente os 2 últimos campos não vazios
            tipo, quarto = tail5[3], tail5[4]

            # contexto para filhas
            contexto.update({
                "atendimento": atendimento,
                "paciente": paciente,
                "hora_ini": hora_ini,
                "hora_fim": hora_fim,
                "aviso": aviso
            })

            # cria registro da linha-mestre
            rec = {
                "atendimento": atendimento,
                "paciente": paciente,
                "data": data_atual,
                "aviso": aviso,
                "procedimento": procedimento,
                "convenio": conv,
                "profissional": prest,
                "anestesista": anest,
                "tipo": tipo,
                "quarto": quarto,
                "hora_ini": hora_ini,
                "hora_fim": hora_fim
            }
            registros.append(rec)
            contexto["idxs_bloco"].append(len(registros) - 1)
            if prest:
                contexto["prestadores_vistos"].add(prest)
                # se o próprio mestre já for o preferido, aplica imediatamente
                talvez_sobrescrever_prestador(prest)

            continue

        # ---------------------------
        # LINHA-FILHA
        # ---------------------------
        # primeira coluna não vazia em posição >= 10 caracteriza filha
        first_idx = next((i for i, c in enumerate(cols) if c != ""), None)
        if first_idx is not None and first_idx >= 10:
            proc_idx = first_idx
            procedimento = cols[proc_idx]

            conv  = cols[proc_idx + 1] if proc_idx + 1 < len(cols) else ""
            prest = cols[proc_idx + 2] if proc_idx + 2 < len(cols) else ""
            anest = cols[proc_idx + 3] if proc_idx + 3 < len(cols) else ""

            # tipo/quarto ancorados pelos 2 últimos não vazios
            tipo = quarto = ""
            tail2 = last_n_nonempty(cols, 2)
            if len(tail2) == 2:
                tipo, quarto = tail2[0], tail2[1]
            elif len(tail2) == 1:
                quarto = tail2[0]

            if contexto["atendimento"]:
                rec = {
                    "atendimento": contexto["atendimento"],
                    "paciente": contexto["paciente"],
                    "data": data_atual,
                    "aviso": contexto["aviso"],
                    "procedimento": procedimento,
                    "convenio": conv,
                    "profissional": prest,
                    "anestesista": anest,
                    "tipo": tipo,
                    "quarto": quarto,
                    "hora_ini": contexto["hora_ini"],
                    "hora_fim": contexto["hora_fim"]
                }
                registros.append(rec)
                contexto["idxs_bloco"].append(len(registros) - 1)
                if prest:
                    contexto["prestadores_vistos"].add(prest)
                    # se encontramos o preferido numa filha, forçamos override no bloco
                    talvez_sobrescrever_prestador(prest)

            continue

        # Demais linhas: ignorar
        continue

    return registros
