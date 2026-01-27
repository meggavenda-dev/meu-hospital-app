# parser.py — versão corrigida COMPLETA
# Mantém 100% da lógica original + garante separação por Data de Realização
# + CORRIGE DETECÇÃO DO ATENDIMENTO (coluna 0/1/2)
# + CORRIGE AVISO/HORAS EM COLUNAS DESLOCADAS

import re
import csv
import io


def parse_tiss_original(csv_text):
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

    reader = csv.reader(io.StringIO(csv_text), delimiter=",", quotechar='"')

    registros = []
    data_atual = None
    contexto = {
        "atendimento": "",
        "paciente": "",
        "hora_ini": "",
        "hora_fim": "",
        "aviso": "",
    }

    for cols in reader:
        cols = [clean(c) for c in cols]

        # linha vazia → ignora
        if all(c == "" for c in cols):
            continue

        line_txt = " ".join(cols)

        # -------------------------------------------------------
        # BLOCO DE DATA
        # -------------------------------------------------------
        if any("Data de Realização" in c for c in cols):
            for c in cols:
                if re.fullmatch(r"\d{2}/\d{2}/\d{4}", c):
                    data_atual = c
                    break

            # reset ao trocar de dia
            contexto = {"atendimento": "", "paciente": "", "hora_ini": "", "hora_fim": "", "aviso": ""}
            continue

        if not data_atual:
            continue

        # -------------------------------------------------------
        # IGNORAR CABEÇALHOS
        # -------------------------------------------------------
        if (
            ("Hora" in line_txt and "Início" in line_txt)
            or any(
                k in line_txt
                for k in [
                    "Atendimento",
                    "Convênio",
                    "Centro Cirurgico",
                    "HEMODINAMICA",
                    "OBSTETRICO",
                    "Total de Avisos",
                    "Total de Cirurgias",
                    "Total Geral",
                ]
            )
        ):
            continue

        # -------------------------------------------------------
        # DETECÇÃO ROBUSTA DO ATENDIMENTO (coluna 0,1,2)
        # -------------------------------------------------------
        att_col = None
        for idx in (0, 1, 2):
            if idx < len(cols) and re.fullmatch(r"\d{7,12}", cols[idx]):
                att_col = idx
                break

        is_master = att_col is not None

        # -------------------------------------------------------
        # LINHA-MESTRE
        # -------------------------------------------------------
        if is_master:
            atendimento = cols[att_col]
            paciente = cols[att_col + 1] if len(cols) > att_col + 1 else ""

            # ----- localizar aviso + horas -----
            aviso_idx = None

            # padrão ideal: aviso, hora_ini, hora_fim
            for k in range(att_col + 2, len(cols) - 2):
                if is_digits(cols[k]) and is_time(cols[k + 1]) and is_time(cols[k + 2]):
                    aviso_idx = k
                    break

            # fallback
            if aviso_idx is None:
                for k in range(att_col + 2, len(cols)):
                    if is_time(cols[k]) and k - 1 >= 0 and is_digits(cols[k - 1]):
                        aviso_idx = k - 1
                        break

            if aviso_idx is None:
                # linha com problema → ignora
                continue

            aviso = cols[aviso_idx]
            hora_ini = cols[aviso_idx + 1]
            hora_fim = cols[aviso_idx + 2]
            proc_idx = aviso_idx + 3

            procedimento = cols[proc_idx] if proc_idx < len(cols) else ""

            # Convênio, prestador, anestesista, tipo, quarto
            tail5 = last_n_nonempty(cols, 5)

            conv = cols[proc_idx + 1] if proc_idx + 1 < len(cols) and cols[proc_idx + 1] != "" else tail5[0]
            prest = cols[proc_idx + 2] if proc_idx + 2 < len(cols) and cols[proc_idx + 2] != "" else tail5[1]
            anest = cols[proc_idx + 3] if proc_idx + 3 < len(cols) and cols[proc_idx + 3] != "" else tail5[2]
            tipo, quarto = tail5[3], tail5[4]

            # salvar contexto
            contexto = {
                "atendimento": atendimento,
                "paciente": paciente,
                "hora_ini": hora_ini,
                "hora_fim": hora_fim,
                "aviso": aviso,
            }

            registros.append(
                {
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
                    "hora_fim": hora_fim,
                    "__cells__": cols,
                }
            )

            continue

        # -------------------------------------------------------
        # LINHA-FILHA
        # -------------------------------------------------------
        first_idx = next((i for i, c in enumerate(cols) if c != ""), None)

        if first_idx is not None and first_idx >= 10:
            proc_idx = first_idx

            procedimento = cols[proc_idx]
            conv = cols[proc_idx + 1] if proc_idx + 1 < len(cols) else ""
            prest = cols[proc_idx + 2] if proc_idx + 2 < len(cols) else ""
            anest = cols[proc_idx + 3] if proc_idx + 3 < len(cols) else ""

            tail2 = last_n_nonempty(cols, 2)
            tipo = tail2[0] if len(tail2) >= 1 else ""
            quarto = tail2[1] if len(tail2) >= 2 else ""

            if contexto["atendimento"]:
                registros.append(
                    {
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
                        "hora_fim": contexto["hora_fim"],
                        "__cells__": cols,
                    }
                )

            continue

        # caso contrário → ignora
        continue

    return registros
