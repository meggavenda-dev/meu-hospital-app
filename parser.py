
import re
import csv
import io

def parse_tiss_original(csv_text):
    """
    Parser robusto para o relatório do Centro Cirúrgico/Hemodinâmica/Obstétrico.
    - Usa csv.reader para respeitar campos com vírgulas entre aspas.
    - Linha-mestre: detecta atendimento (7-12 dígitos), acha 'aviso' (número) e,
      logo depois, duas horas HH:MM; o campo seguinte é o 'procedimento'.
    - Convênio/Prestador/Anestesista: preferimos os campos logo após 'procedimento'
      quando existirem; 'tipo' e 'quarto' são ancorados pelos 2 últimos campos não vazios.
    - Linha-filha: 10+ vazios à esquerda; herda hora_ini/hora_fim/aviso da mestre.
    - Ignora cabeçalhos/totais/seções reentrantes.
    """

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

    data_atual = ""
    contexto = {"atendimento": "", "paciente": "", "hora_ini": "", "hora_fim": "", "aviso": ""}

    for cols in reader:
        cols = [clean(c) for c in cols]
        if all(c == "" for c in cols):
            continue

        line_txt = " ".join(cols)

        # Data do bloco
        if any("Data de Realização" in c for c in cols):
            for c in cols:
                if re.fullmatch(r"\d{2}/\d{2}/\d{4}", c):
                    data_atual = c
                    break
            continue

        # Ignorar cabeçalhos/totais
        if (
            ("Hora" in line_txt and "Início" in line_txt)
            or any(k in line_txt for k in [
                "Atendimento", "Convênio", "Centro", "Centro Cirurgico",
                "HEMODINAMICA", "OBSTETRICO", "Total de Avisos",
                "Total de Cirurgias", "Total Geral"
            ])
        ):
            continue

        # Linha-mestre
        is_master = (len(cols) >= 2 and re.fullmatch(r"\d{7,12}", cols[1] or ""))
        if is_master:
            atendimento = cols[1]
            paciente    = cols[2] if len(cols) > 2 else ""

            # achar 'aviso' (número) + horas subsequentes
            aviso_idx = None
            for k in range(3, len(cols) - 2):
                if is_digits(cols[k]) and is_time(cols[k+1]) and is_time(cols[k+2]):
                    aviso_idx = k
                    break
            if aviso_idx is None:
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

            # Âncora pela direita: conv, prest, anest, tipo, quarto
            conv = prest = anest = tipo = quarto = ""
            tail5 = last_n_nonempty(cols, 5)

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

            tipo, quarto = tail5[3], tail5[4]

            contexto = {
                "atendimento": atendimento,
                "paciente": paciente,
                "hora_ini": hora_ini,
                "hora_fim": hora_fim,
                "aviso": aviso
            }

            registros.append({
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
            })
            continue

        # Linha-filha (procedimento extra)
        first_idx = next((i for i, c in enumerate(cols) if c != ""), None)
        if first_idx is not None and first_idx >= 10:
            proc_idx = first_idx
            procedimento = cols[proc_idx]

            conv  = cols[proc_idx + 1] if proc_idx + 1 < len(cols) else ""
            prest = cols[proc_idx + 2] if proc_idx + 2 < len(cols) else ""
            anest = cols[proc_idx + 3] if proc_idx + 3 < len(cols) else ""

            tipo = quarto = ""
            tail2 = last_n_nonempty(cols, 2)
            if len(tail2) == 2:
                tipo, quarto = tail2[0], tail2[1]
            elif len(tail2) == 1:
                quarto = tail2[0]

            if contexto["atendimento"]:
                registros.append({
                    "atendimento": contexto["atendimento"],
                    "paciente": "",  # já veio na mestre
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
                })
            continue

        # Demais linhas: ignorar
        continue

    return registros
