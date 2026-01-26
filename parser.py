import re
import csv
import io

def parse_tiss_original(csv_text):
    def clean(s: str) -> str:
        return (s or "").replace("\x00", "").strip().strip('"').strip()

    f = io.StringIO(csv_text)
    # Usamos o reader de CSV para tratar as aspas corretamente
    reader = csv.reader(f, delimiter=",", quotechar='"')
    
    registros = []
    data_atual = ""
    # Contexto do bloco de atendimento (Master -> Child)
    ctx = {"atendimento": "", "paciente": "", "aviso": "", "convenio": "", "time_idx": -1}

    for cols in reader:
        cols = [clean(c) for c in cols]
        if not any(cols): continue
        
        line_txt = " ".join(cols)

        # 1. Detecta Mudança de Data
        if "Data de Realiza" in line_txt:
            match = re.search(r'(\d{2}/\d{2}/\d{4})', line_txt)
            if match:
                data_atual = match.group(1)
            continue

        # 2. Identifica Linha-Mestre (Início de um atendimento)
        # O Atendimento está na segunda coluna (índice 1)
        if len(cols) > 1 and re.fullmatch(r"\d{7,12}", cols[1]):
            atendimento = cols[1]
            paciente = cols[2] if len(cols) > 2 else ""
            
            # Localiza colunas de tempo para ancorar os dados (índice T)
            times = [idx for idx, val in enumerate(cols) if re.fullmatch(r"\d{2}:\d{2}", val)]
            
            if times:
                t_idx = times[-1] # Âncora no horário de fim
                # Regra de Âncora: T+1=Procedimento, T+2=Convênio, T+3=Profissional
                procedimento = cols[t_idx+1] if len(cols) > t_idx+1 else ""
                convenio = cols[t_idx+2] if len(cols) > t_idx+2 else ""
                profissional = cols[t_idx+3] if len(cols) > t_idx+3 else ""
                
                # Aviso: procura o número de 6 dígitos antes do primeiro horário
                aviso = ""
                for i in range(times[0]-1, 0, -1):
                    if re.fullmatch(r"\d{5,8}", cols[i]):
                        aviso = cols[i]
                        break
                
                # Salva contexto para as linhas "filhas" que vierem abaixo
                ctx = {
                    "atendimento": atendimento, "paciente": paciente,
                    "aviso": aviso, "convenio": convenio, "time_idx": t_idx
                }
                
                if profissional and profissional.lower() not in ["", "prestador"]:
                    registros.append({
                        "atendimento": atendimento, "paciente": paciente,
                        "data": data_atual, "aviso": aviso,
                        "procedimento": procedimento, "convenio": convenio,
                        "profissional": profissional,
                        "hora_ini": cols[times[0]], "hora_fim": cols[t_idx]
                    })
            continue

        # 3. Identifica Linha-Filha (Procedimentos ou Médicos auxiliares)
        # Não tem atendimento no índice 1, mas o bloco anterior ainda está aberto
        if ctx["atendimento"] and ctx["time_idx"] != -1 and not cols[1]:
            if "total de" in line_txt.lower() or "data de" in line_txt.lower():
                continue
            
            t_idx = ctx["time_idx"]
            procedimento = cols[t_idx+1] if len(cols) > t_idx+1 else ""
            # Convênio costuma vir vazio na linha filha, herdamos do contexto
            convenio = cols[t_idx+2] if (len(cols) > t_idx+2 and cols[t_idx+2]) else ctx["convenio"]
            profissional = cols[t_idx+3] if len(cols) > t_idx+3 else ""
            
            if profissional and profissional.lower() not in ["", "prestador"]:
                registros.append({
                    "atendimento": ctx["atendimento"], "paciente": ctx["paciente"],
                    "data": data_atual, "aviso": ctx["aviso"],
                    "procedimento": procedimento, "convenio": convenio,
                    "profissional": profissional, "hora_ini": "", "hora_fim": ""
                })

    return registros
