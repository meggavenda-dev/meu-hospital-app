
# 🏥 Importador de Internações e Procedimentos Cirúrgicos

Sistema desenvolvido em **Streamlit + SQLite**, com banco armazenado no próprio GitHub.

## 🚀 Funcionalidades

- Importa arquivos CSV de produção cirúrgica.
- Identifica automaticamente:
  - Atendimento
  - Paciente
  - Convênio
  - Data
  - Profissional
  - Procedimento
- Cria automaticamente:
  - Internações
  - Procedimentos conectados à internação
- Se a internação já existir:
  - O sistema adiciona apenas os novos procedimentos.
- Banco `.db` armazenado no repositório GitHub.
- Interface simples para publicar no **share.streamlit.io**.

---

## 📂 Estrutura
# meu-hospital-app
meu-hospital-app


# 🏥 Sistema de Internações — Procedimento-do-dia

## O que é
- Gestão de **Internações** (1 por atendimento — chave única de negócio).
- Para cada **(internação, data)** existe **1 (um) Procedimento-do-dia** com:
  - **Profissional do dia** (o primeiro que surgir na cirurgia),
  - **Situação (status)**: Pendente, Não Cobrar, Enviado para pagamento, Aguardando Digitação - AMHP, Finalizado,
  - **Observação** (texto livre).
- **Import** a partir do CSV hospitalar:
  - Parser robusto (trata vírgulas/aspas, ignora cabeçalhos/totais, ancora tipo/quarto).
  - Cria Internação se não existir.
  - Cria **1** Procedimento-do-dia por data se **ainda não existir** (se já existir, **ignora**).
- **Lançamento manual** de Procedimento-do-dia respeitando a unicidade.

## Como rodar
```bash
pip install -r requirements.txt
streamlit run app.py

