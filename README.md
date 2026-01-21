
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
