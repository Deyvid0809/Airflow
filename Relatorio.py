from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from google.oauth2 import service_account
from googleapiclient.discovery import build
import requests
import logging

# Configurações
SERVICE_ACCOUNT_FILE = '/opt/airflow/dags/gen-lang-client-0414276798-ec71b803f5f2.json'
SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly']
WHATSAPP_API_URL = 'https://graph.facebook.com/v18.0/709815058873308/messages'
WHATSAPP_TOKEN = 'EAAPBBxZBhFUQBO0KHkODEExX6NI9VMpYgByT2rnlMJHAzuZBQn6XsZBmPtOYKt2cGn9GvtZB9a5eLjGx1rivvVrQcvWfBPANCeNo2gBpEWHVErDyw2xtWqN91xQp3EIKKTQ71C4eRFLMtXkDN2LugqZBNfgiK9Kp4G7mrAZBH9a0AuNQ5Ucm9CpgMZBOBRgSArAFAZDZD'
DESTINO = '5511941426329'

PLANILHAS = [
    ('1Qr9KtcidrZOakMpR9TS7Jb-KRHnmfdUSbAOcztU_6Lw', "'General'!A1:S25"),
    ('19cxjy8PSt9XUaA1sIaPjr3XxfmhjFDd8-QbM3AG05TE', "'General'!A1:S15")
]


def ler_planilha(spreadsheet_id, intervalo):
    try:
        creds = service_account.Credentials.from_service_account_file(
            SERVICE_ACCOUNT_FILE, scopes=SCOPES)
        service = build('sheets', 'v4', credentials=creds)
        result = service.spreadsheets().values().get(
            spreadsheetId=spreadsheet_id, range=intervalo).execute()
        return result.get('values', [])
    except Exception as e:
        logging.error(f"Erro ao ler planilha {spreadsheet_id}: {e}")
        return []


def tratar_dados(values):
    if not values:
        return {"periodo": "-", "gasto": 0, "leads": 0, "custo": 0}

    header, data = values[0], values[1:]
    def idx(col): return header.index(col) if col in header else None

    dias, gastos, leads = [], [], []

    for row in data:
        try:
            # Coleta o dia
            if idx('Day') and row[idx('Day')]:
                dias.append(row[idx('Day')])

            # Coleta o gasto (Amount Spent)
            gasto = float(row[idx('Amount Spent')].replace('R$', '').replace(',', '.')) \
                if idx('Amount Spent') is not None and idx('Amount Spent') < len(row) and row[idx('Amount Spent')] else 0

            # Soma dos leads
            lead = sum(int(row[i]) for i in [
                idx('Messaging Conversations Started'),
                idx('Website Leads'),
                idx('On-Facebook Leads')
            ] if i is not None and i < len(row) and row[i])

            gastos.append(gasto)
            leads.append(lead)

        except Exception as e:
            logging.warning(f"Erro ao processar linha: {row} - {e}")
            continue

    if len(set(dias)) > 1:
        periodo = f"{min(dias)} até {max(dias)}"
    else:
        periodo = dias[0] if dias else "-"

    total_gasto = sum(gastos)
    total_leads = sum(leads)
    custo = total_gasto / total_leads if total_leads else 0

    return {
        "periodo": periodo,
        "gasto": total_gasto,
        "leads": total_leads,
        "custo": custo
    }


def enviar_whatsapp(msg):
    headers = {
        "Authorization": f"Bearer {WHATSAPP_TOKEN}",
        "Content-Type": "application/json"
    }
    payload = {
        "messaging_product": "whatsapp",
        "to": DESTINO,
        "type": "text",
        "text": {"body": msg}
    }
    try:
        response = requests.post(
            WHATSAPP_API_URL, headers=headers, json=payload)
        logging.info(
            f"✅ WhatsApp API response: {response.status_code} - {response.text}")
    except Exception as e:
        logging.error(f"Erro ao enviar mensagem via WhatsApp: {e}")


def processar_e_enviar():
    logging.info("🔍 Iniciando processamento dos dados das planilhas...")

    dados_conta_1 = tratar_dados(ler_planilha(*PLANILHAS[0]))
    dados_conta_2 = tratar_dados(ler_planilha(*PLANILHAS[1]))

    total_gasto = dados_conta_1["gasto"] + dados_conta_2["gasto"]
    total_leads = dados_conta_1["leads"] + dados_conta_2["leads"]
    custo_total = total_gasto / total_leads if total_leads else 0

    # Pega o menor e maior período entre ambas contas
    periodos = [dados_conta_1["periodo"], dados_conta_2["periodo"]]
    periodo_final = periodos[0] if periodos[0] == periodos[
        1] else f"{dados_conta_1['periodo']} e {dados_conta_2['periodo']}"

    mensagem = f"""📅 *Período dos anúncios:* {periodo_final}
✅ *Resultados Meta Ads*

*Conta de anuncio Meta Ads 1*
🤑 *Total gasto:* R$ {dados_conta_1['gasto']:.2f}
💬 *Quantidade de Leads:* {dados_conta_1['leads']}
💵 *Custo médio por lead:* R$ {dados_conta_1['custo']:.2f}

*Conta de anuncio Meta Ads 2*
🤑 *Total gasto:* R$ {dados_conta_2['gasto']:.2f}
💬 *Quantidade de Leads:* {dados_conta_2['leads']}
💵 *Custo médio por lead:* R$ {dados_conta_2['custo']:.2f}

✅ *Resultado Total* - SOMATÓRIA DA CONTA 1 E 2
🤑 *Total gasto:* R$ {total_gasto:.2f}
💬 *Quantidade de Leads:* {total_leads}
💵 *Custo médio por lead:* R$ {custo_total:.2f}
"""
    logging.info("📤 Enviando mensagem para WhatsApp...")
    enviar_whatsapp(mensagem)


with DAG(
    dag_id='relatorio_meta_ads_whatsapp',
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=['whatsapp', 'meta_ads'],
) as dag:

    tarefa_relatorio = PythonOperator(
        task_id='processar_e_enviar_whatsapp',
        python_callable=processar_e_enviar
    )
