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
    ('1Qr9KtcidrZOakMpR9TS7Jb-KRHnmfdUSbAOcztU_6Lw', "'General'!A1:AF500"),
    ('19cxjy8PSt9XUaA1sIaPjr3XxfmhjFDd8-QbM3AG05TE', "'General'!A1:AF500")
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


def agrupar_top_leads_maior_fonte(data, chave, header):
    grupos = {}

    def idx(col): return header.index(col) if col in header else None

    for row in data:
        ad_key = row[idx(chave)] if idx(
            chave) is not None and idx(chave) < len(row) else None
        if not ad_key:
            continue

        try:
            fontes = {
                'Messaging Conversations Started': int(row[idx('Messaging Conversations Started')]) if idx('Messaging Conversations Started') is not None and row[idx('Messaging Conversations Started')] else 0,
                'On-Facebook Leads': int(row[idx('On-Facebook Leads')]) if idx('On-Facebook Leads') is not None and row[idx('On-Facebook Leads')] else 0,
                'Website Leads': int(row[idx('Website Leads')]) if idx('Website Leads') is not None and row[idx('Website Leads')] else 0,
            }
            custos = {
                'Messaging Conversations Started': float(row[idx('Cost per Messaging Conversations Started')].replace('R$', '').replace(',', '.')) if idx('Cost per Messaging Conversations Started') is not None and row[idx('Cost per Messaging Conversations Started')] else 0,
                'On-Facebook Leads': float(row[idx('Cost per On-Facebook Lead')].replace('R$', '').replace(',', '.')) if idx('Cost per On-Facebook Lead') is not None and row[idx('Cost per On-Facebook Lead')] else 0,
                'Website Leads': float(row[idx('Cost per Website Lead')].replace('R$', '').replace(',', '.')) if idx('Cost per Website Lead') is not None and row[idx('Cost per Website Lead')] else 0,
            }
        except Exception as e:
            logging.warning(f"Erro ao processar {ad_key}: {e}")
            continue

        total_leads = sum(fontes.values())
        if total_leads == 0:
            continue

        maior_fonte = max(fontes, key=fontes.get)
        maior_leads = fontes[maior_fonte]
        custo_maior_fonte = custos[maior_fonte]

        if ad_key not in grupos:
            grupos[ad_key] = {"leads": 0, "custo_maior": 0,
                              "maior_fonte": "", "maior_leads": 0}

        grupos[ad_key]["leads"] += total_leads

        if maior_leads > grupos[ad_key]["maior_leads"]:
            grupos[ad_key]["maior_leads"] = maior_leads
            grupos[ad_key]["custo_maior"] = custo_maior_fonte
            grupos[ad_key]["maior_fonte"] = maior_fonte

    top3 = sorted(grupos.items(),
                  key=lambda x: x[1]["leads"], reverse=True)[:3]
    return top3


def tratar_dados(values):
    if not values:
        return {"periodo": "-", "gasto": 0, "leads": 0, "custo": 0}

    header, data = values[0], values[1:]
    def idx(col): return header.index(col) if col in header else None

    dias, gastos, leads = [], [], []

    for row in data:
        try:
            if idx('Day') is not None and row[idx('Day')]:
                dias.append(row[idx('Day')])

            gasto = float(row[idx('Amount Spent')].replace('R$', '').replace(',', '.')) \
                if idx('Amount Spent') is not None and idx('Amount Spent') < len(row) and row[idx('Amount Spent')] else 0

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

    relatorios = []
    totais = {"gasto": 0, "leads": 0}
    periodos = []

    todos_ad_sets = []
    todos_ad_names = []

    for planilha_id, intervalo in PLANILHAS:
        valores = ler_planilha(planilha_id, intervalo)
        if not valores or len(valores) < 2:
            continue

        header = valores[0]
        data = valores[1:]

        dados_conta = tratar_dados(valores)
        periodos.append(dados_conta["periodo"])

        totais["gasto"] += dados_conta["gasto"]
        totais["leads"] += dados_conta["leads"]

        todos_ad_sets += agrupar_top_leads_maior_fonte(
            data, 'Ad Set Name', header)
        todos_ad_names += agrupar_top_leads_maior_fonte(
            data, 'Ad Name', header)

        relatorio = f"""
Conta de anuncio Meta Ads {len(relatorios)+1}
🤑 Total gasto: R$ {dados_conta['gasto']:.2f}
💬 Quantidade de Leads: {dados_conta['leads']}
💵 Custo médio por lead: R$ {dados_conta['custo']:.2f}
"""
        relatorios.append(relatorio.strip())

    periodo_final = periodos[0] if len(
        set(periodos)) == 1 else " e ".join(periodos)
    custo_total = totais["gasto"] / totais["leads"] if totais["leads"] else 0

    def consolidar_top(grupos):
        agrupado = {}
        for nome, info in grupos:
            if nome not in agrupado:
                agrupado[nome] = {"leads": 0, "custo_maior": 0,
                                  "maior_fonte": "", "maior_leads": 0}
            agrupado[nome]["leads"] += info["leads"]
            if info["maior_leads"] > agrupado[nome]["maior_leads"]:
                agrupado[nome]["maior_leads"] = info["maior_leads"]
                agrupado[nome]["custo_maior"] = info["custo_maior"]
                agrupado[nome]["maior_fonte"] = info["maior_fonte"]
        return sorted(agrupado.items(), key=lambda x: x[1]["leads"], reverse=True)[:3]

    top_ad_sets = consolidar_top(todos_ad_sets)
    top_ads = consolidar_top(todos_ad_names)

    mensagem = f"""📅 Período dos anúncios: {periodo_final}
✅ Resultados Meta Ads

{chr(10).join(relatorios)}

✅ Resultado Total - SOMATÓRIA DA CONTA 1 E 2
🤑 Total gasto: R$ {totais['gasto']:.2f}
💬 Quantidade de Leads: {totais['leads']}
💵 Custo médio por lead: R$ {custo_total:.2f}

🔥 Melhores Públicos (Ad Set Name):
""" + "\n".join([
        f"📣 {nome} - Leads: {info['maior_leads']} | Custo: R$ {info['custo_maior']:.2f}"
        for nome, info in top_ad_sets
    ]) + """

✨ Melhores Criativos (Ad Name):
""" + "\n".join([
        f"🎨 {nome} - Leads: {info['maior_leads']} | Custo: R$ {info['custo_maior']:.2f}"
        for nome, info in top_ads
    ])

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
