from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from google.oauth2 import service_account
from googleapiclient.discovery import build
import requests
import logging
from collections import defaultdict
import openai
import os  # Adicionado

# Configurações
SERVICE_ACCOUNT_FILE = os.environ.get(
    'GOOGLE_APPLICATION_CREDENTIALS')  # Alterado
SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly']
WHATSAPP_API_URL = 'https://graph.facebook.com/v18.0/709815058873308/messages'
WHATSAPP_TOKEN = os.environ.get('WHATSAPP_TOKEN')  # Alterado
DESTINO = '5511941426329'

PLANILHAS = [
    ('1Qr9KtcidrZOakMpR9TS7Jb-KRHnmfdUSbAOcztU_6Lw', "'General'!A1:S500"),
    ('19cxjy8PSt9XUaA1sIaPjr3XxfmhjFDd8-QbM3AG05TE', "'General'!A1:S500")
]

openai_client = openai.OpenAI(
    api_key=os.environ.get('OPENAI_API_KEY')  # Alterado
)


def gerar_insights_chatgpt(mensagem_usuario):
    try:
        response = openai_client.chat.completions.create(
            model="gpt-4o",
            messages=[
                {"role": "system", "content": "Você é um especialista em marketing digital e análise de campanhas."},
                {"role": "user", "content": mensagem_usuario}
            ],
            temperature=0.4,
            max_tokens=1000
        )
        return response.choices[0].message.content.strip()
    except Exception as e:
        logging.error(f"Erro ao gerar insights via OpenAI: {e}")
        return "Não foi possível gerar insights neste momento."


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

    dados_por_dia = defaultdict(lambda: {"contas": [], "totais": {
                                "gasto": 0, "leads": 0}, "ad_sets": [], "ad_names": []})

    for idx_planilha, (planilha_id, intervalo) in enumerate(PLANILHAS, start=1):
        valores = ler_planilha(planilha_id, intervalo)
        if not valores or len(valores) < 2:
            continue

        header = valores[0]
        data = valores[1:]

        def idx(col): return header.index(col) if col in header else None

        for row in data:
            try:
                dia = row[idx('Day')]
                gasto = float(row[idx('Amount Spent')].replace('R$', '').replace(
                    ',', '.')) if row[idx('Amount Spent')] else 0
                leads = sum(int(row[i]) for i in [
                    idx('Messaging Conversations Started'),
                    idx('Website Leads'),
                    idx('On-Facebook Leads')
                ] if i is not None and i < len(row) and row[i])

                ad_set = row[idx('Ad Set Name')] if idx('Ad Set Name') is not None and idx(
                    'Ad Set Name') < len(row) else 'Desconhecido'
                ad_name = row[idx('Ad Name')] if idx('Ad Name') is not None and idx(
                    'Ad Name') < len(row) else 'Desconhecido'

                dados_dia = dados_por_dia[dia]
                dados_dia['totais']['gasto'] += gasto
                dados_dia['totais']['leads'] += leads
                dados_dia['contas'].append(
                    {"conta": idx_planilha, "gasto": gasto, "leads": leads})
                dados_dia['ad_sets'].append(ad_set)
                dados_dia['ad_names'].append(ad_name)

            except Exception as e:
                logging.warning(f"Erro ao processar linha: {row} - {e}")
                continue

    for dia, dados in sorted(dados_por_dia.items()):
        relatorios = []
        contas = defaultdict(lambda: {"gasto": 0, "leads": 0})

        for conta in dados['contas']:
            contas[conta['conta']]['gasto'] += conta['gasto']
            contas[conta['conta']]['leads'] += conta['leads']

        for conta_num in sorted(contas.keys()):
            c = contas[conta_num]
            custo = c['gasto'] / c['leads'] if c['leads'] else 0
            relatorio = f"""
Conta de anuncio Meta Ads {conta_num}
🤑 Total gasto: R$ {c['gasto']:.2f}
💬 Quantidade de Leads: {c['leads']}
💵 Custo médio por lead: R$ {custo:.2f}
"""
            relatorios.append(relatorio.strip())

        custo_total = dados['totais']['gasto'] / \
            dados['totais']['leads'] if dados['totais']['leads'] else 0

        from collections import Counter
        top_ad_sets = Counter(dados['ad_sets']).most_common(3)
        top_ad_names = Counter(dados['ad_names']).most_common(3)

        mensagem = f"""📅 Data do anúncio: {dia}
✅ Resultados Meta Ads

{chr(10).join(relatorios)}

✅ Resultado Total - SOMATÓRIA DA CONTA 1 E 2
🤑 Total gasto: R$ {dados['totais']['gasto']:.2f}
💬 Quantidade de Leads: {dados['totais']['leads']}
💵 Custo médio por lead: R$ {custo_total:.2f}

🔥 Melhores Públicos (Ad Set Name):
""" + "\n".join([
            f"📣 {nome} - Ocorrências: {count}" for nome, count in top_ad_sets
        ]) + """

✨ Melhores Criativos (Ad Name):
""" + "\n".join([
            f"🎨 {nome} - Ocorrências: {count}" for nome, count in top_ad_names
        ])

        prompt = f"""
Você é um especialista em anúncios Meta Ads.

Com base nos seguintes dados do dia {dia}:

- Melhores Públicos: {top_ad_sets}
- Melhores Criativos: {top_ad_names}
- Totais: Gasto = R$ {dados['totais']['gasto']:.2f}, Leads = {dados['totais']['leads']}

1. Gere *INSIGHTS* com base nos melhores públicos e criativos com no maximo 3 topicos e resumido(Toma a atitude de um social midia proficional e interaja como s estivess explicando para um apessoa).
2. Gere *PRÓXIMOS PASSOS* que o time pode tomar com no maximo 3 topicos e resumido(Toma a atitude de um social midia profisional e interaja como s estivess explicando para um apessoa).

Retorne apenas as duas seções:

*INSIGHTS*: ...

*PRÓXIMOS PASSOS*: ...
"""

        insights = gerar_insights_chatgpt(prompt)

        mensagem += f"""

{insights}
"""

        logging.info(f"📤 Enviando relatório do dia {dia}...")
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
