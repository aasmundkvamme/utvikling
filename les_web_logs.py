import requests
import sys
import pandas as pd
import gzip
import shutil
from datetime import date, datetime, timedelta
import time
# import aasmund
import smtplib
from email.mime.text import MIMEText
import os
import glob
import logging

CD2_base_url = os.environ['CD2_base_url']
CD2_client_id = os.environ['CD2_client_id']
CD2_client_secret = os.environ['CD2_client_secret']
avsendar = "aasmund.kvamme@hvl.no"
mottakarar = ["aasmund.kvamme@hvl.no"]
tittel = "CD2 web log"
idag = date.today().isoformat()

def send_epost(tittel, innhald, avsender, mottakarar):
    msg = MIMEText(innhald)
    msg['Subject'] = tittel
    msg['From'] = avsendar
    msg['To'] = ', '.join(mottakarar)
    smtp_server = smtplib.SMTP('smtp-ut.hvl.no', port=25)
    smtp_server.sendmail(avsendar, mottakarar, msg.as_string())
    smtp_server.quit() 
    logger.info(f"Sendte e-post")


def hent_filar(innfil, n):
    requesturl = "https://api-gateway.instructure.com/dap/object/url"
    payload = f"{respons2['objects']}"
    payload = payload.replace('\'', '\"')
    headers = {
        'x-instauth': access_token,
        'Content-Type': 'text/plain'
    }
    logger.info(f"Hentar datafil nr. {n}")
    r4 = requests.request(
        "POST",
        requesturl,
        headers=headers,
        data=payload
    )
    if r4.status_code == 200:
        respons4 = r4.json()
        url = respons4['urls'][innfil]['url']
        data = requests.request("GET", url)
        no = datetime.now()
        utfil = f"web_logs-{no.year}{no.month:02}{no.day:02}{no.hour:02}{no.minute:02}-{n}"
        open(f'{utfil}.gz', 'wb').write(data.content)
        with gzip.open(f'{utfil}.gz', 'rb') as f_in:
            with open(f'{utfil}.txt', 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)
        logger.info(f" skrevet til {f'{utfil}.txt'}")
        os.remove(f"{utfil}.gz")
    return f"{utfil}.txt"


# opprett ein logger
logger = logging.getLogger('my_logger')
logger.setLevel(logging.DEBUG)  # Sett ønska loggnivå

# Opprett formatter
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

# Opprett filhandler for å logge til fil (ein loggfil kvar dag)
file_handler = logging.FileHandler(f'loggfil-web_log-{idag}.log')
file_handler.setLevel(logging.DEBUG)
file_handler.setFormatter(formatter)

# Opprett konsollhandler for å logge til konsollen
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.DEBUG)
console_handler.setFormatter(formatter)

# Legg til handlerne i loggeren
logger.addHandler(file_handler)
logger.addHandler(console_handler)

# Finn alle filer som samsvarar med mønsteret "web_logs-*.txt"
filer = glob.glob("web_logs-*.txt")

# Sletta kvar fil
for fil in filer:
    try:
        os.remove(fil)
        logger.info(f"Slettet fil: {fil}")
    except OSError as e:
        logger.error(f"Error: {fil} : {e.strerror}")

# Hent access_token
requesturl = "https://api-gateway.instructure.com/ids/auth/login"
payload = {'grant_type': 'client_credentials'}
r = requests.request(
    "POST",
    requesturl,
    data=payload,
    auth=(CD2_client_id, CD2_client_secret)
)
if r.status_code == 200:
    respons = r.json()
    access_token = respons['access_token']
    logger.info(f"Henta access_token OK.")
else:
    logger.error(f"Klarte ikkje å skaffe access_token, feil {r.status_code}")

# Les tidspunkt for forrige oppdatering
try:
    with open("sist_oppdatert_web_logs.txt", "r") as f_in:
        sist_oppdatert = f_in.read()
except:
    sist_oppdatert = (datetime.now()-timedelta(days=1)).isoformat() + "Z"

# Hent oppdateringane
start_hent_oppdateringar = time.perf_counter()
requesturl = "https://api-gateway.instructure.com/dap/query/canvas_logs/table/web_logs/data"
payload = '{"format": "csv", "since": \"%s\"}' % (sist_oppdatert)
headers = {'x-instauth': access_token, 'Content-Type': 'text/plain'}
logger.info(f"Sender (inkrementel) spørjing til {requesturl}")
r = requests.request(
    "POST",
    requesturl,
    headers=headers,
    data=payload
)
if r.status_code == 200:
    respons = r.json()
    id = respons['id']
    vent = True
    while vent:
        logger.info(f"Sjekker status på jobb {id}")
        requesturl = f"https://api-gateway.instructure.com/dap//job/{id}"
        r2 = requests.request("GET", requesturl, headers=headers)
        respons2 = r2.json()
        if respons2['status'] == "complete":
            vent = False
        time.sleep(5)
    logger.info("Status oppdatert")
else:
    logger.error(f"Feil i spørjing, kode {r.status_code}")
antal = len(respons2['objects'])
tidsbruk_hent_oppdateringar = time.perf_counter() - start_hent_oppdateringar

start_hent_filar = time.perf_counter()
filar_i_dag = []
for i in range(antal):
    f = hent_filar(respons2['objects'][i]['id'], i)
    filar_i_dag.append(f)
tidsbruk_hent_filar = time.perf_counter() - start_hent_filar

# Start analyse av dei nye filane, og legg til i den akkumulerte lista
start_analyse = time.perf_counter()
logger.info(f"Startar analyse av data")
grand_total = 0
rekker = []
history_liste = []
for f in filar_i_dag:
    data = pd.read_csv(
        f,
        sep=",",
        usecols=[
            'value.timestamp',
            'value.user_agent',
            'value.user_id',
            'value.url',
            'value.web_application_context_type',
            ]
        )
    data.dropna(subset=[
            'value.timestamp',
            'value.user_agent',
            'value.user_id',
            'value.url',
            'value.web_application_context_type',
        ],
        inplace=True)
    data['dato'] = data['value.timestamp'].str[0:10]
    data['brukar'] = data['value.user_id'].apply(lambda x: str(int(x)))
    history_liste.append(data)

    datoar = pd.unique(data.dato)
    user_agents = [
        ('androidStudent', '', 'applitenskjerm'),
        ('androidTeacher', '', 'applitenskjerm'),
        ('candroid', '', 'applitenskjerm'),
        ('iCanvas', 'iPhone', 'applitenskjerm'),
        ('iosTeacher', 'iPhone', 'applitenskjerm'),
        ('iCanvas', 'iPad', 'appstorskjerm'),
        ('iosTeacher', 'iPad', 'appstorskjerm'),
        ('Mozilla', 'iPhone', 'nettlesarlitenskjerm'),
        ('Mozilla', 'iPad', 'nettlesarstorskjerm'),
        ('Mozilla', 'Linux', 'nettlesarstorskjerm'),
        ('Mozilla', 'Macintosh', 'nettlesarstorskjerm'),
        ('Mozilla', 'Windows', 'nettlesarstorskjerm')
        ]
    studentar = pd.read_csv("studentar.csv")
    tilsette = pd.read_csv("tilsette.csv")
    # assistentar = pd.read_csv("assistentar.csv")
    # observatørar = pd.read_csv("observatørar.csv")
    for dato in datoar:
        logger.info(f"Log-fil for {dato}")
        antal = []
        total = 0
        datofil = data.loc[data.dato==dato]
        tilsette_log = pd.merge(datofil, tilsette, on='value.user_id')
        studentar_log = pd.merge(datofil, studentar, on='value.user_id')
        # assistentar_log = pd.merge(datofil, assistentar, on='value.user_id')
        # observatørar_log = pd.merge(datofil, observatørar, on='value.user_id')
        for ua in user_agents:
            if ua[1] == '':
                antal_tilsette = tilsette_log.loc[(tilsette_log['value.user_agent'].str.contains(ua[0]))].count().iloc[0]
                antal_studentar = studentar_log.loc[(studentar_log['value.user_agent'].str.contains(ua[0]))].count().iloc[0]
                # antal_assistentar = assistentar_log.loc[(assistentar_log['value.user_agent'].str.contains(ua[0]))].count().iloc[0]
                # antal_observatørar = observatørar_log.loc[(observatørar_log['value.user_agent'].str.contains(ua[0]))].count().iloc[0]
                total += antal_tilsette + antal_studentar
                # total += antal_tilsette + antal_studentar + antal_assistentar + antal_observatørar
                antal.append({
                    'Plattform': ua[0],
                    'total': total,
                    'type': ua[2],
                    'antal': {
                        'tilsette': antal_tilsette,
                        'studentar': antal_studentar,
                        }
                    })
            else:
                antal_tilsette = tilsette_log.loc[(tilsette_log['value.user_agent'].str.contains(ua[0]))&(tilsette_log['value.user_agent'].str.contains(ua[1]))].count().iloc[0]
                antal_studentar = studentar_log.loc[(studentar_log['value.user_agent'].str.contains(ua[0]))&(studentar_log['value.user_agent'].str.contains(ua[1]))].count().iloc[0]
                # antal_assistentar = assistentar_log.loc[(assistentar_log['value.user_agent'].str.contains(ua[0]))&(assistentar_log['value.user_agent'].str.contains(ua[1]))].count().iloc[0]
                # antal_observatørar = observatørar_log.loc[(observatørar_log['value.user_agent'].str.contains(ua[0]))&(observatørar_log['value.user_agent'].str.contains(ua[1]))].count().iloc[0]
                total += antal_tilsette + antal_studentar
                # total += antal_tilsette + antal_studentar + antal_assistentar + antal_observatørar
                antal.append({
                    'Plattform': ua[0],
                    'total': total,
                    'type': ua[2],
                    'antal': {
                        'tilsette': antal_tilsette,
                        'studentar': antal_studentar,
                        # 'assistentar': antal_assistentar,
                        # 'observatørar': antal_observatørar
                        }
                    })
        grand_total += total
        oppteljing = []
        for a in antal:
            oppteljing.append((a['type'], a['antal']['tilsette'], a['antal']['studentar']))
            # oppteljing.append((a['type'], a['antal']['tilsette'], a['antal']['studentar'], a['antal']['assistentar'], a['antal']['observatørar']))
        temp = pd.DataFrame(oppteljing, columns=['plattform', 'tilsette', 'studentar'])
        # temp = pd.DataFrame(oppteljing, columns=['plattform', 'tilsette', 'studentar', 'assistentar', 'observatørar'])
        temp_gruppert = temp.groupby('plattform').sum()
        temp_flattened = temp_gruppert.T.unstack().to_frame().T
        temp_flattened.columns = ['_'.join(col).strip() for col in temp_flattened.columns.values]
        temp_flattened['dato'] = dato
        rekker.append(temp_flattened)
    os.remove(f)

historydata = pd.concat(df for df in history_liste if not df.empty)
historydata.to_csv("history.csv", index=False)
rekker_i_dag = pd.concat(rekker)
gamle_rekker = pd.read_csv("plattformbruk.csv")
oppdatert_plattformbruk = pd.concat([rekker_i_dag, gamle_rekker]).groupby('dato').sum().reset_index()
oppdatert_plattformbruk.to_csv("plattformbruk.csv", index=False)
logger.info("Har oppdatert fila plattformbruk.csv")
tidsbruk_analyse = time.perf_counter() - start_analyse

innhald = f"Resultat frå web_log {idag}\n"
innhald += f"Sjekke oppdateringar: {tidsbruk_hent_oppdateringar:.3f} sekund.\n"
innhald += f"Hente filar med oppdateringar: {tidsbruk_hent_filar:.3f} sekund.\n"
innhald += f"Analysere data: {tidsbruk_analyse:.3f} sekund.\n"
innhald += "\n"
innhald += f"I alt er {grand_total} klikk registrert sidan {sist_oppdatert[0:10]}."

df = pd.read_csv("tid_logg.csv")
df.loc[len(df)] = [idag, tidsbruk_hent_oppdateringar, tidsbruk_hent_filar, tidsbruk_analyse]
df.to_csv('tid_logg.csv', index=False)

# send_epost(tittel, innhald, avsendar, mottakarar)

# Skriv tidspunkt for siste oppdatering til fil
with open('sist_oppdatert_web_logs.txt', 'w') as f_out:
    f_out.write(respons2['until'])