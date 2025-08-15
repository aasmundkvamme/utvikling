import requests
import sys
import pandas as pd
import gzip
import shutil
from datetime import date, datetime, timedelta
import time
import aasmund_ny
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email import encoders
import os
import glob
import logging

base_url = aasmund_ny.CD2_base_url
client_id = aasmund_ny.CD2_client_id
client_secret = aasmund_ny.CD2_client_secret
idag = date.today().isoformat()
igår = (date.today() - timedelta(days=1)).isoformat()


FSbrukar = aasmund_ny.FSbrukar
FSpassord = aasmund_ny.FSpassord

parametreCanvas = {'per_page': '100'}
hodeCanvas = {'Authorization': 'Bearer ' + aasmund_ny.tokenCanvas}


def send_epost(tittel, innhald, avsender, mottakarar, vedlegg):
    msg = MIMEMultipart()
    msg['Subject'] = tittel
    msg['From'] = avsender
    msg['To'] = ', '.join(mottakarar)
    
    # Attach the email body to the message
    if vedlegg != "":
        msg.attach(MIMEText(innhald, 'plain'))
    
    try:
        with open(vedlegg, "rb") as attachment:
            # Create the attachment
            part = MIMEBase('application', 'octet-stream')
            part.set_payload(attachment.read())
            encoders.encode_base64(part)
            part.add_header('Content-Disposition', f'attachment; filename= {vedlegg}')
            
            # Attach the file to the email
            msg.attach(part)
        
        # Set up the server and send the email
        smtp_server = smtplib.SMTP('smtp-ut.hvl.no', port=25)
        smtp_server.sendmail(avsender, mottakarar, msg.as_string())
        smtp_server.quit() 
        logger.info(f"Sendte e-post til {', '.join(mottakarar)}")

    except Exception as e:
        logger.error(f"Feil ved sending av e-post: {e}")

def query_FS_graphql(query, variable):
    hode = {
        'Accept': 'application/json;version=1',
        "Feature-Flags": "beta, experimental"
    }
    GraphQLurl = "https://api.fellesstudentsystem.no/graphql/"
    svar = requests.post(
        GraphQLurl, 
        json = {
            'query': query,
            'variables': variable
        },
        headers=hode,
        auth=(FSbrukar, FSpassord))
    if 200 <= svar.status_code < 300:
        return svar.json()
    else:
        return {}


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

query = """
    query MyQuery($emnekode: [String!]) {
    emner(
        filter: {eierInstitusjonsnummer: "203", emnekoder: $emnekode, ikkeUtloptITermin: {arstall: 2025, terminbetegnelse: VAR}}
    )  {
    nodes {
      personroller(filter: {erAktiv: true}) {
        fsRolle {
          navn {
            publiseringsnavn
          }
          erAktiv
        }
        fagperson {
          navn {
            etternavn
            fornavn
          }
          feideBruker
        }
        emne {
          navnAlleSprak {
            nn
          }
        }
      }
    }
  }
}
"""

sider = pd.read_csv("sider.csv")
sider.info()

# Finn og send melding om mest sette sider
avsendar = "aasmund.kvamme@hvl.no"
mottakarar = ["aasmund.kvamme@hvl.no"]
# mottakarar = ["aasmund.kvamme@hvl.no", "alisa.rysaeva@hvl.no", "rdeb@hvl.no"]
tittel = "Mest sette sider"
innhald = f"Dei ti mest sette sidene {igår}\n"
innhald += f"Antal\tSide\n"
for i in range(10):
    innhald += f"{sider.iloc[i]['frekvens']}\thttps://hvl.instructure.com{sider.iloc[i]['index']}\n"
innhald += "\nLitt statistikk:\n"
innhald += f"Antal unike sider besøkt: {len(sider)}\n"
innhald += f"Antal sider besøkt ein gang: {len(sider[sider['frekvens']==1])}"

innhald += "\n\n"

innhald += f"Dei ti mest sette emna {igår}\n"
mest_sett_emne = sider['emne'].value_counts()
for s in sider['emne'].value_counts().index[0:10]:
    canvasurl = f"https://hvl.instructure.com/api/v1/courses/{s}"
    responsCanvas = requests.get(canvasurl, headers=hodeCanvas, params=parametreCanvas)
    if 200 <= responsCanvas.status_code < 300:
        dataCanvas = responsCanvas.json()
        if dataCanvas['sis_course_id'][0:1] == "U":
            emnekode = dataCanvas['sis_course_id'].split('_')[2]
            variable = {"emnekode": emnekode}
            svar = query_FS_graphql(query, variable)
            namn = svar['data']['emner']['nodes'][0]['personroller'][0]['fagperson']['navn']['fornavn'] + ' ' + svar['data']['emner']['nodes'][0]['personroller'][0]['fagperson']['navn']['etternavn']
            brukarnamn = svar['data']['emner']['nodes'][0]['personroller'][0]['fagperson']['feideBruker']
            emnenamn = svar['data']['emner']['nodes'][0]['personroller'][0]['emne']['navnAlleSprak']['nn']
            innhald += f"{sider[sider['emne']==str(s)].count()['frekvens']}\thttps://hvl.instructure.com/courses/{s} {emnekode} {emnenamn}. Emneansvarleg: {namn}, {brukarnamn}\n"
vedlegg = "sider.csv"

print(innhald)
# send_epost(tittel, innhald, avsendar, mottakarar, vedlegg)