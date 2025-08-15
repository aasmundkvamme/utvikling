import requests
import pandas as pd
import base64
import gzip
from io import BytesIO
import shutil
from datetime import datetime, timedelta
import time
import os
import logging
import smtplib
import pyodbc
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email import encoders



CD2_base_url = os.environ['CD2_base_url']
CD2_client_id = os.environ['CD2_client_id']
CD2_client_secret = os.environ['CD2_client_secret']
conn_str = os.environ["Connection_SQL"] 
FSbrukar = os.environ["FSbrukar"]
FSpassord = os.environ["FSpassord"]
# FSpålogging = f"{FSbrukar}:{FSpassord}"
# FSpålogging_kode = base64.b64encode(FSpålogging.encode('utf-8')).decode('utf-8')


def test():
    print(CD2_base_url)


def hent_filar(innfil, access_token, payload, n, logger):
    """
    Hentar filar frå web_logs, og skriv data til filer. Best egna for lokal testing, ikkje til drift på Azure.
    """
    requesturl = "https://api-gateway.instructure.com/dap/object/url"
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


def ny_hent_filar(innfil, access_token, payload, n, logger):
    """
    Hentar filar frå web_logs, og samler alt i ei diger DataFrame.
    """
    requesturl = "https://api-gateway.instructure.com/dap/object/url"
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
        with gzip.open(BytesIO(data.content), 'rb') as f:
            # Assuming the file is a CSV inside the .gz
            df = pd.read_csv(f, dtype=str)
    return df


def lag_logger(log_namn):
    # opprett ein logger
    logger = logging.getLogger('my_logger')
    logger.setLevel(logging.DEBUG)  # Sett ønska loggnivå

    # Opprett formatter
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    # Opprett filhandler for å logge til fil (ein loggfil kvar dag)
    file_handler = logging.FileHandler(log_namn)
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)

    # Opprett konsollhandler for å logge til konsollen
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.DEBUG)
    console_handler.setFormatter(formatter)

    # Legg til handlerne i loggeren
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    return logger

def les_access_token(logger):
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
        return access_token
    else:
        logger.error(f"Klarte ikkje å skaffe access_token, feil {r.status_code}")
        return None

def send_epost(tittel, innhald, avsender, mottakarar, vedlegg):
    msg = MIMEMultipart()
    msg['Subject'] = tittel
    msg['From'] = avsender
    msg['To'] = ', '.join(mottakarar)
    
    # Attach the email body to the message
    msg.attach(MIMEText(innhald, 'plain'))

    if vedlegg != "":
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

        except Exception as e:
            print(f"Feil ved sending: {e}")
    else:
        smtp_server = smtplib.SMTP('smtp-ut.hvl.no', port=25)
        smtp_server.sendmail(avsender, mottakarar, msg.as_string())
        smtp_server.quit()


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


def query_canvas_graphql(query, variable):
    """
    Send a GraphQL query to Canvas and return the response.

    :param query: GraphQL query
    :type query: str
    :param variable: GraphQL variable
    :type variable: dict
    :return: JSON response
    :rtype: dict
    :raises Exception: if the request fails
    """
    hode = {
        'Authorization': f'Bearer {os.environ["tokenCanvas"]}',
    }
    GraphQLurl = "https://hvl.instructure.com/api/graphql/"
    svar = requests.post(
        GraphQLurl, 
        json = {
            'query': query,
            'variables': variable
        },
        headers=hode)
    if 200 <= svar.status_code < 300:
        return svar.json()
    else:
        raise Exception(f"Feil i spørjing med kode {svar.status_code}. {query}")


def finn_sist_oppdatert(tabell):
    """
    Return the latest update time for the given table from the akv_sist_oppdatert table.
    """
    try:
        with pyodbc.connect(conn_str) as connection:
            cursor = connection.cursor()
            print(connection)
            try:
                query = """
                SELECT [sist_oppdatert] FROM [dbo].[akv_sist_oppdatert]
                WHERE [tabell] = ?
                """
                cursor.execute(query, (tabell,))
                row = cursor.fetchone()
                print(row)
                if row:
                    print("Har henta frå Azure")
                    if tabell == "web_logs":
                        return (datetime.now() - timedelta(days=1)).isoformat() + "Z"
                    else:
                        return row[0].isoformat() + "Z"
                else:
                    print("Har ikkje henta frå Azure")
                    if tabell == "web_logs":
                        return (datetime.now() - timedelta(days=1)).isoformat() + "Z"
                    else:
                        return (date.today() - timedelta(days=1)).isoformat() + "Z"
            except pyodbc.Error as exc:
                print("Har ikkje henta frå Azure")
                if tabell == "web_logs":
                    return (datetime.now() - timedelta(days=1)).isoformat() + "Z"
                else:
                    return (date.today() - timedelta(days=1)).isoformat() + "Z"
    except pyodbc.Error as exc:
        print("Har ikkje henta frå Azure")
        if tabell == "web_logs":
            return (datetime.now() - timedelta(days=1)).isoformat() + "Z"
        else:
            return (datetime.today() - timedelta(days=1)).isoformat() + "Z"


def skriv_sist_oppdatert(tabell, sist_oppdatert):
    try:
        with pyodbc.connect(conn_str) as conn:
            cursor = conn.cursor()
            try:
                query = """
                MERGE INTO [dbo].[akv_sist_oppdatert] AS target 
                USING (VALUES (?, ?)) AS source (tabell, sist_oppdatert) 
                ON target.[tabell] = source.[tabell]
                WHEN MATCHED THEN
                    UPDATE SET target.[sist_oppdatert] = source.[sist_oppdatert]
                WHEN NOT MATCHED THEN
                    INSERT ([tabell], [sist_oppdatert]) VALUES (source.[tabell], source.[sist_oppdatert]);
                """ 
                print(f"tabell: {tabell}, sist_oppdatert: {sist_oppdatert}")
                cursor.execute(query, (tabell, sist_oppdatert))
                conn.commit()
            except pyodbc.Error as e:
                print(f"Feil ved opplasting av sist oppdatert: {e}")
    except pyodbc.Error as e:
        print(f"Feil ved opplasting av sist oppdatert: {e}")


def les_web_logs(logger):
    # Hent access_token
    access_token = les_access_token(logger)

    if access_token is not None:  
        # Les tidspunkt for forrige oppdatering
        sist_oppdatert = finn_sist_oppdatert("web_logs")

        # Hent oppdateringane
        start_sjekk_status = time.perf_counter()
        requesturl = "https://api-gateway.instructure.com/dap/query/canvas_logs/table/web_logs/data"
        payload = '{"format": "csv", "since": \"%s\"}' % (sist_oppdatert)
        headers = {'x-instauth': access_token, 'Content-Type': 'text/plain'}
        logger.info(f"payload er {payload}")
        logger.info(f"Sender (inkrementel) spørjing til {requesturl}")
        r = requests.request(
            "POST",
            requesturl,
            headers=headers,
            data=payload
        )
        if 200 <= r.status_code < 300:
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
            logger.error(f"Feil i spørjing, kode {r.status_code}\n{r.content}")
            return None
        antal = len(respons2['objects'])
        tidsbruk_sjekk_status = time.perf_counter() - start_sjekk_status
        logger.info(f"Totalt for sjekk_status: {tidsbruk_sjekk_status}")

        # Hent filane
        start_hent_filar = time.perf_counter()
        web_logs_liste = []
        for i in range(antal):
            innfil = respons2['objects'][i]['id']
            payload = f"{respons2['objects']}"
            f = ny_hent_filar(innfil, access_token, payload, i, logger)
            web_logs_liste.append(f)
        web_logs = pd.concat(web_logs_liste)
        tidsbruk_hent_filar = time.perf_counter() - start_hent_filar
        logger.info(f"Totalt for hent_filar: {tidsbruk_hent_filar}")
        # logging.info(f"web_logs er {web_logs.info()}")
        # Skriv tidspunkt for siste oppdatering til fil
        skriv_sist_oppdatert("web_logs", respons2['until'])
        return web_logs

    else:
        logging.error("Fant ikkje access_token") 
        return None


def ny_history(data, logger):
    try:
        antal = len(data)
        logger.info(f"Skriv {len(data)} linjer i ny_history_alle.csv (som test)")
        data.to_csv("ny_history_alle.csv", index=False)
        logger.info(f"Antall linjer før reduksjon: {antal}")
        temp = data[['value.timestamp', 'value.url', 'value.user_id', 'value.course_id', 'value.web_application_controller', 'value.web_application_action', 'value.web_application_context_type']]
        logger.info("Fjerner linjer med api")
        temp = temp[~temp['value.url'].str.contains("/api/")]
        logger.info("Fjerner linjer med retrieve")
        temp = temp[temp['value.web_application_action'] != "retrieve"]
        temp['dato'] = temp['value.timestamp'].apply(lambda x: x[0:10])
        med_user_id = temp[temp['value.user_id'].notna()]
        med_user_id.to_csv("/home/aasmund/diverse_Canvas/ny_history.csv", index=False)
        logger.info(f"Antall linjer etter reduksjon: {len(med_user_id)}")
        conn_str = os.environ['Connection_SQL']
        # with pyodbc.connect(conn_str) as connection:
        #     cursor = connection.cursor()
        #     try:
        #         query = """
        #             INSERT INTO [dbo].[akv_ny_canvas_history] ([dato], [user_id], [course_id], [web_application_controller], [web_application_action], [web_application_context_type], [url])
        #             VALUES (?, ?, ?, ?, ?, ?, ?)
        #         """
        #         for index, row in med_user_id.iterrows():
        #             dato = row['dato']
        #             url = row['value.url']
        #             course_id = row['value.course_id']
        #             user_id = row['value.user_id']
        #             web_application_controller = row['value.web_application_controller']
        #             web_application_action = row['value.web_application_action']
        #             web_application_context_type = row['value.web_application_context_type']
        #             cursor.execute(query, (dato, user_id, course_id, web_application_controller, web_application_action, web_application_context_type, url))
        #         connection.commit()
        #     except pyodbc.Error as exc:
        #         logger.error(exc)
    except Exception as e:
        logger.error(e)


def sjekk_external_tools(url, logger):
    try:
        pass
    except Exception as e:
        logger.error(e)