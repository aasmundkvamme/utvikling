import pyodbc
import gzip
import io
import pandas as pd
import time
from db import *
from konstantar import *
import logging
from datetime import date, timedelta
import requests

def akv_finn_sist_oppdatert(tabell):
    """
    Returner den siste oppdateringstida for den gitte tabellen fra akv_sist_oppdatert-tabellen.
    Hvis ingen dato er gitt (eller vi ikkje får kontakt med databasen), returner igår.
    """
    try:
        with pyodbc.connect(conn_str) as connection:
            cursor = connection.cursor()
            query = """
            SELECT [sist_oppdatert] FROM [dbo].[akv_sist_oppdatert]
            WHERE [tabell] = ?
            """
            cursor.execute(query, (tabell,))
            row = cursor.fetchone()
            if row:
                logging.debug(f"{tabell} er sist oppdatert (Azure): {row[0].isoformat() + 'Z'}")
                return row[0].isoformat() + "Z"
    except pyodbc.Error as exc:
        logging.debug(f"{tabell} er sist oppdatert (lokal): {(date.today() - timedelta(days=1)).isoformat() + 'Z'}") 
        return (date.today() - timedelta(days=1)).isoformat() + "Z"


def akv_lagre_sist_oppdatert(tabell, dato):
    """
    Lagre datoen for siste oppdatering av tabell i Azure eller lokalt (dersom vi ikkje får kontakt med databasen).
    """
    
    try:
        with pyodbc.connect(conn_str) as conn:
            cursor = conn.cursor()
            query = """
            MERGE INTO [dbo].[akv_sist_oppdatert] AS target 
            USING (VALUES (?, ?)) AS source (tabell, sist_oppdatert) 
            ON target.[tabell] = source.[tabell]
            WHEN MATCHED THEN
                UPDATE SET target.[sist_oppdatert] = source.[sist_oppdatert]
            WHEN NOT MATCHED THEN
                INSERT ([tabell], [sist_oppdatert]) VALUES (source.[tabell], source.[sist_oppdatert]);
            """ 
            cursor.execute(query, (tabell, dato))
            conn.commit()
            logging.debug(f"{tabell} er sist oppdatert (Azure): {dato}")
    except pyodbc.Error as e:
        with open(f'sist_oppdatert_{tabell}.txt', 'w') as f_out:
            f_out.write(dato)
            logging.debug(f"{tabell} er sist oppdatert (lokal): {dato}")
    return None


def akv_query_canvas_graphql(query, variable):
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
        'Authorization': f'Basic {os.environ["tokenCanvas"]}',
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


def akv_query_FS_graphql(query, variable):
    """
    Send a GraphQL query to Felles Studentsystem and return the response.

    :param query: GraphQL query
    :type query: str
    :param variable: GraphQL variable
    :type variable: dict
    :return: JSON response
    :rtype: dict
    :raises Exception: if the request fails
    """
    hode = {
        'Accept': 'application/json;version=1',
        'Authorization': f'Basic {os.environ["tokenFS"]}',
        "Feature-Flags": "beta, experimental"
    }
    GraphQLurl = "https://api.fellesstudentsystem.no/graphql/"
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
        return {}


def akv_hent_CD2_access_token():
    be_om_access_token = requests.request(
        "POST",
        f"{CD2_base_url}/ids/auth/login",
        data={'grant_type': 'client_credentials'},
        auth=(CD2_client_id, CD2_client_secret)
        )
    if be_om_access_token.status_code == 200:
        CD2_access_token = be_om_access_token.json()['access_token']
        return CD2_access_token
    else:
        feilmelding = f"Klarte ikkje å skaffe access_token, feil {be_om_access_token.status_code}"
        logging.error(feilmelding)
        return feilmelding


def akv_hent_CD2_filar(innfil, token, svar):
    requesturl = f"{CD2_base_url}/dap/object/url"
    payload = f"{svar['objects']}"
    payload = payload.replace('\'', '\"')
    headers = {'x-instauth': token, 'Content-Type': 'text/plain'}
    r4 = requests.request("POST", requesturl, headers=headers, data=payload)
    if r4.status_code == 200:
        respons4 = r4.json()
        url = respons4['urls'][innfil]['url']
        data = requests.request("GET", url)
        buffer = io.BytesIO(data.content)
        with gzip.GzipFile(fileobj=buffer, mode='rb') as utpakka_fil:
            utpakka_data = utpakka_fil.read().decode()
    return utpakka_data


def akv_les_CD2_tabell(tabell):
    CD2_access_token = akv_hent_CD2_access_token()
    headers = {'x-instauth': CD2_access_token, 'Content-Type': 'text/plain'}
    sist_oppdatert = akv_finn_sist_oppdatert(tabell)
    if sist_oppdatert == None:
        payload = "{\"format\":\"csv\"}"
    else:
        payload = '{"format": "csv", "since": \"%s\"}' % (sist_oppdatert)
    requesturl = f"{CD2_base_url}/dap/query/canvas/table/{tabell}/data"
    print(f"Sender søk til {requesturl}")
    try:
        r = requests.request("POST", requesturl, headers=headers, data=payload)
        r.raise_for_status()
        respons = r.json()
        id = respons['id']
        vent = True
        while vent:
            requesturl2 = f"{CD2_base_url}/dap//job/{id}"
            r2 = requests.request("GET", requesturl2, headers=headers)
            time.sleep(5)
            respons2 = r2.json()
            print(respons2)
            if respons2['status'] == "complete":
                vent = False
                filar = respons2['objects']
        dr_liste = []
        print(filar)
        for fil in filar:
            data = io.StringIO(akv_hent_CD2_filar(fil['id'], CD2_access_token, respons2))
            df = pd.read_csv(data, sep=",")
            dr_liste.append(df)
        alledata = pd.concat(df for df in dr_liste if not df.empty)
        if sist_oppdatert == None:
            denne_oppdateringa = respons2['at']
        else:
            denne_oppdateringa = respons2['until']
        return alledata, sist_oppdatert, denne_oppdateringa
    except requests.exceptions.RequestException as exc:
        raise exc
    

def akv_les_CD2_pseudonyms():
    """
    Leser pseudonyms-tabellen fra Canvas Data 2, henter nye poster og oppdaterer Azure-tabellen "akv_user_id_kobling.
    """
    start_CD2_pseudonyms = time.perf_counter()
    CD2_tabell = "pseudonyms"
    alledata, sist_oppdatert, denne_oppdateringa = akv_les_CD2_tabell(CD2_tabell)
    alle_nye = alledata[(alledata['value.created_at']>sist_oppdatert)]
    alle_nye.to_csv(f"{CD2_tabell}_nye_{denne_oppdateringa[0:10]}.csv", index=False)
    ekte_nye = alle_nye.dropna(subset='value.sis_user_id')

    query = """
        MERGE INTO [dbo].[akv_user_id_kobling] AS target 
        USING (VALUES (?, ?)) AS source (user_id, sis_user_id) 
        ON target.[user_id] = source.[user_id]
        WHEN MATCHED THEN
            UPDATE SET target.[sis_user_id] = source.[sis_user_id]
        WHEN NOT MATCHED THEN
            INSERT ([user_id], [sis_user_id]) VALUES (source.[user_id], source.[sis_user_id]);
    """
    try:
        nye = ekte_nye[['value.user_id', 'value.sis_user_id']]
        with pyodbc.connect(conn_str) as conn:
            cursor = conn.cursor()
            for index, row in nye.iterrows():
                user_id = str(row[0])
                sis_user_id = str(row[1])
                cursor.execute(query, (user_id, sis_user_id))
            conn.commit()
    except pyodbc.Error as e:
        with open(f'sist_oppdatert_{CD2_tabell}.txt', 'w') as f_out:
            f_out.write(idag)
            logging.debug(f"{CD2_tabell} er sist oppdatert (lokal): {idag}")
    akv_lagre_sist_oppdatert(CD2_tabell, denne_oppdateringa)
    print(f"Tabell: {CD2_tabell} er oppdatert {denne_oppdateringa}")
    print(f"Total tidsbruk: {time.perf_counter() - start_CD2_pseudonyms}")
