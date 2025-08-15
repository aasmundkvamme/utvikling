import requests
import os
import pandas as pd
from io import StringIO
from datetime import datetime, timedelta, date

FSbrukar = os.environ["FSbrukar"]
FSpassord = os.environ["FSpassord"]

hodeCanvas = {
    'Authorization': f'Bearer {os.environ["tokenCanvas"]}',
}

parametre = {'per_page': 100}

today = date.today()
idag = today.isoformat()
igår = (today - timedelta(days=1)).isoformat()



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


def finn_emnekode(streng):
    if streng.startswith(("UA", "UE")):
        return streng.split('_')[2]
    return None


def sjekk_aktiv(rekke):
    sem1 = rekke['semester1']
    sem2 = rekke['semester2']
    return (sem1 <= 2025 and sem2 >= 2025)


url = "https://hvl.instructure.com/api/v1/accounts/1/sis_imports"
respons = requests.get(url, headers=hodeCanvas, params=parametre).json()
for r in respons['sis_imports']:
    if (r['user']['id']==1596) and (datetime.strptime(r['created_at'], "%Y-%m-%dT%H:%M:%SZ").hour < 3):
        for v in r['csv_attachments']:
            if v['filename'] == "courses.csv":
                fil = v['url']
                data = pd.read_csv(StringIO(requests.get(fil, headers=hodeCanvas).content.decode("utf-8")))
                break
        break
emne = pd.read_csv(StringIO(requests.get(fil, headers=hodeCanvas).content.decode("utf-8")))
temp = emne[~emne['course_id'].str.startswith('KK')]
emne_no = temp[~temp['term_id'].isna()]
emne_no['startår'] = emne_no['term_id'].apply(lambda x: x.split('-')[0].split('_')[0])
emne_no['sluttår'] = emne_no['term_id'].apply(lambda x: x.split('-')[1].split('_')[0] if '-' in x else x.split('_')[0])
emne_no['starttermin'] = emne_no['term_id'].apply(lambda x: x.split('-')[0].split('_')[1])
emne_no['slutttermin'] = emne_no['term_id'].apply(lambda x: x.split('-')[1].split('_')[1] if '-' in x else x.split('_')[0])
emne_no['semester1'] = emne_no['startår'].apply(lambda x:int(x)) + 0.5*(emne_no['starttermin']=="HØST")
emne_no['semester2'] = emne_no['sluttår'].apply(lambda x:int(x)) + 0.5*(emne_no['slutttermin']=="HØST")
emne_no['aktiv'] = emne_no.apply(sjekk_aktiv, axis=1)
aktive_emne = emne_no[emne_no['aktiv']]
aktive_emne['emnekode'] = aktive_emne['course_id'].apply(finn_emnekode)
unike_emne = aktive_emne['emnekode'].unique()

query = """
query Emneansvarleg($emnekode: [String!], $arstall: Int!, $terminbetegnelse: EmneIkkeUtloptITerminTerminbetegnelse!) {
    emner(
    filter: {eierInstitusjonsnummer: "203", emnekoder: $emnekode, ikkeUtloptITermin: {arstall: $arstall, terminbetegnelse: $terminbetegnelse}}
  ) {
    nodes {
      personroller {
        aktiv
        fagperson {
          navn {
            etternavn
            fornavn
          }
          feideBruker
        }
      }
      organisasjonsenhet {
        studieAnsvarlig {
          instituttnummer
          fakultet {
            fakultetsnummer
          }
        }
      }
    }
  }
}
"""

dataliste = []
for e in unike_emne:
    variable = {'emnekode': e, 'arstall': 2024, 'terminbetegnelse': 'VAR'}
    svar = query_FS_graphql(query, variable)
    data = svar['data']['emner']['nodes']
    for d in data:
        fakultet = d['organisasjonsenhet']['studieAnsvarlig']['fakultet']['fakultetsnummer']
        institutt = d['organisasjonsenhet']['studieAnsvarlig']['instituttnummer']
        for p in d['personroller']:
            if p['aktiv']:
                try:
                    etternamn = p['fagperson']['navn']['etternavn']
                    fornamn = p['fagperson']['navn']['fornavn']
                    epost = p['fagperson']['feideBruker']
                    dataliste.append([fakultet, institutt,e, etternamn, fornamn, epost])

                    print(fakultet, institutt, e, etternamn, fornamn, epost)
                except:
                    pass

emneansvarlege = pd.DataFrame(dataliste, columns = ['fakultet', 'institutt','emnekode', 'etternamn', 'fornamn', 'epost'])
emneansvarlege.to_excel("emneansvarlege.xlsx", index=None)