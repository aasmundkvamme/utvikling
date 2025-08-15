import requests
import os
import pandas as pd
from datetime import date, datetime, timedelta
import time
from io import StringIO
idag = date.today().isoformat()[0:10]
tokenCanvas = os.environ['tokenCanvas']

headers = {'Authorization': f'Bearer {tokenCanvas}',}
params = {'per_page': 100,}

har_testemne = pd.read_csv("har_testemne.csv")
url = "https://hvl.instructure.com/api/v1/accounts/1/sis_imports"
svar = requests.get(url, headers=headers, params=params)
importerte_filar = svar.json()

no = datetime.now()
igår = (datetime.now() - timedelta(hours=24)).isoformat()
filliste = []
for i in importerte_filar['sis_imports']:
    import_dato = datetime.fromisoformat(i['created_at'].replace('Z', '+00:00')).replace(tzinfo=None)
    igår_dato = datetime.fromisoformat(igår)
    if (import_dato > igår_dato):
        for j in i['csv_attachments']:
            if j['filename'] == "users_filtered.csv":
                filliste.append({'dato': i['created_at'], 'url': j['url']})
users_liste = []
for i in filliste:
    temp = pd.read_csv(StringIO(requests.get(i['url'], headers=headers).content.decode("utf-8")))
    print(f"{i['dato']}: {len(temp)}")
    users_liste.append(temp)

overført = pd.concat(users_liste)
overført['brukarnamn'] = overført['login_id'].apply(lambda x: x.split('@')[0])
overført = overført[overført['brukarnamn'].str.isalpha()]
nye = overført[~overført['brukarnamn'].isin(har_testemne['brukar'])]

konto = 9044
løpenummer = 100
for i, r in nye.iterrows():
    # Først lager eg emnet
    namn = f"{r['first_name']} {r['last_name']}"
    url = f"https://hvl.instructure.com/api/v1/accounts/{konto}/courses"
    params = {
        'course[name]': f"Testemne for {namn}",
        'course[course_code]': "Testemne", 
        'course[sis_course_id]': f"hvl{idag}-testemne{løpenummer}-akv",
        'course[default_view]': 'syllabus',
        'course[syllabus_body]': "Dette er ditt testemne; her kan du gjere alt du vil prøve på i eit 'vanleg' emne. Ting du lager her kan du hente inn i andre emne til bruk i undervising.",
        }
    respons = requests.post(url, headers=headers, params=params)
    resultat = respons.json()
    id = resultat['id']

    # Så leiter eg etter canvas_id for brukaren
    sis_id = r['user_id']
    url = f"https://hvl.instructure.com/api/v1/accounts/1/users?search_term={sis_id}"
    user_id = requests.get(url, headers=headers).json()['id']

    # Så kan eg legge brukaren inn som lærar
    url = f"https://hvl.instructure.com/api/v1/courses/{id}/enrollments"
    params = {
        'enrollment[user_id]': user_id,
        'enrollment[type]': "TeacherEnrollment",
        'enrollment[enrollment_state]': 'active',
    }
    respons = requests.post(url, headers=headers, params=params)

    # Til slutt slår eg av vising av kalenderen
    url = f"https://hvl.instructure.com/api/v1/courses/{id}/settings"
    params = {
        'syllabus_course_summary': 'false',
    }
    respons = requests.put(url, headers=headers, params=params)

    # Og så gjenstår det berre å legge brukaren til i lista over dei som har eit testemne
    har_testemne[len(har_testemne)] = [r['user'], r['id'], r['name']]
    løpenummer += 1

# Og skrive dette til fil.
har_testemne.to_csv("har_testemne.csv", index = False)