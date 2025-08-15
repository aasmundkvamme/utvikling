import requests
import os
import pandas as pd
import akvut
import time

query = """
query MyQuery($antal: Int, $start: String) {
    programStudieretter(
        filter: {eierOrganisasjonskode: "0203", aktivStatus: AKTIV}, 
        after: $start,
        first: $antal) {
        pageInfo {
            endCursor
            hasNextPage
        }
        nodes {
            personProfil {
                personlopenummer
            }
            studieprogram {
                kode
            }
            campus {
                kode
            }
            kull {
                termin {
                    arstall
                    betegnelse {
                        navnAlleSprak {
                            nb
                        }
                    }
                }
            }
        }
    }
}
"""

start_les_FS_programstudierettar = time.perf_counter()
programstudierettar = []
n = 0
hentmeir = True
antal_per_side = 1000
while hentmeir:
    if n == 0:
        start = None
    else:
        start = svar['data']['programStudieretter']['pageInfo']['endCursor']
    variable = {'antal': antal_per_side, 'start': start}
    n += 1
    print(f"Hentar side {n} ({antal_per_side*(n-1)}-{antal_per_side*n})")
    svar = akvut.akv_query_FS_graphql(query, variable)
    print(len(svar))
    try:
        for pSr in svar['data']['programStudieretter']['nodes']:
            try:
                plnr = pSr['personProfil']['personlopenummer']
            except (TypeError, KeyError, ValueError):
                plnr = ''
            try:
                studieprogram = pSr['studieprogram']['kode']
            except (TypeError, KeyError, ValueError):
                studieprogram = ''
            try:
                campus = pSr['campus']['navnAlleSprak']['nb']
            except (TypeError, KeyError, ValueError):
                campus = ''
            try:
                år = pSr['kull']['termin']['arstall']
            except (TypeError, KeyError, ValueError):
                år = ''
            try:
                termin = pSr['kull']['termin']['betegnelse']['navnAlleSprak']['nb']
            except (TypeError, KeyError, ValueError):
                termin = ''
            programstudierettar.append([plnr, studieprogram, campus, år, termin])
    except (KeyError, TypeError):
        raise Exception("Feil i henting av FS-data.")
    hentmeir = svar['data']['programStudieretter']['pageInfo']['hasNextPage']
