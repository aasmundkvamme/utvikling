import pandas as pd
import glob
import akvut
from datetime import date, timedelta

today = date.today()
idag = today.isoformat()
igår = (today - timedelta(days=1)).isoformat()

logger = akvut.lag_logger(f'loggfil-diverse_statistikk-{idag}.log')

filer = glob.glob("web_logs-*.txt")

if filer is not []:
    logger.info(f"Les {len(filer)} filer")
    data = pd.concat([pd.read_csv(f) for f in filer])
    antal = len(data)
    logger.info(f"Antall linjer: {antal}")
    klikk = len(data[data['value.url'].str.contains('/images/thumbnails/')])
    logger.info(f"Antall klikk på profilbilete: {klikk}")

    nye_lister = pd.DataFrame([{'dato': idag, 'profilbilete': klikk}])
    gamle_lister = pd.read_csv("diverse_statistikk.csv")
    oppdaterte_lister = pd.concat([nye_lister, gamle_lister]).groupby('dato').sum().reset_index()
    oppdaterte_lister.to_csv("diverse_statistikk.csv", index=False)

    logger.info("Sender epost")
    avsendar = "aasmund.kvamme@hvl.no"
    mottakarar = ["aasmund.kvamme@hvl.no"]
    tittel = "Diverse statistikk frå web_log"
    vedlegg = "diverse_statistikk.csv"
    innhald = f"Andel klikk på profilbilete: {klikk/antal*100:.2f} ({klikk} av {antal})"

    akvut.send_epost(tittel, innhald, avsendar, mottakarar, vedlegg)
    logger.info("Sendt epost til {mottakarar}")
else:
    print("Ingen filer")