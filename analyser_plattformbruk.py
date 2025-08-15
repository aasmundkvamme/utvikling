import pandas as pd
from datetime import date
import time
import glob
import akvut

idag = date.today().isoformat()

logger = akvut.lag_logger(f"analyser_plattformbruk_{idag}.log")


# Sjå etter nye filer:
filer = glob.glob("web_logs-*.txt")

if filer is not []:
    # Start analyse av dei nye filane, og legg til i den akkumulerte lista
    start_analyse = time.perf_counter()
    logger.info(f"Startar analyse av data")
    grand_total = 0
    rekker = []
    history_liste = []
    for f in filer:
        data = pd.read_csv(
            f,
            sep=",",
            usecols=[
                'value.timestamp',
                'value.user_agent',
                'value.user_id',
                'value.url',
                'value.course_id',
                'value.web_application_context_type',
                ]
            )
        data.dropna(subset=[
                'value.timestamp',
                'value.user_agent',
                'value.user_id',
                'value.url',
                'value.course_id',
                'value.web_application_context_type',
            ],
            inplace=True)
        data['dato'] = data['value.timestamp'].str[0:10]
        data['brukar'] = data['value.user_id'].apply(lambda x: str(int(x)))

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
            logger.info(f"Ser på data frå {dato}")
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

    rekker_i_dag = pd.concat(rekker)
    gamle_rekker = pd.read_csv("plattformbruk.csv")
    oppdatert_plattformbruk = pd.concat([rekker_i_dag, gamle_rekker]).groupby('dato').sum().reset_index()
    oppdatert_plattformbruk.to_csv("plattformbruk.csv", index=False)
    logger.info("Har oppdatert fila plattformbruk.csv")
    tidsbruk_analyse = time.perf_counter() - start_analyse

    innhald = f"Resultat frå web_log {idag}\n"
    innhald += f"Analysere data: {tidsbruk_analyse:.3f} sekund.\n"
    innhald += "\n"
    innhald += f"I alt er {grand_total} klikk registrert."

    # df = pd.read_csv("tid_logg.csv")
    # df.loc[len(df)] = [idag, 0, 0, tidsbruk_analyse]
    # df.to_csv('tid_logg.csv', index=False)

    avsendar = "aasmund.kvamme@hvl.no"
    mottakarar = ["aasmund.kvamme@hvl.no"]
    tittel = f"CD2 web log {idag}"
    vedlegg = ""
    akvut.send_epost(tittel, innhald, avsendar, mottakarar, vedlegg)

else:
    logger.info("Ingen web_logs-filer i dag.")