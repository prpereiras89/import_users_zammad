from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.combining import OrTrigger
from apscheduler.triggers.cron import CronTrigger
import pytz

import os
import pandas as pd
from utils.siga import ConsultaSIGA
from utils.sigs import ConsultaSIGS
from utils.sigaa import ConsultaSIGAA
from utils.clean_data import CleanData
from utils.import_zammad import DataToZammad
from  utils.update_zammad import UpdateZammad
from datetime import datetime

REC = pytz.timezone("America/Recife")

def start_import():

    print("+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
    print("+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
    print("++                                                                                                         ++")
    print("++                                                                                                         ++")
    print("++                                             IMPORT USERS                                                ++")
    print("++                                                                                                         ++")
    print("++                                                                                                         ++")
    print("+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
    print("+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")

    print("\n\n[STEP 1 - IMPORTING] " + datetime.now(REC).strftime('%H:%M:%S %d-%m-%Y') + " - GETTING DATA FROM DATABASES...")
    consulta_siga = ConsultaSIGA(os.getenv('SIGA_HOST'),
                        os.getenv('SIGA_PORT'),
                        os.getenv('SIGA_SERVICENAME'),
                        os.getenv('SIGA_USER'),
                        os.getenv('SIGA_PASSWORD'))
    df_siga = consulta_siga.search()
    print("[STEP 1.1 - IMPORTING] " + datetime.now(REC).strftime('%H:%M:%S %d-%m-%Y') + " - DATA FROM SIG@", df_siga)

    df_sigs = ConsultaSIGS(os.getenv('SIGS_HOST'),
                        os.getenv('SIGS_DATABASE'),
                        os.getenv('SIGS_USER'),
                        os.getenv('SIGS_PASSWORD')).search()
    print("\n\n[STEP 1.2 - IMPORTING] " + datetime.now(REC).strftime('%H:%M:%S %d-%m-%Y') + " - DATA FROM SIGs", df_sigs)
    
    df_sigaa = ConsultaSIGAA(os.getenv('SIGAA_HOST'),
                            os.getenv('SIGAA_DATABASE'),
                            os.getenv('SIGAA_USER'),
                            os.getenv('SIGAA_PASSWORD')).search()
    print("\n\n[STEP 1.3 - IMPORTING] " + datetime.now(REC).strftime('%H:%M:%S %d-%m-%Y') + " - DATA FROM SIGAA", df_sigaa, "\n[STEP 1] GETTING DATA FROM DATABASES FINISHED...")

    df_siga, df_sigs, df_sigaa = CleanData(df_siga, df_sigs, df_sigaa).clean_data()

    DataToZammad(df_siga, df_sigs, df_sigaa, consulta_siga).import_zammad()
    print("[STEP 4 - IMPORTING] " + datetime.now(REC).strftime('%H:%M:%S %d-%m-%Y') + " - COMPLETE...")

def start_update():
    
    print("+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
    print("+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
    print("++                                                                                                         ++")
    print("++                                                                                                         ++")
    print("++                                  UPDATING INFORMATION OF USERS                                          ++")
    print("++                                                                                                         ++")
    print("++                                                                                                         ++")
    print("+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
    print("+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")

    print("\n\n[STEP 1 - UPDATING] " + datetime.now(REC).strftime('%H:%M:%S %d-%m-%Y') + " - GETTING DATA FROM SIGS...")
    df_sigs_2 = ConsultaSIGS(os.getenv('SIGS_HOST'),
                        os.getenv('SIGS_DATABASE'),
                        os.getenv('SIGS_USER'),
                        os.getenv('SIGS_PASSWORD')).search()
    print("\n\n[STEP 2 - UPDATING] " + datetime.now(REC).strftime('%H:%M:%S %d-%m-%Y') + " - DATA FROM SIGs", df_sigs_2)

    UpdateZammad(df_sigs_2).update_zammad()
    print("[STEP 4 - IMPORTING] " + datetime.now(REC).strftime('%H:%M:%S %d-%m-%Y') + " - COMPLETE...")

if __name__ == '__main__':
    print("RUNNING SCHEDULER...")
    scheduler = BackgroundScheduler()
    trigger = OrTrigger([CronTrigger(day_of_week='mon,wed,fri', hour='20',timezone=REC)])
    trigger2 = OrTrigger([CronTrigger(day_of_week='sat', hour='10',timezone=REC)])
    scheduler.add_job(start_import, trigger)
    scheduler.add_job(start_update, trigger2)
    scheduler.start()
