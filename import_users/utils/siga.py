import cx_Oracle
import pandas as pd

''' ORACLE DB - SIG@ '''
class ConsultaSIGA:
    
    query = '''SELECT p.nm_cpf_pess, p.nm_pess, p.nm_email_princ, pf.nm_progr_form, so.nm_org 
                       FROM admsiga.SIGA_PESSOA p, admsiga.SIGA_VINCULO v, admsiga.SIGA_PROGRAMA_FORMACAO pf, 
                       admsiga.SIGA_ORGAO so WHERE p.nm_cpf_pess = v.nm_cpf_pess 
                       AND v.cd_progr_form = pf.cd_progr_form AND pf.cd_org = so.cd_org AND v.fl_vinc = '1' 
                       ORDER BY p.nm_pess, pf.nm_progr_form'''

    def __init__(self, host, port, service_name, user, password):
        self.host = host
        self.port = port
        self.service_name = service_name
        self.user = user
        self.password = password
    

    def search(self, query_aux=None):
        df = pd.DataFrame()

        try:
            dsn_tns = cx_Oracle.makedsn(self.host, self.port, service_name=self.service_name)
            conn = cx_Oracle.connect(user=self.user, password=self.password, dsn=dsn_tns) 
            c = conn.cursor()
        except Exception as e:
            print("Error to connect to Oracle:", e)

        try:
            if query_aux is None:
                c.execute(self.query)
            else:
                c.execute(query_aux)
            df = pd.DataFrame(c)
            conn.close()
        except:
            print("I am unable to connect to the database.")

        return df