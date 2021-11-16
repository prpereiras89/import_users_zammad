import psycopg2
import pandas as pd

''' PostgreSQL - SIGAA '''
class ConsultaSIGAA:
    
    query = '''select CAST (p.cpf_cnpj AS VARCHAR(11)) as "CPF",p.nome as "NOME",p.email as "EMAIL",c.nome as "Curso",u2.nome as "UNIDADE" 
            from sigaa.public.discente d join sigaa.comum.pessoa p on p.id_pessoa = d.id_pessoa 
            left join curso c on c.id_curso = d.id_curso left join sigaa.comum.usuario u on u.id_pessoa = p.id_pessoa 
            left join sigaa.comum.unidade u2 on u2.id_unidade = u.id_unidade where d.status = 1'''

    def __init__(self, host, database, user, password):
        self.host = host
        self.database = database
        self.user = user
        self.password = password
    

    def search(self):
        df = pd.DataFrame()

        try:
            conn = psycopg2.connect(host=self.host,database=self.database,user=self.user,password=self.password)
            cur = conn.cursor()
        except Exception as e:
            print("Error to connect to Postgres:", e)
        
        try:
            cur.execute(self.query)
            df = pd.DataFrame(cur.fetchall()) 
        except:
            print("Can't SELECT from SIGAA")

        return df