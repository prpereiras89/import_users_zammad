import psycopg2
import pandas as pd

''' PostgreSQL - SIGs '''
class ConsultaSIGS:
    
    query = '''select CAST (p.cpf_cnpj AS VARCHAR(11)) as "CPF", p.nome as "NOME", p.email as "EMAIL SERVIDOR", u.email as "EMAIL USUÁRIO", 
            c.denominacao "CARGO", case when d.id_designacao is null then \'--\' else a.descricao end, u2.nome "EXERCÍCIO", 
            u3.nome "LOTAÇÃO", c2.descricao as "CATEGORIA" from RH.servidor s left join comum.pessoa p on 
            p.id_pessoa = s.id_pessoa left join rh.categoria c2 on c2.id_categoria = s.id_categoria left join comum.unidade u2 on 
            u2.id_unidade = s.id_unidade left join comum.unidade u3 on u3.id_unidade = s.id_unidade_lotacao left join 
            rh.cargo c on	c.id = s.id_cargo left join comum.usuario u on u.id_pessoa = p.id_pessoa left join rh.designacao d on 
            d.id_servidor = s.id_servidor left join rh.atividade a on a.id_atividade = d.id_atividade left join comum.tipo_usuario tu 
            on tu.id = u.tipo where s.id_ativo = 1'''

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
            print("Can't SELECT from SIGs")

        return df