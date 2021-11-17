import os
import requests
import pandas as pd
from collections import defaultdict

class DataToZammad:
    def __init__(self, df_siga, df_sigs, df_sigaa, consulta_siga):
        self.df_siga = df_siga
        self.df_sigs = df_sigs
        self.df_sigaa = df_sigaa
        self.consulta_siga = consulta_siga

    def import_zammad(self):
        print("\n[STEP 3] IMPORTING TO ZAMMAD...")
        csv_columns = ["id","login","firstname","lastname","email","web","phone","fax","mobile","department",
                       "street","zip","city","country","address","vip","verified","active","note","last_login",
                       "out_of_office","out_of_office_start_at","out_of_office_end_at","cpf","lotacao","roles",
                       "karma_user","cargo","funcao","categoria","exercicio","curso","organization"]

        aux_dict = defaultdict(list)

        for i in csv_columns:
            aux_dict[i] = []

        print("[SIG@] LOADING...\n")
        for i in range(0, len(self.df_siga['cpf'])):
            if self.df_siga['cpf'].iloc[i] not in aux_dict['cpf']:
                if isinstance(self.df_siga['email_institucional'].iloc[i], str):
                    if "ufrpe.br" in self.df_siga['email_institucional'].iloc[i].lower():
                        aux_dict['login'].append(self.df_siga['email_institucional'].iloc[i].lower())
                        aux_dict['email'].append(self.df_siga['email_institucional'].iloc[i].lower())

                        aux_dict['id'].append('')
                        aux_dict['cpf'].append(self.df_siga['cpf'].iloc[i])
                    
                
                        name = self.df_siga['nome'].iloc[i].split(" ",1)
                
                        if len(name) > 1:
                            aux_dict['firstname'].append(name[0])
                            aux_dict['lastname'].append(name[1])
                        else:
                            aux_dict['firstname'].append(name)
                            aux_dict['lastname'].append(name)

                        aux_dict['vip'].append('false')
                        aux_dict['verified'].append('false')
                        aux_dict['active'].append('true')
                        aux_dict['out_of_office'].append('false')
                        aux_dict['lotacao'].append('')
                        aux_dict['roles'].append('Customer')
                        aux_dict['karma_user'].append('')
                        aux_dict['cargo'].append('')
                        aux_dict['funcao'].append('')
                        aux_dict['categoria'].append('Discente')
                        aux_dict['exercicio'].append('')
                        aux_dict['curso'].append(self.df_siga['programa_formacao'].iloc[i])
                        aux_dict['organization'].append('')



        print("[SIGAA] LOADING...\n")
        for i in range(0, len(self.df_sigaa['cpf'])):
            if "CODAI" in str(self.df_sigaa['unidade'].iloc[i]) and self.df_sigaa['cpf'].iloc[i] not in aux_dict['cpf']:
                aux_dict['login'].append(str(self.df_sigaa['email'].iloc[i]).lower())
                aux_dict['email'].append(str(self.df_sigaa['email'].iloc[i]).lower())

                aux_dict['id'].append('')
                aux_dict['cpf'].append(self.df_sigaa['cpf'].iloc[i])
            

                name = self.df_sigaa['nome'].iloc[i].split(" ",1)

                if len(name) > 1:
                    aux_dict['firstname'].append(name[0])
                    aux_dict['lastname'].append(name[1])
                else:
                    aux_dict['firstname'].append(name)
                    aux_dict['lastname'].append(name)

                aux_dict['vip'].append('false')
                aux_dict['verified'].append('false')
                aux_dict['active'].append('true')
                aux_dict['out_of_office'].append('false')
                aux_dict['lotacao'].append('')
                aux_dict['roles'].append('Customer')
                aux_dict['karma_user'].append('')
                aux_dict['cargo'].append('')
                aux_dict['funcao'].append('')
                aux_dict['categoria'].append('Discente')
                aux_dict['exercicio'].append('')
                aux_dict['curso'].append(self.df_sigaa['curso'].iloc[i])
                aux_dict['organization'].append('')
                


        print("[SIGS] BUILDING QUERY...\n")
        aux_servidor = []
        for i in range(0, len(self.df_sigs['cpf'])):
            
            e = str(self.df_sigs['cpf'].iloc[i]).replace(".","")
            e = e.replace("-","")
            
            if i == 0:
                e = "'" + e + "',"
            elif i%100 > 0 and i < len(self.df_sigs['cpf']) - 1:
                e = "'" + e + "',"
            elif i%100 == 0 and i < len(self.df_sigs['cpf']) - 1:
                e = "'" + e + "') OR p.nm_cpf_pess IN ("
            else:
                e = "'" + e + "')"

            aux_servidor.append(e)

        query = '''SELECT p.nm_cpf_pess, p.nm_email_princ FROM admsiga.SIGA_PESSOA p WHERE p.nm_cpf_pess IN (''' + ''.join(aux_servidor)

        servidor = self.consulta_siga.search(query)
        servidor.columns = ['cpf','email_institucional']
        servidor['cpf'] = servidor['cpf'].astype('str').apply(lambda s: s[0:3] + '.' + s[3:6] + '.' + s[6:9] + '-' + s[9:])

        self.df_sigs = pd.merge(self.df_sigs, servidor, on='cpf')

        print("[SIGS] LOADING...\n")
        for i in range(0, len(self.df_sigs['cpf'])):
            if self.df_sigs['cpf'].iloc[i] in aux_dict['cpf']:
                index = aux_dict['cpf'].index(self.df_sigs['cpf'].iloc[i])
                if aux_dict['email'][index] != '' or aux_dict['email'][index] is not None:
                    aux_dict['cargo'][index] = self.df_sigs['cargo'].iloc[i]
                    aux_dict['exercicio'][index] = self.df_sigs['exercicio'].iloc[i]
                    aux_dict['lotacao'][index] = self.df_sigs['lotacao'].iloc[i]
                    aux_dict['categoria'][index] = self.df_sigs['categoria'].iloc[i] if aux_dict['categoria'][index] != '' or aux_dict['categoria'][index] is not None else aux_dict['categoria'][index] + ' e ' + self.df_sigs['categoria'].iloc[i]
                    aux_dict['funcao'][index] = self.df_sigs['descricao'].iloc[i]
            else:
                if "ufrpe.br" in str(self.df_sigs['email_institucional'].iloc[i]).lower():
                    aux_dict['login'].append(str(self.df_sigs['email_institucional'].iloc[i]).lower())
                    aux_dict['email'].append(str(self.df_sigs['email_institucional'].iloc[i]).lower())

                    aux_dict['id'].append('')
                    aux_dict['cpf'].append(self.df_sigs['cpf'].iloc[i])
                

                    name = self.df_sigs['nome'].iloc[i].split(" ",1)

                    if len(name) > 1:
                        aux_dict['firstname'].append(name[0])
                        aux_dict['lastname'].append(name[1])
                    else:
                        aux_dict['firstname'].append(name)
                        aux_dict['lastname'].append(name)

                    aux_dict['vip'].append('false')
                    aux_dict['verified'].append('false')
                    aux_dict['active'].append('true')
                    aux_dict['out_of_office'].append('false')
                    aux_dict['roles'].append('Customer')
                    aux_dict['karma_user'].append('')
                    aux_dict['cargo'].append(self.df_sigs['cargo'].iloc[i])
                    aux_dict['exercicio'].append(self.df_sigs['exercicio'].iloc[i])
                    aux_dict['lotacao'].append(self.df_sigs['lotacao'].iloc[i])
                    aux_dict['categoria'].append(self.df_sigs['categoria'].iloc[i])
                    aux_dict['funcao'].append(self.df_sigs['descricao'].iloc[i])
                    aux_dict['curso'].append('')
                    aux_dict['organization'].append('')
            
            

        self.df_sigaa = self.df_sigaa[self.df_sigaa["cpf"].isin(aux_dict['cpf']) == True]

        aux_dict = pd.DataFrame(dict([ (k,pd.Series(v)) for k,v in aux_dict.items() ]))
        df_zammad = pd.DataFrame.from_dict(aux_dict)

        nan_value = float("NaN")
        df_zammad.replace("", nan_value, inplace=True)
        df_zammad.dropna(subset=['login','email'],inplace=True)
        df_zammad.replace(nan_value,"", inplace=True)

        df_zammad['name'] = df_zammad['firstname'].astype(str) + " " + df_zammad['lastname'].astype(str)

        #remove users from Garanhuns
        df_zammad = df_zammad[~df_zammad['lotacao'].astype(str).str.contains("GARANHUNS")]
        df_zammad = df_zammad[~df_zammad['curso'].astype(str).str.contains("UAG")]

        df_aux = pd.DataFrame()
        try:
            df_aux = pd.read_csv("csv/users_zammad.csv", header=0, sep = ",", encoding='utf-8-sig', dtype={'login': 'string'})
        except Exception as e:
            print(e)

        df_zammad.to_csv("csv/users_zammad.csv", header=True, columns=["login"], sep = ",", index=False, encoding='utf-8-sig')
    
        if not df_aux.empty:
            common = df_zammad.merge(df_aux, on=["login"])
            df_zammad = df_zammad[~df_zammad['login'].isin(common['login'])]

        
        print("\n\nDF_ZAMMAD\n", df_zammad)

        if df_zammad.empty:
            return
        
        print("\n[STEP 3] IMPORTING TO ZAMMAD...")

        # SERVIÇOS
        access_token = os.getenv("TOKEN_ZAMMAD")
        headers = {'Authorization' : 'Bearer ' + access_token}

        for i in range(0,len(df_zammad['cpf'])):
            try:
                response = requests.post(os.getenv('HOST_ZAMMAD'), headers=headers,
                                        data = {
                                            #"id":df_zammad['id'].iloc[i],
                                            "login":df_zammad['login'].iloc[i],
                                            "firstname":df_zammad['firstname'].iloc[i],
                                            "lastname":df_zammad['lastname'].iloc[i],
                                            "email":df_zammad['email'].iloc[i],
                                            "web":df_zammad['web'].iloc[i],
                                            "phone":df_zammad['phone'].iloc[i],
                                            "fax":df_zammad['fax'].iloc[i],
                                            "mobile":df_zammad['mobile'].iloc[i],
                                            "department":df_zammad['department'].iloc[i],
                                            "street":df_zammad['street'].iloc[i],
                                            "zip":df_zammad['zip'].iloc[i],
                                            "city":df_zammad['city'].iloc[i],
                                            "country":df_zammad['country'].iloc[i],
                                            "address":df_zammad['address'].iloc[i],
                                            "vip":df_zammad['vip'].iloc[i],
                                            "verified":df_zammad['verified'].iloc[i],
                                            "active":df_zammad['active'].iloc[i],
                                            "note":df_zammad['note'].iloc[i],
                                            "last_login":df_zammad['last_login'].iloc[i],
                                            "out_of_office":df_zammad['out_of_office'].iloc[i],
                                            "out_of_office_start_at":df_zammad['out_of_office_start_at'].iloc[i],
                                            "out_of_office_end_at":df_zammad['out_of_office_end_at'].iloc[i],
                                            "cpf":df_zammad['cpf'].iloc[i][0:14],
                                            "lotacao":df_zammad['lotacao'].iloc[i],
                                            "roles":df_zammad['roles'].iloc[i],
                                            "karma_user":df_zammad['karma_user'].iloc[i],
                                            "cargo":df_zammad['cargo'].iloc[i],
                                            "funcao":df_zammad['funcao'].iloc[i],
                                            "categoria":df_zammad['categoria'].iloc[i],
                                            "exercicio":df_zammad['exercicio'].iloc[i],
                                            "curso":df_zammad['curso'].iloc[i],
                                            "organization":None,

                                            })


                print("[" + str(i) + "] - RESPONSE:" + str(response) + " Name: " + df_zammad['name'].iloc[i])

            except Exception as e:
                print("exception: " + str(i), e)
