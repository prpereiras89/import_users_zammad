import os
import json
import requests
import pandas as pd

import pytz

REC = pytz.timezone("America/Recife")

class UpdateZammad:
    def __init__(self, df_sigs):
        self.df_sigs = df_sigs

    def update_zammad(self):
    
        access_token = os.getenv("TOKEN_ZAMMAD")
        headers = {'Authorization' : 'Bearer ' + access_token}
        
        for email_servidor in self.df_sigs["login"]:
            if "@ufrpe.br" in email_servidor:
                response = requests.get(os.getenv('HOST_ZAMMAD') + "search?query=login:" + str(email_servidor), headers=headers)

                aux = json.loads(response.text)
                
                aux_dict = {"id": '', "login": '', "firstname": '', "lastname": '', "email": '', "cpf": '', "lotacao": '', "cargo": '', "funcao": '', "categoria": '', "exercicio": '', "curso": ''}

                for k in aux_dict.keys():
                    aux_dict[k] = aux[0][k]

                df = pd.DataFrame(aux_dict, index=[0])
                self.df_sigs.drop(df.columns.difference(df.columns), 1, inplace=True)



                if self.df_sigs.loc[self.df_sigs["cpf"] == df["cpf"][0]] is not None:
                    for column in df.columns:
                        if column not in  ["cpf", "id", "login"]:
                            df.iloc[0, df.columns.get_loc(column)] = self.df_sigs.loc[self.df_sigs["cpf"] == df["cpf"][0], column]

                print(df)    

                response = requests.put(os.getenv('HOST_ZAMMAD') + str(df['id'][0]), headers=headers,
                                            data = {
                                                'firstname':df['firstname'][0], 
                                                'lastname':df['lastname'][0], 
                                                'email':df['email'][0], 
                                                'lotacao':df['lotacao'][0],
                                                'cargo':df['cargo'][0], 
                                                'funcao':df['funcao'][0], 
                                                'categoria':df['categoria'][0], 
                                                'exercicio':df['exercicio'][0], 
                                                'curso':df['curso'][0],
                                            }
                                        )

                print(response)