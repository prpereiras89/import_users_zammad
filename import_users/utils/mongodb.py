import pandas as pd
import numpy as np
import pymongo
from pymongo import MongoClient
import os
from datetime import datetime

def _db_connection():
    client = MongoClient("mongodb://" + os.getenv("MONGODB_HOST")   + ":" + os.getenv('MONGODB_PORT') + "/",
                            username=os.getenv('MONGODB_USER'),
                            password=os.getenv('MONGODB_PASSWORD'), authSource='admin', authMechanism='SCRAM-SHA-256')
    db = client[os.getenv('MONGODB_DB')]
    return db
    

def save_logins(df):
    db = _db_connection()
    my_collection = db['login_users']

    login = df['login'].tolist()
    df_banco = get_many_logins({"login": {"$in": login}})

    if df_banco.empty:
        print("Inserindo logins...")
        df.reset_index(inplace=True)
        data_dict = df.to_dict("records")
        my_collection.insert_many(data_dict)
        print("Finalizado...")
        return

    common = df.merge(df_banco, how = 'inner', on=["login"])
    common = common[['login']]
    df_result = pd.concat([df,common]).drop_duplicates(keep=False)
            
    if not df_result.empty:
        print("Inserindo novos logins...")
        df_result.reset_index(drop=True, inplace=True)
        data_dict = df_result.to_dict("records")
        my_collection.insert_many(data_dict)
    else:
        print("Nenhum novo login!")

    print('Finalizado atualização do MongoDB...')


def get_many_logins(query=None):
    db = _db_connection()
    my_collection = db['login_users']
    
    if query is None:
        data_from_db = my_collection.find()
    else:
        data_from_db = my_collection.find(query)
    
    df = pd.DataFrame(data_from_db)
    
    return df
