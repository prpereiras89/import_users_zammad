import pandas as pd

class CleanData:
    def __init__(self, df_siga, df_sigs, df_sigaa):
        self.df_siga = df_siga
        self.df_sigs = df_sigs
        self.df_sigaa = df_sigaa

    def clean_data(self):
        print("\n[STEP 2] CLEANING DATA...")
        self.df_siga.columns = ['cpf','nome','email_institucional','programa_formacao','orgao']
        self.df_sigs.columns = ['cpf', 'nome', 'email_p0', 'email_p1','cargo','descricao','exercicio','lotacao','categoria']
        self.df_sigaa.columns = ['cpf', 'nome', 'email', 'curso','unidade']

        # treating cpf
        for i in range(len(self.df_siga['cpf'])):
            try:
                if len(self.df_siga.loc[i, 'cpf']) <= 10:
                    while len(self.df_siga.loc[i, 'cpf']) < 11:
                        self.df_siga.loc[i, 'cpf'] = '0' + self.df_siga.loc[i, 'cpf']
            except:
                pass

        for i in range(len(self.df_sigaa['cpf'])):
            try:
                if len(self.df_sigaa.loc[i, 'cpf']) <= 10:
                    while len(self.df_sigaa.loc[i, 'cpf']) < 11:
                        self.df_sigaa.loc[i, 'cpf'] = '0' + self.df_sigaa.loc[i, 'cpf']
            except:
                pass
            
        for i in range(len(self.df_sigs['cpf'])):
            try:
                if len(self.df_sigs.loc[i, 'cpf']) <= 10:
                    while len(self.df_sigs.loc[i, 'cpf']) < 11:
                        self.df_sigs.loc[i, 'cpf'] = '0' + self.df_sigs.loc[i, 'cpf']
            except:
                pass

        self.df_siga = self.df_siga[self.df_siga['cpf'].notna()]
        self.df_sigs = self.df_sigs[self.df_sigs['cpf'].notna()]
        self.df_sigaa = self.df_sigaa[self.df_sigaa['cpf'].notna()]

        self.df_siga = self.df_siga.reset_index(drop=True)
        self.df_sigs = self.df_sigs.reset_index(drop=True)
        self.df_sigaa = self.df_sigaa.reset_index(drop=True)
        
        self.df_siga['cpf'] = self.df_siga['cpf'].astype('str').map(lambda s: s[0:3] + '.' + s[3:6] + '.' + s[6:9] + '-' + s[9:])
        self.df_sigs['cpf'] = self.df_sigs['cpf'].astype('str').map(lambda s: s[0:3] + '.' + s[3:6] + '.' + s[6:9] + '-' + s[9:])
        self.df_sigaa['cpf'] = self.df_sigaa['cpf'].astype('str').map(lambda s: s[0:3] + '.' + s[3:6] + '.' + s[6:9] + '-' + s[9:])
        print("\n\n[STEP 2] CLEANING DATA FINISHED")

        return self.df_siga, self.df_sigs, self.df_sigaa
