import requests
from requests.exceptions import HTTPError
import urllib3
from autenticacao import autenticacao
import pandas as pd
from datetime import date
import os
import time

time_incial = time.time()

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

ambiente = 'https://datacatalog.*********'

api = '/access/2/catalog/data/objects?'

link = '&includeDstLinks=true&includeRefObjects=false&includeSrcLinks=true&offset=0&pageSize=1000'

auth = autenticacao()
        
def conectaEDC():
        
    #resources = ('RSC_GDC_HIVE_GOLD', 'RSC_GDC_DTB_H')
        
    ofset500 = 'RSC_GDC_HIVE_H','RSC_DIC_HIVE_H'
    erros = 'RSC_GDC_HIVE_BRONZE_1','RSC_GDC_DTB_BRONZE', 'RSC_GDC_DTB_SILVER', 'RSC_GDC_HIVE_BRONZE_3', 'RSC_GDC_HIVE_H'
    #resorces = ('RSC_GDC_DTB_BRONZE', 'RSC_GDC_DTB_SILVER', 'RSC_GDC_HIVE_BRONZE_1')

    resources = ('RSC_GDC_HIVE_GOLD', 'RSC_GDC_DTB_H','RSC_GDC_DTB_GOLD','RSC_GDC_HIVE_SMART',
                'RSC_GDC_HIVE_DATASHARING','RSC_GDC_HIVE_BRONZE_2','RSC_DIC_DTB_H',
                'RSC_DIC_HIVE_DATASHARING','RSC_DIC_HIVE_SMART','RSC_GDC_HIVE_SILVER')
        
    for resource in resources:
        fq = f"fq-id%3A*{resource}*%2F*%2F*"
        fq1 = 'fq-core.classType%3A%22com.infa.ldm.relational.Table%22or%20%22com.infa.ldm.relational.View%22'
        url = ambiente + api + fq + link
        print(url)
        pass_edc = "Basic bGx5cmlv0kdyalWxsQOAwNDU-"
        json = requests.get(url, headers={"Authorization": "Basic + auth"}, verify=False)
        resultado = json.json()
        numero_objetos = resultado["metadata"]["totalCount"]
        numoffset = 0
        print(numero_objetos)
        ids ={}
        while numoffset <= numero_objetos:
            print(numoffset) 
            link_2 = f"&includeDstLinks=true&includeRefObjects=false&includeSrcLinks=true&offset="+str(numoffset)
            url_2 = ambiente + api + fq + link_2
            json_2 = requests.get(url_2, headers={"Authorization": "Basic + auth"}, verify=False)
            resultado = json_2.json()
            for key in resultado['items']:
                print(key['id'])
                ids[key['id']] = [key['id'],'','','','','','','','','','','','','','','','','',''.''.''.''.'']
            for mtd in key['facts']:
                if mtd['attributeId'] == 'core.name':
                    print(mtd['value'])
                ids[key['id']][1] = mtd['value']
            if mtd['attributeId'] == "core.classType":
                if mtd['value'] == 'com.infa.ldm.relational.Schema':
                    valor = 'Schema'
                    print("Schema")
                elif mtd['value'] == 'com.infa.Idm.relational.Table':
                    valor = 'Table'
                    print("Table")
                elif mtd['value'] == 'com.infa.Idm.relational.Column':
                    valor = 'Column'
                    print("Column")
                elif mtd['value'] == 'com.infa.Idm.relational.ViewColumn':
                    valor = 'ViewColumn'
                    print("ViewColumn")
                else:
                    print('-')
                print(valor)
                ids[key['id']] [2] = valor
            if mtd['attributeId'] == 'com.infa.appmodels.ldm.LDM_afb31a87_649e_4475_98bd_567ba2a8660c':
                print(mtd['value']) 
                ids[key['id']][3] = mtd['value']
            if mtd['attributeId'] == "com.infa.ldm.ootb.enrichments.businessDescription":
                print(mtd['value'])
                ids[key['id']][4] = mtd['value'] 
            if mtd['attributeId'] == 'com.infa.appmodels.ldm.LDM_aebe9fb6_5019_4e42_a4b7_611329fed3fe':
                print(mtd['value'])
                ids[key['id']][5] = mtd['value']
            if mtd['attributeId'] == "com.infa.appmodels.ldm.LDM_a895cd2c_7fdb_40e3_a279_e9a2ebab859e":
                print(mtd['value'])
                ids[key['id']][6] = mtd['value']
            if mtd['attributeId'] == 'com.infa.appmodels.ldm.LDM_210d7486_02b5_4b31_87cf_055bce2e740c':
                print(mtd['value'])
                ids[key['id']][7] = mtd['value']
            if mtd['attributeId'] == 'com.infa.ldm.ootb.enrichments.dataOwner':
                print(mtd['value'])
                ids[key['id']][8] = mtd['value']
            if mtd['attributeId'] == 'com.infa.ldm.ootb.enrichments.dataSteward':
                print(mtd['value'])
                ids[key['id']][9] = mtd['value']
            if mtd['attributeId'] == 'com.infa.appmodels.ldm.LDM_b6529de8_61ba_47d7_a268_7021e88ada6c':
                print(mtd['value'])
                ids[key['id']][10] == mtd['value']
            if mtd['attributeId'] =='com.infa.ldm.ootb.enrichments.displayName':
                print(mtd['value'])
                ids[key['id']][11] == mtd['value']
            if mtd["attributeId"] == "com.inta.appmodels.Idm.LDM_b5e303a0_32c0_45+3_abec_dc88cd0a432a":
                print(mtd['value']) 
                ids[key['id']][12] == mtd['value']
            if mtd['attributeId'] == 'com.infa.ldm.ootb.enrichments.assetLocation':
                print(mtd['value']) 
                ids[key['id']][13] = mtd['value']
            if mtd['attributeId'] == 'com.infa.appmodels.ldm.LDM_ae6c9601_1bf2_4ddb_b55c_d79084414660':
                print(mtd['value'])
                ids[key['id']][14] = mtd['value'] 
            if mtd['attributeId'] == 'com.infa.appmodels.ldm.LDM_96a78b9b_6520_4a2f_8a4d_4b17df856eb7':
                print(mtd['value'])
                ids[key['id']][15] = mtd['value'] 
            if mtd['attributeId'] == 'com.infa.appmodels.Aldm.LDM_157efd21_dbb1_4c38_a252_250074a6390b':
                print(mtd['value'])
                ids[key['id']][16] = mtd['value']
            if mtd['attributeId'] =='com.infa.appmodels.ldm.LDM_633f6cce_706c_4e97_bd70_2c43aefc7ff5':
                print(mtd['value'])
                ids[key['id']][17] = mtd['value']
            if mtd['attributeId'] == 'com.infa.appmodels.ldm.LDM_3c39fda9_69e2_4fdd_982c_e829813443f8': #Pontos
                print(mtd['value'])
                ids[key['id']][18] = mtd['value']
            if mtd['attributeId'] == 'com.infa.appmodels.ldm.LDM_2f083890_a144_4b12_90ea_a532f3c8dbab': #SLA
                print(mtd['value'])
                ids[key['id']][19] = mtd['value']
            if mtd['attributeId'] == 'com.infa.appmodels.ldm.LDM_f02c2637_af28_466e_917b_5dd5bc9b9e69':
                print(mtd['value'])
                ids[key['id']][20] - mtd['value']
            if mtd['attributeId'] == 'com.infa.appmodels.ldm.LDM_1369dbb5_de13_48b4_95c8_ef088a446fd9': #Tempest
                print(mtd['value'])
                ids[key['id']][21] - mtd['value']
            if mtd['attributeId'] == 'com.infa.appmodels.ldm.LDM_cd616879_aaf6_4655_a2be_240183631837':
                print(mtd['value'])
                ids[key['id']][22] - mtd['value']
            if mtd['attributeId'] == "com.infa.ldm.ootb.enrichments.URL":
                print(mtd['value'])
                ids[key['id']][23] - mtd['value']
        numoffset-numoffset + 1000
    data = []
    for id, value in ids.items():
        print(value)
        data.append(value)
    df = pd.DataFrame(data, columns = ['ID', 'name', 'classType', 'Anonymization', 'Business Description',
    'Primary Key', 'Data Custodian', 'Data Owner', 'Data Steward', 'Data Owner Delegation','Display Name',
    'Interface', 'Location', 'Confidentiality Level', 'Data Source', 'Frequency','Retention Period (days)',
    'Focal Points', 'SLA','Source System Technology', 'Timeliness', 'Processing Type','URL'])
    print(df)
    caminho = 'C:\\Users\\*****************'
    file = caminho + resource + '.xlsx'
    df.to_excel(file)
data_atual = date.today()
conectaEDC()
