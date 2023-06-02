import s3fs
import os
import io
import pymssql
import pandas as pd
import tkinter as tk
import matplotlib.pyplot as plt


def transforma_df(df):
    df.timestamp=pd.to_datetime(df.timestamp,format='%m/%d/%Y')
    return df


def obtener_id_user(id_user):
    try:        
        conn = pymssql.connect(server=server, user=username, password=password, database=database)
        cursor = conn.cursor()
        query = f"SELECT nombre FROM TBL_PRICE_USER WHERE ID = '{id_user}'"
        cursor.execute(query)
        row = cursor.fetchone()        
        conn.close()        
        if row:
            return row[0]  
        else:
            return 'NO'
    except Exception as e:
        return 'NO'
    

def obtener_stadisticas():
    try:        
        conn = pymssql.connect(server=server, user=username, password=password, database=database)
        cursor = conn.cursor()
        query = "select count(1),avg(price),min(price),max(price) from TBL_PRICE_ID"
        cursor.execute(query)
        row = cursor.fetchone()        
        conn.close()        
        if row:
            count = row[0]
            avg_price = row[1]
            min_price = row[2]
            max_price = row[3]
            return count, avg_price, min_price, max_price
        else:
            return 'NO'
    except Exception as e:
        return 'NO'    
        
def registrar_data(data):
    try:        
        conn = pymssql.connect(server=server, user=username, password=password, database=database)
        cursor = conn.cursor()
        
        for index, row in data.iterrows():
            print(index)
            query = "INSERT INTO TBL_PRICE_ID(fecha, price, user_id) \
                    VALUES (%s, %s, %s)"
            values = (row['timestamp'], row['price'], row['user_id'])
            cursor.execute(query, values)
            
            existe=obtener_id_user(row['user_id'])                  
            if existe=='NO': 
                query = "INSERT INTO cliente (ID, NOMBRE) \
                VALUES (%s, %s)"
                values = (row['user_id'], 'xxxx')
                cursor.execute(query, values)        
            conn.commit()  
        conn.close()        
        return "Registro exitoso"       
    except Exception as e:
        return f"Error de conexión: {str(e)}"
    
    
def nombres_archivos_en_s3(s3_name, ruta):    
    lista_archivos=[]
    fs = s3fs.S3FileSystem(anon=True)
    archivos = fs.ls(f"{s3_name}/{ruta}")    
    for archivo in archivos:
        lista_archivos.append(archivo)
    return lista_archivos

def lee_archivos_en_s3(archivo):
    fs = s3fs.S3FileSystem(anon=True)
    with fs.open(archivo, "r") as f:
        contenido = f.read()    
    df = pd.read_csv(io.StringIO(contenido))
    return df
    
def extraer_archivos_locales(ruta):
    if os.path.exists(ruta) and os.path.isdir(ruta):
        archivos = os.listdir(ruta)
        archivos = [archivo for archivo in archivos if os.path.isfile(os.path.join(ruta, archivo))]
        for archivo in archivos:
            print(archivo)
    else:
        print("La carpeta no existe.")

def ejecutar_pipeline(df):
    #estadisticas inciales 
    cantidad, avg_price, min_price, max_price = obtener_stadisticas()
    print("Cantidad inicial: ", cantidad,"\npromedio: ",avg_price,"\nMin Precio: ",min_price,"\nMax_precio:",max_price)
    #transforma la data 
    df=transforma_df(df)
    #Carga la data 
    salida=registrar_data(df)
    print(salida)
    
    
    


##Configruacion Sql SErver Azure
server = 'sql-server-test-analitica.database.windows.net'
database = 'consultoria'
username = 'consultoria'
password = 'Colombia23*'

##configuracion S3 AWS
arvhivos=nombres_archivos_en_s3('aws-logs-793650758881-us-east-2', 'ETLPython/')


##Proceso de ETL

#labels = ['Count', 'Average Price', 'Minimum Price', 'Maximum Price']
#plt.bar(labels,[cantidad, avg_price, min_price, max_price])

for i in sorted(arvhivos):   
    if "validation" in i:
        continue
    else :
        ## Extrae los archivos del datalake en este caso simulamos que los datos son dejados en un S3
        #extraer_archivos_locales('data')
        #archivo='aws-logs-793650758881-us-east-2/ETLPython/2012-1.csv'
        print(i)
        #df=lee_archivos_en_s3(i)
        #df.head()
        #ejecutar_pipeline(df)
        