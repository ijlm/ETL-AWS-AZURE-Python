import s3fs
import os
import io
import pymssql
import pandas as pd
import tkinter as tk
import matplotlib.pyplot as plt



##Configruación conexion Sql Server Azure
server = 'sql-server-test-analitica.database.windows.net'
database = 'consultoria'
username = 'consultoria'
password = 'Colombia23*'

##Extracción de informacion en partes 
#primera parte busca los archivos a cargar lotes y configura la lista al proceso
def nombres_archivos_en_s3(s3_name, ruta):    
    lista_archivos=[]
    fs = s3fs.S3FileSystem(anon=True)
    archivos = fs.ls(f"{s3_name}/{ruta}")    
    for archivo in archivos:
        lista_archivos.append(archivo)
    return lista_archivos

#carga los archivos a memoria y retorna un dataframe listo para trabajar
def lee_archivos_en_s3(archivo):
    fs = s3fs.S3FileSystem(anon=True)
    with fs.open(archivo, "r") as f:
        contenido = f.read()    
    if contenido:
        df = pd.read_csv(io.StringIO(contenido))
        return df
    else:
        return None
    

#Formato de trabajo local en caso de no usar S3
def extraer_archivos_locales(ruta):
    if os.path.exists(ruta) and os.path.isdir(ruta):
        archivos = os.listdir(ruta)
        archivos = [archivo for archivo in archivos if os.path.isfile(os.path.join(ruta, archivo))]
        for archivo in archivos:
            print(archivo)
    else:
        print("La carpeta no existe.")
   
   
#transformacion de los datos, formato de fechas y llenado de nulos
def transforma_df(df):
    df.timestamp=pd.to_datetime(df.timestamp,format='%m/%d/%Y')
    df.fillna(0,inplace=True)
    return df

## funcion que busca validar si existe el ID del usuario para registrar
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

#proceso de load carga los datos a la BD
def registrar_data(data,cant,media,mini,maxi,tipo):
    try:        
        conn = pymssql.connect(server=server, user=username, password=password, database=database)
        cursor = conn.cursor()
        
        j=0
        price=0
        n_max=maxi
        n_min=mini
        for index, row in data.iterrows():
            
            query = "INSERT INTO TBL_PRICE_ID(fecha, price, user_id) \
                    VALUES (%s, %s, %s)"
            values = (row['timestamp'], row['price'], row['user_id'])
            cursor.execute(query, values)
            conn.commit() 
            
            #existe=obtener_id_user(row['user_id'])                  
            #if existe=='NO': 
            #    query = "INSERT INTO cliente (ID, NOMBRE) \
            #    VALUES (%s, %s)"
            #    values = (row['user_id'], 'xxxx')
            #    cursor.execute(query, values)   
            #      
            
            price=price+row['price']
            j=j+1 
            n_max= maxi if row['price']<maxi else row['price']
            n_min= mini if row['price']>mini else row['price']
            obtener_stadisticas_poderadas(cant,media,n_min,n_max,j,price,tipo)  
               
            
        #obtener_stadisticas()               
        conn.close()        
        #return None 
        return "Registro exitoso"       
    except Exception as e:
        return f"Error de conexión: {str(e)}"
    
## funcion que extrae estadisticas 
def obtener_stadisticas(tipo):
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
            avg_price= 0 if avg_price is None else avg_price   
            min_price= 0 if min_price is None else min_price   
            max_price= 0 if max_price is None else max_price   
            print("############################ Datos En BD ########################################")
            print("##### Cantidad:", count," | Promedio:",round(avg_price,2)," | Min Precio:",min_price," | Max precio:",max_price,"  #####") 
            print("###################################################################################")
            archivo = open("salidas/",tipo,".txt", "w")
            archivo.write("############################ Datos En BD ########################################\n")
            archivo.write("##### Cantidad:", count," | Promedio:",round(avg_price,2)," | Min Precio:",min_price," | Max precio:",max_price,"  #####\n")
            archivo.write("###################################################################################\n")
            archivo.close()

            return count, avg_price, min_price, max_price
        else:
            #return 'NO'
            None
    except Exception as e:
        #return 'NO'    
        print(e)

#funcion que nos permite realizar calculos sin consultar la BD
def obtener_stadisticas_poderadas(cant,media,mini,maxi,j,price,tipo):
    try:
        print("Calculo en Memoria; Cantidad:",(j+cant),"Media:",round(((price)+(cant*media))/(j+cant),2),"Min Precio",mini,"max Precio",maxi)
        archivo = open(tipo,".txt", "w")
        archivo.write("Calculo en Memoria; Cantidad:",(j+cant),"Media:",round(((price)+(cant*media))/(j+cant),2),"Min Precio",mini,"max Precio",maxi)
        archivo.close()
    except Exception as e:
        #return 'NO'    
        print(e)

         
## PIPELINE de ETL
def ejecutar_pipeline(tipo):    
    if tipo=='Normal':
        archivos=nombres_archivos_en_s3('aws-logs-793650758881-us-east-2', 'ETLPython')
        j=1
        for i in sorted(archivos):       
            if str(j)+".csv" in i:
                ## Extrae los archivos del datalake en este caso simulamos que los datos son dejados en un S3
                #extraer_archivos_locales('data')                
                df=lee_archivos_en_s3(i)    
                #transforma la data 
                df=transforma_df(df)
                #Carga la data 
                cant,media,mini,maxi=obtener_stadisticas(tipo)
                salida=registrar_data(df,cant,media,mini,maxi,tipo)
                print(salida)
                j=j+1
                 
            else :
                print("no existe el consecutivo de carga")
            
                
    elif tipo=='Validacion':       
        archivo='aws-logs-793650758881-us-east-2/ETLPython/validation.csv'
        #estadisticas inciales         
        df=lee_archivos_en_s3(archivo) 
        #transforma la data 
        df=transforma_df(df)
        #Carga la data 
        cant,media,mini,maxi=obtener_stadisticas(tipo)
        salida=registrar_data(df,cant,media,mini,maxi,tipo)
        print(salida)
                 
    else:
        print(tipo,"no es del proceso ")
    obtener_stadisticas()
        
        
