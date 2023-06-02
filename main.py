import pandas as pd
import tkinter as tk
import matplotlib.pyplot as plt
from pipepline_ETL import ejecutar_pipeline
  

##Proceso de ETL
##posibles entrdadas :
### "Validacion" si se corre el ultimo archivo
### "Normal" recorre el directorio buscando archivos que cargar inciando desde la ultima carga"
ejecutar_pipeline("Normal")
    

        
        
       