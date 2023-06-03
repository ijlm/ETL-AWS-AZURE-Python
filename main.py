from pipepline_ETL import ejecutar_pipeline
import sys
  

##Proceso de ETL
##posibles entrdadas :
### "Validacion" si se corre el ultimo archivo
### "Normal" recorre el directorio buscando archivos que cargar inciando desde la ultima carga"

ejecutar_pipeline(sys.argv[1])
    

       