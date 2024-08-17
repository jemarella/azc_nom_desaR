
library(DBI)
library(RPostgres)
library(dplyr)
library(ini)
library(readxl)
library(lubridate)

setwd ('D:/Program Files/R/R-4.4.1/codigo/azc_nom_desaR')

# datos prueba 
anio = 2024
quincena = '07'
tipo = 'Compuesta'
archivo1 = '52'
archivo2 = '52-azcapotzalco'

file1 <- 'C:/Users/jemar/OneDrive/Escritorio/raiz/52_desa.xlsx'
print (file1)

file2 <- 'C:/Users/jemar/OneDrive/Escritorio/raiz/52_azc_desa.xlsx'
print (file2)

#print (data_to_insert)

      checkini <- list()
      iniFile <- "./cfg_creacheq.ini"
      checkini <- read.ini(iniFile)
    


# script_principal.R
source("./control_log.R") 
  


dbDisconnect (con_bd)
   		  cerrar_log(file_conn)


con_bd = abrir_BD()
ctrl_idx = -1

print (con_bd)

if (dbIsValid(con_bd)){
   ctrl_idx = crear_nomina_idx (anio,quincena,tipo,con_bd)
update_nomina_idx_ok (con_bd,ctrl_idx)

dbDisconnect (con_bd)
}
