# Ruta a la biblioteca personalizada
lib_path <- "C:/Users/USUARIO.138-1596/AppData/Local/Temp/Rtmp6J3eRS/downloaded_packages"


#Faltan estos paquetes
#library(stringr)
#library(readr)


# Configurar un espejo de CRAN
options(repos = c(CRAN = "https://cran.rstudio.com/"))

# Función para instalar y cargar paquetes
install_and_load <- function(package) {
  if (!require(package, character.only = TRUE, lib.loc = lib_path)) {
    install.packages(package, lib = lib_path)
    library(package, character.only = TRUE, lib.loc = lib_path)
  }
}

# Lista de paquetes necesarios
packages <- c("bit", "bit64", "DBI", "RPostgres", "dplyr", "lubridate", "readxl", "blob", "withr", "ini","later","stringr","readr")

# Instalar y cargar paquetes
sapply(packages, install_and_load)

args <- commandArgs(trailingOnly = TRUE)
quincena <- args[1]
anio <- args[2]
vuser <- args[3]  # El usuario que carga el archivo
varchivo1 <- args[4] # El nombre del archivo a cargar
tipo_carga <- args[5]  # El tipo de carga puede ser "dispersion" o "edocta" 

setwd("C:/Users/USUARIO.138-1596/Documents/R/Script")   ### proveer el directorio raiz para archivo log y archivo ini

# script_principal.R
source("./control_log.R")
source("./relacion_clc.R")
source("./carga_cta_disper.R") 
source("./leer_ecta_largo.R") 
source("./leer_ecta_corto.R") 


result_bd = abrir_BD() 
con_bd = result_bd$con_bd
codigoerror = result_bd$codigoerror

if (codigoerror == 715) { # !dbIsValid (con)) {
   val_return = -1
   #stop ("Conexion DB invalida")
} else {
    result_carga = -1

    if (tipo_carga == 'EstadosCuenta') {
       result_carga = carga_edocta_corto (anio,tipo_carga,varchivo1,con_bd)
       if (result_carga == 0) {
         result_carga = carga_relacion_edocta (anio,con_bd)
       }
    } else if (tipo_carga == 'Dispersion') {
       result_carga = carga_dispersion (anio,quincena,tipo_carga,varchivo1,con_bd)
    }
    
    result_edo = crear_idx_edocta (anio,quincena,tipo_carga,vuser,varchivo1,con_bd,result_carga) 

    #En este punto codigoerror contiene el valor de error si todo fue bien sera igual a 0
    #print (codigoerror)
    if (dbIsValid (con_bd)){   #Cerramos conexion a BD que se utilizo a traves de todos los modulos
       dbDisconnect(con_bd)
    }   

}




