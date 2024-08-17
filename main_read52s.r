# Ruta a la biblioteca personalizada
lib_path <- "C:/Users/USUARIO.138-1596/AppData/Local/Temp/Rtmp6J3eRS/downloaded_packages"


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
packages <- c("bit", "bit64", "DBI", "RPostgres", "dplyr", "lubridate", "readxl", "blob", "withr", "ini","later")

# Instalar y cargar paquetes
sapply(packages, install_and_load)


args <- commandArgs(trailingOnly = TRUE)
quincena <- args[1]
anio <- args[2]
tipo <- args[3]
archivo1 <- args[4]
archivo2 <- args[5]

setwd("C:/Users/USUARIO.138-1596/Documents/R/Script")   ### proveer el directorio raiz para archivo log y archivo ini

# script_principal.R
source("./control_log.R") 
source("./add_update.R") 
source("./add_update_nomina.R")
source("./empleados_totales.R") 
source("./carga_detalle_dp.R")
source("./honorarios.R")

#source("./codigo/asigna_cheques.R") 
#crea_cheques (2024,'05','Compuesta',10000)

#conn_bd = abrir_BD
#if conn_bd != NA {

con_bd = abrir_BD()
ctrl_idx = -1

if (dbIsValid (con_bd)){
   ctrl_idx = crear_nomina_idx (anio,quincena,tipo,con_bd)
}

result_carga = '-1'

if (ctrl_idx != -1 ) {
   if (tipo == "Honorarios") {
      result = carga_honorarios	(con_bd,anio,quincena,tipo,archivo2)
   } else {
      result = tabla_empleados (con_bd,anio,quincena,tipo,archivo1,con_bd)
      if (result == '0') {
         result = tabla_empleados_nomina(anio,quincena,tipo,archivo1,con_bd) 
      }
      if (result == '0') {
         result = carga_resumen_nom (anio,quincena,tipo,archivo1,archivo2,con_bd,ctrl_idx)
      }
      if (result != '0') {
         result = carga_detalles_nom (anio,quincena,tipo,archivo2,con_bd,ctrl_idx)
      } 
   }
   if (result == '0') {
      update_nomina_idx_ok (con_bd,ctrl_idx)    
   }

} 

