library(DBI)
library(RPostgres)
library(dplyr)
library(ini)
library(readxl)
library(lubridate)
library(stringr)
library(readr)
source("./control_log.R")
source("./main_edocta.R")


setwd ('D:/Program Files/R/R-4.4.1/codigo/azc_nom_desaR')

# datos prueba 
ianio = 2024
imes = '01'
iquincena = '01'
itipo = 'Honorarios'
iarchivo1 = '52'
iarchivo2 = '52-azcapotzalco'

file1 <- 'C:/Users/jemar/OneDrive/Escritorio/raiz/52_desa.xlsx'
print (file1)

file2 <- 'C:/Users/jemar/OneDrive/Escritorio/raiz/52_azc_desa.xlsx'
print (file2)

print (data_to_insert)
      