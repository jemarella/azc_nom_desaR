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
packages <- c("bit", "bit64", "DBI", "RPostgres", "dplyr", "lubridate", "readxl", "blob", "withr")

# Instalar y cargar paquetes
sapply(packages, install_and_load)

# Crear/abrir archivo de log
file_conn <- file("C:/Users/USER/Music/R/mi_log.txt", open='a')

# Función para registrar mensajes en el log
log_message <- function(message) {
  writeLines(paste(Sys.time(), message, sep = " "), file_conn)
}

log_message("Inicia creación de cheques...")

# Obtener parámetros desde la línea de comandos
args <- commandArgs(trailingOnly = TRUE)
quincena <- args[1]
anio <- args[2]
tipo <- args[3]
ini_cheq <- as.integer(args[4])

log_message(paste("Parámetros leídos:", anio, quincena, tipo, ini_cheq))

tryCatch(
  {
#Leer valores archivo ini o properties
	checkini = list()	
      iniFile = "./cfg_creacheq.ini"
      checkini <- read.ini(iniFile)


	#Conectar a la base de datos
	      if (length(checkini) > 0) {
         con <- dbConnect(RPostgres::Postgres(),
                       dbname = checkini$Database$dbname,
                       host = checkini$Database$host,
                       port = checkini$Database$port,
                       user = checkini$Database$user,
                       password = checkini$Database$password)
      }	else {
         con <- dbConnect(RPostgres::Postgres(),
                      dbname = "SistemaNomina",
                      host = "192.168.100.215",
                      port = 5432,
                      user = "postgres",
                      password = "Pjmx3840")
      }

    # Consulta para obtener el IDX de la quincena
    linea1 <- paste("select idx from nomina_ctrl as aa",
                    "where aa.anio = %s and aa.quincena = '%s' and aa.nombre_nomina = '%s'")
    str_query <- sprintf(linea1, anio, quincena, tipo)
    log_message(paste("Query para búsqueda de quincena:", str_query))

    resultado <- dbGetQuery(con, str_query)

    if (nrow(resultado) > 0) {
      idx_quincena <- as.integer(resultado[1,1])
      log_message(paste("IDX control de quincena =", idx_quincena))
    } else {
      stop("No se encontró quincena con los argumentos de entrada.")
    }

    # Consulta para contar el número de cheques
    linea1 <- paste("select count(aa.idx) from nomina_ctrl as aa",
                    "join empleados_totales as bb on bb.ctrl_idx = aa.idx",
                    "join empleados_nomina as cc on cc.id_empleado = bb.id_empleado",
                    "where aa.idx = %s and cc.id_banco is NULL")
    str_query <- sprintf(linea1, idx_quincena)
    log_message(paste("Query para búsqueda del total de cheques:", str_query))

    resultado <- dbGetQuery(con, str_query)

    if (nrow(resultado) > 0) {
      num_cheques <- as.integer(resultado[1,1])
      log_message(paste("Número de cheques a crear =", num_cheques))
    } else {
      stop("No se encontró el total de cheques a crear.")
    }

    # Verificar si los cheques ya existen en el rango dado
    linea1 <- paste("select num_cheque from t_cheques as aa",
                    "where aa.num_cheque >= %s and aa.num_cheque <= %s")
    str_query <- sprintf(linea1, ini_cheq, ini_cheq + num_cheques)
    log_message(paste("Query para búsqueda de cheques pre-creados:", str_query))

    resultado <- dbGetQuery(con, str_query)

    if (nrow(resultado) > 0) {
      stop(paste("Error. Cheques ya existen en ese rango =", ini_cheq, "<>", ini_cheq + num_cheques))
    }

    # Consulta para obtener la lista de empleados
    linea1 <- paste("select aa.idx, bb.id_empleado , bb.liquido from nomina_ctrl as aa",
                    "join empleados_totales as bb on bb.ctrl_idx = aa.idx",
                    "join empleados_nomina as cc on cc.id_empleado = bb.id_empleado",
                    "where aa.idx = %s and cc.id_banco is NULL order by bb.id_empleado")
    str_query <- sprintf(linea1, idx_quincena)
    log_message(paste("Query para búsqueda de tabla empleados:", str_query))

    resultado <- dbGetQuery(con, str_query)

    if (nrow(resultado) > 0) {
      log_message(paste("Lista de tabla empleados, registros =", nrow(resultado)))
    } else {
      stop("No se encontraron registros de la tabla empleados.")
    }

    # Crear registros de cheques
    for (ii in 1:5) {  # Puedes cambiar 5 por nrow(resultado) para recorrer todos los empleados
      tabla_cheques <- rbind(tabla_cheques, data.frame(
        num_cheque = ini_cheq,
        ctrl_idx = idx_quincena,
        id_empleado = resultado[ii,2],
        num_poliza = ini_cheq,
        id_honorarios = 0,
        monto = resultado[ii,3],
        estado = 'creado'
      ))
      ini_cheq <- ini_cheq + 1	
    }

    log_message(paste("Listo el dataframe con los registros:", nrow(tabla_cheques)))

    # Guardar los cheques en la base de datos
    dbWriteTable(con, "t_cheques", tabla_cheques, append = TRUE, row.names = FALSE)
    dbDisconnect(con)
    close(file_conn)
    
  }, error = function(e) {
    # Manejo de errores: escribir el mensaje en el log y cerrar la conexión
    log_message(paste("Error:", e$message))
    dbDisconnect(con)
    close(file_conn)
    stop(e)
  }
)
