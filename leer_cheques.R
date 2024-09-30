
carga_cheques <- function (ianio,itipo,iarchivo1,con)
{
   file_conn = abrir_log ()
   escribir_log (file_conn, "Inicio carga cheques.........")

   tryCatch (
   {
      codigoerror = 0
      flag_commit = FALSE
	  checkini = list()	
      iniFile = "./cfg_creacheq.ini"
      checkini <- read.ini(iniFile)

      result_bd = abrir_BD()    # La base de datos ya esta abierta
      con = result_bd$con_bd
      codigoerror = result_bd$codigoerror

      if (!dbIsValid (con)) {
         val_return = -1
         codigoerror = 715
         stop ("Conexion DB invalida")
      }
      # Leer el archivo .txt

      root_dir <- checkini$Directory$drootedo
      #file_path <- paste0(root_dir, "Cheques/", iarchivo1 )
	  file_path <- "../excel_disper/2a ENERO 2024 NOMINA 8_cheques.xlsx"

      # Verificar si el archivo existe
      if (!file.exists(file_path)) {
         #agregar linea en cada stop
         codigoerror = 701  
         stop(paste("Error: El archivo no existe en la ruta especificada:", file_path))
      }

	escribir_log (file_conn,paste ("Leyendo archivo: ",file_path, sep = " "))
      #data <- read_excel(file_path,col_types = c("numeric", "date","text","text","text","text","text","text"))
      #data <- read_excel(file_path, locale = locale(encoding = "LATIN1")) # al final sera un archivo excel
	
      data <- read_excel(file_path) 
	if (ncol(data) != 8) {
	  escribir_log (file_conn,paste ("Se esperaban 8 columnas pero hay ", ncol(data), "se intentara leer de todos modos", sep = " "))
	}
	vncol = ncol(data)

	vcol_types <- rep("numeric", vncol)
	vcol_types[2] <- "date"
	vcol_types[6] <- "text"   
	vcol_types[8] <- "text"

	#col_names <- c("columna1", "fecha", "valor") Cuando se quieren poner nombre a las columnas antes de leer
      #datos <- read_excel("mi_archivo.xlsx", col_names = col_names

      data <- read_excel(file_path, col_types = c(vcol_types ) ) 
	df_cheq <- data[!(is.na(data[,1]) | is.na(data[,2]))  , ]
	#df_cheq <- df_cheq[rowSums(is.na(df_cheq)) > 3, ]  #Esto sería para borrar filas con 3 o mas NA

	columnas_na <- colSums(is.na(df_cheq)) == nrow(df_cheq)

	# Eliminar las columnas que tienen NA en todas las lineas
	df_cheq <- df_cheq[, !columnas_na]

	escribir_log (file_conn,paste ("Se han leido " , nrow(data) , "del archivo excel" , sep = " "))

      colnames(df_cheq) <- c("idx","fec_cheque","id_empleado","num_cheque","num_poliza","nombre","monto","quincena")

	df_cheq <- df_cheq %>%
  		mutate(fec_cheque = as.Date(fec_cheque, format = "%Y-%m-%d"))

	if (any(is.na(df_cheq$fec_cheque)) | any(is.na(df_cheq$idx))) {   # Si despues de hacer todas las conversiones hay un valor NA en fecha o en idx, descartamos la data
	   codigoerror = 726
	   stop ("Los valores del archivo para cheques son incorrectas") 
	}
	

	df_cheq$id_empleado[is.na(df_cheq$id_empleado)] <- 0
	df_cheq$num_poliza[is.na(df_cheq$num_poliza)] <- 0

      df_cheq <- df_cheq %>% select (-idx,-nombre,-quincena) # Registros que no se encuentran en la tabla edocta

	vfec_min = min(df_cheq$fec_cheque)
	vfec_max = max(df_cheq$fec_cheque)

	sqry = "select fec_cheque, num_cheque from t_cheques where fec_cheque between '%s' and '%s'"
	sqry = sprintf (sqry,vfec_min,vfec_max)

	df_hist = dbGetQuery (con,sqry) 

	escribir_log (file_conn,paste ("Despues de limpiar excel" , nrow(df_cheq) , " en historico se encontraron" , nrow(df_hist), sep = " "))
	

	if (nrow(df_hist) > 0) {
	   df_cheq = anti_join(df_cheq, df_hist, by = c("fec_cheque","num_cheque"))
	}

	escribir_log (file_conn,paste ("Se van a guardar" , nrow(df_cheq) , "en cheques" , sep = " "))

	if (nrow(df_cheq) > 0) {
	   dbWriteTable(con, "t_cheques", df_cheq, append = TRUE, row.names = FALSE)
	}

      escribir_log (file_conn,paste ("Proceso carga cheques finalizado...", sep = " "))
      cerrar_log (file_conn) 

      if (dbIsValid (con)) {   #dejamos conexion abierta para ligar CLCs
         dbDisconnect (con)
      }
      return (codigoerror)
 
   }, error = function(e) {
            # Formatear el mensaje de error con la fecha y hora
            mensaje_error <- paste(Sys.time(), ": ", e$message, sep = "")
   
            escribir_log (file_conn, mensaje_error)
            cerrar_log(file_conn)
            
            if (dbIsValid (con)) {   #Cerramos base de datos
               if (flag_commit == TRUE) {
                  dbRollback (con)  # en caso de error deshacemos la transaccion 
               }
               # dejamos conexion abierta dbDisconnect (con)
            }

            if (codigoerror == 0) {
               codigoerror = 700
            }     
            return (codigoerror) 
         }
   )
}

# Cargar librerías necesarias
library(readxl)
library(dplyr)

# Función para leer archivos Excel desde el inicio de la tabla y filtrar filas no deseadas
leer_tabla_excel <- function(file_path, sheet = 1, start_row = 5) {
  # Leer los datos desde la fila de inicio
  datos <- read_excel(file_path, sheet = sheet, skip = start_row - 1)
  
  # Renombrar las columnas para evitar problemas con espacios o caracteres especiales
  datos <- datos %>%
    rename_all(~make.names(., unique = TRUE))
  
  # Filtrar filas: eliminar aquellas que tengan valores NA o que contengan 'J.U.D.' o 'JEFE' en el campo 'NOMBRE.DEL.EMPLEADO'
  datos <- datos %>% filter(!is.na(NOMBRE.DEL.EMPLEADO) & !grepl("J.U.D.|JEFE", NOMBRE.DEL.EMPLEADO, ignore.case = TRUE))
  
  return(datos)
}

# Uso de la función
file_path <- "C:/Users/PJMX/Desktop/Desgloce nómina 2024 enero a julio 2024/1.-ENERO 2024/CHEQUES/1a ENERO 2024 NOMINA 8.xlsx"
datos_tabla <- leer_tabla_excel(file_path)


# Mostrar los datos
View(datos_tabla)