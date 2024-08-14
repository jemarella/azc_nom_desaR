

carga_detalles_nom <- function (ianio,iquincena,itipo,iarchivo2)
{
   file_conn = abrir_log()
   escribir_log(file_conn,"Inicio Carga Percepciones y Deducciones....")
   
   tryCatch({
    
   
      checkini <- list()
      iniFile <- "./cfg_creacheq.ini"
      checkini <- read.ini(iniFile)
    
      # Conectar a la base de datos
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
   
      ## ambiente desarrollo fidel root_dir <- "C:/Users/PJMX/Desktop/Raiz/"
      ## ambiente desarrollo fidel file_path <- paste0(root_dir, "Compuesta/202405_52-azcapotzalco.xlsx")
      # Imprimir la ruta del archivo para verificar
   
      root_dir <- checkini$Directory$droot
      file_path <- paste0(root_dir, "nomina ", itipo, "/Respaldos/", ianio, "/", iquincena, "_", iarchivo2, ".xlsx")

      escribir_log(file_conn,paste("Verificando la ruta del archivo:", file_path))

      # Verificar si el archivo existe
      if (!file.exists(file_path)) {
         stop(paste("Error: El archivo no existe en la ruta especificada:", file_path))
      }
      data <- read_excel(file_path)
  
      # Convertir nombres de columnas a minúsculas
      colnames(data) <- tolower(colnames(data))

      # Verificar los primeros registros del archivo Excel
      escribir_log(file_conn,"Datos del archivo Excel:")
      escribir_log(file_conn,head(data))

      # Convertir ID_EMPLEADO a integer en el archivo Excel
      data$id_empleado <- as.integer(data$id_empleado)

      # Convertir las columnas de fechas a formato Date y luego a character en formato ISO
      data$fec_inicio_p <- as.character(as.Date(data$fec_inicio_p, format = "%d/%m/%Y"))
      data$fec_fin_p <- as.character(as.Date(data$fec_fin_p, format = "%d/%m/%Y"))
      data$fec_imputacion <- as.character(as.Date(data$fec_imputacion, format = "%d/%m/%Y"))

	  # Obtener KEY_ctr de la tabla nomina_ctrl 
      key_quincena_query <- 
		sprintf (checkini$Queries$ctrl_nom_pendiente,ianio,iquincena,itipo)
      key_quincena_data <- dbGetQuery(con, key_quincena_query)
	 
	  ictrl_idx = 0
	  if (nrow (key_quincena_data) > 0) {
	     ictrl_idx = key_quincena_data$idx[1]
 	  } else {
	     stop ("No hay registro en nomina control")
	  }
	 
      # Obtener KEY_QUINCENA de la tabla EMPLEADOS_TOTALES
      key_quincena_query <- 
	     sprintf ('SELECT id_empleado, key_quincena FROM empleados_totales where ctrl_idx = %s ',ictrl_idx) 
      key_quincena_data <- dbGetQuery(con, key_quincena_query)

	  if (nrow (key_quincena_data) == 0) {
	     stop ("No hay registros de donde obtener la Key de nomina")
	  }
	  
      # Verificar los datos obtenidos de EMPLEADOS_TOTALES
      escribir_log(file_conn,"Datos de la tabla EMPLEADOS_TOTALES:")
      escribir_log(file_conn,head(key_quincena_data))

      # Obtener los id_concepto válidos de la tabla CAT_CONCEPTOS
      valid_concepts_query <- 'SELECT id_concepto FROM public.cat_conceptos;'
      valid_concepts <- dbGetQuery(con, valid_concepts_query)$id_concepto

      # Unir los datos del archivo Excel con los de la tabla EMPLEADOS_TOTALES
      merged_data <- data %>%
         left_join(key_quincena_data, by = "id_empleado")

      # Filtrar filas que tienen valores nulos en columnas permitiendo nulos
      filtered_data <- merged_data %>%
        filter(!is.na(id_concepto) | is.na(id_concepto)) %>%
        filter(!is.na(id_tipo_prestamo) | is.na(id_tipo_prestamo)) %>%
        filter(!is.na(id_subtipo_prestamo) | is.na(id_subtipo_prestamo)) %>%
        filter(!is.na(valor) | is.na(valor))

      # Verificar si hay id_concepto no válidos, excluyendo NA
      invalid_concepts <- filtered_data %>%
      filter(!is.na(id_concepto) & !id_concepto %in% valid_concepts) %>%
      select(id_concepto) %>%
      distinct()

      if (nrow(invalid_concepts) > 0) {
         escribir_log(file_conn,"Alerta: Los siguientes id_concepto no están registrados en la base de datos:")
         escribir_log(file_conn,invalid_concepts)
      # Aquí puedes agregar código para mostrar una alerta al usuario en el front-end.
      } else {
         # Filtrar y seleccionar las columnas necesarias para percepciones_empleado
         percepciones_empleado <- filtered_data %>%
         select(
            key_quincena, 
            id_empleado,
            id_concepto,
            no_linea,
            valor,
            fec_inicio_p,
            fec_fin_p, 
            fec_imputacion
            )
  
         # Verificar los datos finales que se van a insertar en percepciones_empleado
         escribir_log(file_conn,"Datos finales a insertar en percepciones_empleado:")
         escribir_log(file_conn,head(percepciones_empleado))
         ## ambiente desarrollo fidel summary(percepciones_empleado)
  
         # Insertar los datos en la tabla PERCEPCIONES_EMPLEADO permitiendo duplicados
         dbWriteTable(con, "percepciones_empleado", percepciones_empleado, append = TRUE, row.names = FALSE)
      }

      # Filtrar y seleccionar las columnas necesarias para deducciones_empleado
      deducciones_empleado <- merged_data %>%
      select(
          key_quincena,
          id_empleado,
          no_linea,
          id_concepto1,
          id_tipo_prestamo,
          id_subtipo_prestamo,
          valor1,
          fec_inicio_p,
          fec_fin_p,
          fec_imputacion
      )

      # Verificar si hay id_concepto1 no válidos, excluyendo NA
      invalid_concepts <- deducciones_empleado %>%
      filter(!is.na(id_concepto1) & !id_concepto1 %in% valid_concepts) %>%
      select(id_concepto1) %>%
      distinct()

      if (nrow(invalid_concepts) > 0) {
         escribir_log(file_conn,"Alerta: Los siguientes id_concepto1 no están registrados en la base de datos:")
         escribir_log(file_conn,invalid_concepts)
      # Aquí puedes agregar código para mostrar una alerta al usuario en el front-end.
      } else {
         # Verificar los datos finales que se van a insertar en deducciones_empleado
         escribir_log(file_conn,"Datos finales a insertar en deducciones_empleado:")
         escribir_log(file_conn,head(deducciones_empleado))
         ## ambiente desarrollo fidel summary(deducciones_empleado)
  
         # Insertar los datos en la tabla DEDUCCIONES_EMPLEADO permitiendo duplicados y nulos
         dbWriteTable(con, "deducciones_empleado", deducciones_empleado, append = TRUE, row.names = FALSE, overwrite = FALSE)
      }

      # Cerrar la conexión a la base de datos
      dbDisconnect(con)
	  cerrar_log (file_conn) 

  }, error = function(e) {
    mensaje_error <- paste(Sys.time(), ": ", e$message, sep = "")
	escribir_log (file_conn,mensaje_error)	
    cerrar_log (file_conn)
  })
  
}
