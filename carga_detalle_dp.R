carga_detalles_nom <- function(ianio, iquincena, itipo, iarchivo2, con, v_ctrl_idx) {
  file_conn <- abrir_log()
  escribir_log(file_conn, "Inicio Carga Percepciones y Deducciones....")
  
  tryCatch({
  
		codigoerror <- 0
		
	  checkini <- list()
      iniFile <- "./cfg_creacheq.ini"
      checkini <- read.ini(iniFile)
    
    # Conectar a la base de datos si no es válida
    if (!dbIsValid(con)) {
      if (length(checkini) > 0) {
        con <- dbConnect(
          RPostgres::Postgres(),
          dbname = checkini$Database$dbname,
          host = checkini$Database$host,
          port = checkini$Database$port,
          user = checkini$Database$user,
          password = checkini$Database$password)
      } else {
        con <- dbConnect(
          RPostgres::Postgres(),
          dbname = "SistemaNomina",
          host = "192.168.100.215",
          port = 5432,
          user = "postgres",
          password = "Pjmx3840")
      }
    }
     ## ambiente desarrollo fidel root_dir <- "C:/Users/PJMX/Desktop/Raiz/"
      ## ambiente desarrollo fidel file_path <- paste0(root_dir, "Compuesta/202405_52-azcapotzalco.xlsx")
      # Imprimir la ruta del archivo para verificar
	  
    # Construir ruta del archivo
    root_dir <- checkini$Directory$droot
    file_path <- paste0(root_dir, "nomina ", itipo, "/Respaldos/", ianio, "/", iquincena, "_", iarchivo2, ".xlsx")
    
    escribir_log(file_conn,paste("Verificando la ruta del archivo:", file_path))
    
    # Verificar existencia del archivo
    if (!file.exists(file_path)) {
      codigoerror = 701
      stop(paste("Error: El archivo no existe en la ruta especificada:", file_path))
    }
    
    data <- read_excel(file_path)
    
    # Convertir nombres de columnas a minúsculas
    colnames(data) <- tolower(colnames(data))
    
    # Verificar los primeros registros del archivo Excel
    escribir_log(file_conn,"Datos del archivo Excel:")
    escribir_log(file_conn,head(data))
    
    # Convertir ID_EMPLEADO a integer
    data$id_empleado <- as.integer(data$id_empleado)
    
    # Convertir fechas a formato Date y luego a character en formato ISO
    data$fec_inicio_p <- as.character(as.Date(data$fec_inicio_p, format = "%d/%m/%Y"))
    data$fec_fin_p <- as.character(as.Date(data$fec_fin_p, format = "%d/%m/%Y"))
    data$fec_imputacion <- as.character(as.Date(data$fec_imputacion, format = "%d/%m/%Y"))
    
    # Obtener KEY_QUINCENA de la tabla EMPLEADOS_TOTALES
	  #key_quincena_query <- 
		#sprintf (checkini$Queries$ctrl_nom_pendiente,ianio,iquincena,itipo)
      #key_quincena_data <- dbGetQuery(con, key_quincena_query)
	 
	  #ictrl_idx = 0
	  #if (nrow (key_quincena_data) > 0) {
	  #   ictrl_idx = key_quincena_data$idx[1]
 	  #} else {
	  #   stop ("No hay registro en nomina control")
	  #}
    ictrl_idx = v_ctrl_idx
    escribir_log (file_conn,paste("Valor idx ",ictrl_idx))
          # Obtener KEY_QUINCENA de la tabla EMPLEADOS_TOTALES
    key_quincena_query <- sprintf('SELECT id_empleado, key_quincena FROM empleados_totales WHERE ctrl_idx = %s',ictrl_idx)
    key_quincena_data <- dbGetQuery(con, key_quincena_query)
    
    if (nrow(key_quincena_data) == 0) {
      codigoerror = 707
      stop("No hay registros de donde obtener la Key de nomina")
    }
          # Verificar los datos obtenidos de EMPLEADOS_TOTALES
    escribir_log(file_conn, "Datos de la tabla EMPLEADOS_TOTALES:")
    escribir_log(file_conn,head(key_quincena_data))
    
    # Obtener los id_concepto válidos de la tabla CAT_CONCEPTOS
    valid_concepts_query <- 'SELECT id_concepto FROM public.cat_conceptos;'
    valid_concepts <- dbGetQuery(con, valid_concepts_query)$id_concepto
    
    # Obtener conceptos desde el archivo Excel
    percepciones_conceptos <- data %>%
      filter(!is.na(id_concepto)) %>%
      select(id_concepto, nombre_concepto) %>%
      distinct() %>%
      filter(!is.na(nombre_concepto) & nombre_concepto != "")
    
    deducciones_conceptos <- data %>%
      filter(!is.na(id_concepto1)) %>%
      select(id_concepto1, nombre_concepto1) %>%
      distinct() %>%
      filter(!is.na(nombre_concepto1) & nombre_concepto1 != "")
    
    # Unir y filtrar conceptos nuevos
    nuevos_conceptos <- bind_rows(
      percepciones_conceptos %>% rename(id_concepto = id_concepto, nombre_concepto = nombre_concepto),
      deducciones_conceptos %>% rename(id_concepto = id_concepto1, nombre_concepto = nombre_concepto1)
    ) %>%
    filter(!id_concepto %in% valid_concepts) %>%
    distinct()
    
    # Insertar nuevos conceptos en la tabla cat_conceptos
    if (nrow(nuevos_conceptos) > 0) {
      escribir_log(file_conn, "Insertando nuevos conceptos en la tabla cat_conceptos:")
      escribir_log(file_conn, nuevos_conceptos)
      
      for (i in 1:nrow(nuevos_conceptos)) {
        insert_query <- sprintf(
          "INSERT INTO public.cat_conceptos (id_concepto, nombre_concepto) VALUES (%s, '%s') ON CONFLICT (id_concepto) DO NOTHING;",
          nuevos_conceptos$id_concepto[i], nuevos_conceptos$nombre_concepto[i]
        )
        dbExecute(con, insert_query)
      }
    } else {
      escribir_log(file_conn, "No hay conceptos nuevos para insertar.")
    }
    
    # Unir datos del archivo Excel con EMPLEADOS_TOTALES
    merged_data <- data %>%
      left_join(key_quincena_data, by = "id_empleado")
    
    # Procesar Percepciones
    percepciones_empleado <- merged_data %>%
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
    
    escribir_log(file_conn, "Datos finales a insertar en percepciones_empleado:")
    escribir_log(file_conn, head(percepciones_empleado))
    dbWriteTable(con, "percepciones_empleado", percepciones_empleado, append = TRUE, row.names = FALSE)
    
    # Procesar Deducciones
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
    
    escribir_log(file_conn, "Datos finales a insertar en deducciones_empleado:")
    escribir_log(file_conn, head(deducciones_empleado))
    dbWriteTable(con, "deducciones_empleado", deducciones_empleado, append = TRUE, row.names = FALSE)
    
    # Agregar fragmento adicional para nóminas extraordinarias
    if (itipo == "Extraordinarios") {
      v_nom_concepts_query <- sprintf('SELECT nombre_concepto FROM public.cat_conceptos WHERE id_concepto = %s',percepciones_empleado$id_concepto[1])
      v_nom_concepts <- dbGetQuery(con, v_nom_concepts_query)$nombre_concepto[1]
      
      # Tomar nombre de concepto para actualizar cuando son nóminas extraordinarias
      sql_descrip <- checkini$Queries$update_desc_extaor
      sql_descrip <- sprintf(sql_descrip, v_nom_concepts,v_ctrl_idx)
      
      escribir_log(file_conn, paste("Actualizar descripción extraordinaria de acuerdo a conceptos:", sql_descrip))
      dbExecute(con, sql_descrip)
    }
    # Cerrar la conexión a la base de datos
      #dbDisconnect(con)
	  cerrar_log (file_conn) 
    
      #return ('0')
      return (codigoerror) #agregado 20-08-2024
	
  },error = function(e) {
    mensaje_error <- paste(Sys.time(), ": ", e$message, sep = "")
	escribir_log (file_conn,mensaje_error)	
    cerrar_log (file_conn)
    #return (mensaje_error)
    if (codigoerror == 0) {
                codigoerror = 700
             }
    return (codigoerror)#agregado 20-08-2024
  })
}
