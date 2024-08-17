
testit <- function(x)
{
    p1 <- proc.time()
    Sys.sleep(x)
    proc.time() - p1 # The cpu usage should be negligible
}




carga_honorarios <- function (ianio,iquincena,itipo,iarchivo,con,v_ctrl_idx)
{
   
  
   tryCatch({
   
     file_conn = abrir_log()
     escribir_log(file_conn,"Inicio Carga Honorarios....")
     
      checkini <- list()
      iniFile <- "./cfg_creacheq.ini"
      checkini <- read.ini(iniFile)
    
      # Conectar a la base de datos
      if (!dbIsValid(con)) {
      if (length(checkini) > 0) {
         escribir_log (file_conn, "Dentro de Ini, para leer parametros DB") 
		 
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
      }


	# Obtener KEY_ctr de la tabla nomina_ctrl 
    #  key_quincena_query <- 
		#sprintf ("SELECT * from nomina_ctrl where anio = %s and quincena = '%s' and nombre_nomina = '%s' and cancelado = FALSE and pendiente_dem = TRUE",ianio,iquincena,itipo)
		#	    escribir_log(file_conn,paste('Query para control : ', key_quincena_query))

	  #read_sql <- dbGetQuery(con, key_quincena_query)
	  #read_sql = as.data.frame (key_quincena_data)
	  #ictrl_idx <- dbGetQuery(con, key_quincena_query)
	  
	  #escribir_log(file_conn,paste('nrow data : ', nrow(read_sql)))

	  ictrl_idx = v_ctrl_idx
	  #if (nrow (read_sql) > 0) {
	  #    ictrl_idx = read_sql$idx[1]
 	  #} else {
	 #    stop (paste ("No hay registro en nomina control " , ianio, iquincena,itipo , sep = " "))
	  #}
	  escribir_log (file_conn,paste ("Registro de control en honorarios: " ,ictrl_idx))

      # Ruta del archivo
      #desarrollo Fidel  file_path <- "C:/Users/jemar/OneDrive/Escritorio/raiz/52-azcapotzalco_honor.xlsx"

      root_dir <- checkini$Directory$droot
      file_path <- paste0(root_dir, "nomina ", itipo, "/Respaldos/", ianio, "/", iquincena, "_", iarchivo, ".xlsx")

      escribir_log(file_conn,paste("Verificando la ruta del archivo:", file_path))

      # Verificar si el archivo existe
      if (!file.exists(file_path)) {
         stop(paste("Error: El archivo no existe en la ruta especificada:", file_path))
      }

	  Ex_hoja1 = checkini$Honorarios$Hoja1
	  Ex_hoja2 = checkini$Honorarios$Hoja2
	  SkipLine = as.integer(checkini$Honorarios$SkipLine)
	
      # Leer la hoja 'POSTNOMINA' del archivo Excel, omitiendo las primeras 9 filas
      data <- read_excel(file_path, sheet = Ex_hoja1, skip = SkipLine)
      data_2 <- read_excel(file_path, sheet = Ex_hoja2, skip = SkipLine)

      #names(data) <- make.names (tolower(names(data)))
      #names(data_2) <- make.names (tolower(names(data_2)))

      data <- as.data.frame ( data) 
      data_2 <- as.data.frame (data_2)

      "   Valores esperados en la hoja1 = POSTNOMINA , se reemplazaran los nombres debido a que tienen espacios o estan repetidos.
      U.A	
      SUBPROGRAMA	
      IDENTIFICADOR	
      NOMBRE DEL EMPLEADO	
      NOMBRE DEL PUESTO	
	  FOLIO	
	  FECHA PAGO	
	  CONCEPTO	
	  NOMBRE CONCEPTO	
	  VALOR	
	  CONCEPTO	
	  NOMBRE CONCEPTO	
	  VALOR	
	  NOMBRE BENEFICIARIO	
	  VALOR	
	  NUMERO DE CUENTA	
	  BANCO	
	  AGENCIA	
	  CUENTA DISPERSORA	
	  CUENTA EMISORA
	  "
	  colnames(data) <- c("ua","subprograma","id","nom_emp","puesto","folio","fec_pago","concepto1","nom_concepto1","valor1","concepto2","nom_concepto2","valor2","benefi","valorB","NC","banco","agencia","c_dispersora","c_emisora")

	  "  Valores esperados en la hoja2 = ANEXO I , se reemplazaran los nombres debido a que tienen espacios o estan repetidos.
	  U.A	
	  SUBPROGRAMA	
	  IDENTIFICADOR	
	  NOMBRE DEL EMPLEADO	
	  R.F.C.	
	  C.U.R.P	
	  FOLIO	
	  NOMBRE DEL PUESTO	
	  FECHA PAGO	
	  PERCEPCIONES	
	  DEDUCCIONES	
	  LIQUIDO	
	  NUMERO DE CUENTA	
	  BANCO	
	  AGENCIA	
	  CUENTA DISPERSORA	
	  CUENTA EMISORA
	  "	
	  colnames(data_2) <- c("ua2","subprograma2","id","nom_emp2","rfc","curp","folio2","puesto2","fec_pago2","percepciones","deducciones","liquido","NC2","banco2","agencia2","c_dispersora2","c_emisora2")


        data$fec_pago <- as.Date(data$fec_pago,format = "%d-%m-%Y")

      escribir_log(file_conn,paste("declarando tabla para insertar:"))

   	  t_hono <- data.frame(ctrl_idx = numeric(),
   	      identificador = character (),                 
					unidad_administrativa = character(),
                              subprograma = character(),
                              nombre_empleado = character(),
                              nombre_puesto = character(),
				              folio = numeric(),
                              fecha_pago = Date(),
                              concepto_1_id = character(),
                              valor_concepto_1 = numeric(),
				              concepto_2_id = character(),
				              valor_concepto_2 = numeric(),
				              desc_concepto1 = character(),
				              desc_concepto2 = character(),
							  percepciones = numeric(),
							  deducciones = numeric(),
							  liquido = numeric()		
					)


      data <- data[ !is.na(data$id), ]
      data_2 <- data_2[ !is.na(data_2$id), ]

      if (nrow(data) != nrow(data_2)) {
         stop ("Las filas en hoja1 y hoja2 del excel no coinciden, abortar proceso") 
      }

      escribir_log(file_conn,paste("antes del merge:"))

      df_merge = merge(data, data_2, by = "id")	
      escribir_log(file_conn,paste("despues del merge"))

      for (ii in 1:nrow(df_merge)) 
	  {  #recorremos todos los empleados
   	   t_hono <- rbind (t_hono, data.frame ( ctrl_idx = ictrl_idx,
   	                  identificador = df_merge$id[ii],
 		  			          unidad_administrativa = df_merge$ua[ii],
                              subprograma = df_merge$subprograma[ii],
                              nombre_empleado = df_merge$nom_emp[ii],
                              nombre_puesto = df_merge$puesto[ii],
				              folio = df_merge$folio[ii],
                              fecha_pago = df_merge$fec_pago[ii],
                              concepto_1_id = df_merge$concepto1[ii],
                              valor_concepto_1 = df_merge$valor1[ii],
				              concepto_2_id = df_merge$concepto2[ii],
				              valor_concepto_2 = df_merge$valor2[ii],
				              desc_concepto1 = df_merge$nom_concepto1[ii],
				              desc_concepto2 = df_merge$nom_concepto2[ii],
							  percepciones = df_merge$percepciones[ii],
							  deducciones = df_merge$deducciones[ii],
							  liquido = df_merge$liquido[ii]  
	    ))
	  }

      escribir_log(file_conn,paste("Se insertaran: ", nrow(t_hono), " registros en Honorarios",sep=" "))

      dbWriteTable(con, "honorarios", t_hono, append = TRUE, row.names = FALSE)


      # Cerrar la conexión a la base de datos
      #dbDisconnect(con)
	  cerrar_log (file_conn)
	  return ('0')

  }, error = function(e) {
      mensaje_error <- paste(Sys.time(), ": ", e$message, sep = "")
	escribir_log (file_conn,mensaje_error)	
      cerrar_log (file_conn)
      return (mensaje_error)
  })
  
}