#library(readr)
#library(DBI)
#library(RPostgres)
#library(dplyr)
#library(ini)
#library(readxl)
#library(lubridate)
#library(stringr)

#setwd ('D:/Program Files/R/R-4.4.1/codigo/azc_nom_desaR')
#source("./control_log.R")

carga_edocta_corto <- function (ianio,itipo,iarchivo1,con)
{
   file_conn = abrir_log ()
   escribir_log (file_conn, "Inicio carga estado cuenta corto.........")

   tryCatch (
   {
      codigoerror = 0
      flag_commit = FALSE
	   checkini = list()	
      iniFile = "./cfg_creacheq.ini"
      checkini <- read.ini(iniFile)

      #result_bd = abrir_BD()    # La base de datos ya esta abierta
      #con = result_bd$con_bd
      #codigoerror = result_bd$codigoerror

      if (!dbIsValid (con)) {
     	   escribir_log (file_conn,"Conexion BD invalida")
	      val_return = -1
         codigoerror = 715
         stop ("Conexion DB invalida")
      }
      # Leer el archivo .txt

      root_dir <- checkini$Directory$drootedo
      file_path <- paste0(root_dir, "EstadosCuenta/", iarchivo1 )
	   #file_path <- "./movimientos_01720122252006_2024-09-01_2024-09-10.xlsx"

      # Verificar si el archivo existe
      if (!file.exists(file_path)) {
         #agregar linea en cada stop
         codigoerror = 701  
         stop(paste("Error: El archivo no existe en la ruta especificada:", file_path))
      }

      columnas_esperadas <- c("NUMERO DE CUENTA", "FECHA DE OPERACION", "FECHA DE APLICACION",'CONCEPTO','IMPORTE','SALDO','MOVIMIENTO')

      data <- read_excel(file_path)
      #data <- read_lines(file_path, locale = locale(encoding = "LATIN1")) # al final sera un archivo excel

      if (ncol(data) != 7) {
         codigoerror = 725  
         stop(paste("Las columnas del archivo de edocta son incorrectas, se esperan 7 columnas"))
      }

      if (!setequal(names(data), columnas_esperadas)) {
         codigoerror = 726
         stop(paste("Las columnas del archivo de edocta no coinciden con ", paste(columnas_esperadas, collapse = ',')))    
      }

	   colnames(data) <- c('cuenta','fechaop','fechaval','concepto','importe','saldo','movto')

      anchos <- c(11,11,13,8,39,23,23,23)

      df_inserta <- data.frame(fecha_op = character(),
                             fecha_val = character(),
                             movto = numeric(),
                             codigo = character(),
                             concepto1 = character(),
                             cargo = numeric(),
                             abono = numeric(),
                             saldo = numeric(),
                             cheque = numeric(),
                             cuenta_pago = character(),
                             nombre_benef = character ())

      #parsed_data <- slice(parsed_data, 0)	

      flag_212_213 = FALSE
      scuenta_pago = ''
      snombre_benef = ''
      #for (linea in data) {
	   resultado = data

	   for (irow in 1:nrow(resultado)) {

	      #if (colnames(data[ ,irow <= 0 ) { # Saltamos inea vacia 
	      #   next
	      #}

         #resultado <- dividir_linea(linea, anchos)

	      vconcepto = resultado$concepto[irow]

         if (!is.na(vconcepto) & flag_212_213 == TRUE) {    # checamos que el concepto no sea nulo, con el codigo 212 existen al menos 6 filas de información extra
            concepto_extra = vconcepto
		      escribir_log (file_conn, paste ('Revisando fila del flag 212 ', concepto_extra))
            if (!is.na(str_locate(concepto_extra,'CUENTA:')[1])) {   #Verificamos que en la descripción venga un número de cuenta el [1] es para encontrar la primera posicion
               scuenta_pago <- substring(concepto_extra,str_locate(concepto_extra,"CUENTA:")[1]+8)
		         escribir_log (file_conn, paste ('Encontre CUENTA: ', scuenta_pago))
            }  
            if (!is.na(str_locate(concepto_extra,'NOM BENEF:')[1])) {   #Para el 212 guardamos nombre del Beneficiario
               snombre_benef <- substring(concepto_extra,str_locate(concepto_extra,"NOM BENEF:")[1]+11)
            }
            if (codigo == '213' & !is.na(str_locate(concepto_extra,'CONCEPTO:')[1]) ) {   #Para el 213, guardamos concepto , es un SPEI hacia el beneficiario
               snombre_benef <- substring(concepto_extra,str_locate(concepto_extra,"CONCEPTO:")[1]+10)
            }
         } 

	      vfecha = trimws(resultado$fechaop[irow])
         if (is.na(vfecha)) {  # si el campo de fecha esta vacia no es necesario descomponer toda la linea
	         next
	      }

	      fecha <- as.Date(vfecha, format = "%Y-%m-%d")

	      if (!is.na(fecha)) # si no es una fecha valida continuamos al siguiente registro
         {

            if (flag_212_213 == TRUE) {
               flag_212_213 <- FALSE  # apagamos bandera  del movto 212 debido a que encontramos una fecha al inicio de la fila
               df_inserta <- rbind(df_inserta, data.frame(
                  fecha_op = fecha_op,
                  fecha_val = fecha_val,
                  movto = movto,
                  codigo = codigo,
                  concepto1 = concepto,
                  cargo = cargo,
                  abono = abono,
                  saldo = saldo,
                  cheque = chequei,
                  cuenta_pago = scuenta_pago,
                  nombre_benef = snombre_benef ))
		            scuenta_pago <- ''
                  snombre_benef <- ''
            }
            movto <- 0 
            cargo <- 0
            abono <- 0
            fecha_busca = fecha #salvamos la fecha para luego hacer una busqueda por rango

            movto <- as.integer (resultado$movto[irow])  #
		
		      if (resultado$importe[irow] < 0) {
               cargo <- gsub(",","",resultado$importe[irow] * -1)
            } else {
               abono <- gsub(",","",resultado$importe[irow])
            }

            fecha_op <- fecha
            fecha_val <- as.Date (resultado$fechaval[irow],format = '%Y-%m-%d') 
            #codigo <- resultado$codigo[irow] # no hay codigo en esta version de archivo corto
		      codigo = ''
            concepto <- vconcepto
            saldo <- gsub(",","",resultado$saldo[irow]) 

            cheque <- ''
            chequei <- 0 
               
            if (!is.na(str_locate(concepto,':')[1])) {   #Verificamos que en la descripción venga un número de cheque
                  cheque <- substring(concepto,str_locate(concepto,":")[1]+1)
               if (!is.na(cheque)) { 
                        chequei <- as.integer(cheque)

                  # DEBIDO A QUE EN ESTA LETURA DE EDO CORTO NO CONTAMOS CON CODIGO; VAMOS A ASUMIR ESTOS CODIGO QUE YA CONOCEMOS 875,439,880     
                  if (!is.na(str_locate(concepto,'Camara')[1]) and cargo > 0 ) {
                     codigo = '875'
                  } else if (!is.na(str_locate(concepto,'Propio')[1]) and cargo > 0) {
                     codigo = '439'
                  }
                  if (!is.na(str_locate(concepto,'Devolucion')[1]) and abono > 0 ) {   #Es un abono , devolucion cheque
                     codigo = '874'
                  } else if (!is.na(str_locate(concepto,'Propio')[1]) and abono > 0) {
                     codigo = '438'
                  }
               }
            }

            if (!is.na(str_locate(concepto,'ABONO LIQUIDACION')[1])) {  
               codigo = '880'
            }

            if (codigo == '212' | codigo == '213') {  #Estos son muy similares en estructura por eso decidimos usar una sola bandera
               flag_212_213 <- TRUE
               next  # faltan filas por revisar entonces hacemos un salto y no grabamos nada todavía
            }

            df_inserta <- rbind(df_inserta, data.frame(
               fecha_op = fecha_op,
               fecha_val = fecha_val,
               movto = movto,
               codigo = codigo,
               concepto1 = concepto,
               cargo = cargo,
               abono = abono,
               saldo = saldo,
               cheque = chequei,
		         cuenta_pago = '',
		         nombre_benef = ''))

         }
      }
      # Encontrar el primer día del mes
      #d_from <- floor_date(fecha_busca, "month")
      #d_to <- ceiling_date(fecha_busca, "month") - days(1)

	  d_from <- min(df_inserta$fecha_val)
	  d_to <- max(df_inserta$fecha_val)

      s_qry = "select movto, concepto from edocta where fecha_val >= '%s' and fecha_val <= '%s' "
      s_qry = sprintf (s_qry,d_from,d_to)
	  
	  escribir_log (file_conn, paste ('Query busqueda en edocta actual', s_qry))
	  
      df_enBD <- dbGetQuery(con, s_qry)
    
      if (nrow(df_enBD) > 0 ) {
         df_faltan <- merge(df_inserta, df_enBD, by = "movto", all.x = TRUE, all.y = FALSE)
         # Filtrar los registros que tienen NA en la columna de B
         df_faltan <- df_faltan[is.na(df_faltan$concepto), ]
         df_faltan <- df_faltan %>% select (-concepto) # Registros que no se encuentran en la tabla edocta
	      df_faltan <- df_faltan %>%
  		        rename(concepto = concepto1)

      } else {
	      df_faltan = df_inserta
	      df_faltan <- df_faltan %>%
  		      rename(concepto = concepto1)  # renombramos el nombre de la columna al nombre que tiene en la BD
	   }
      #print (head(df_faltan))
      if (nrow (df_faltan) > 0 ) {
      # Insertar los datos en la tabla de PostgreSQL
         dbBegin (con)
            flag_commit = TRUE
            escribir_log (file_conn,paste ("Se van a escribir ", nrow(df_faltan), sep = " "))
            dbWriteTable(con, "edocta", df_faltan, append = TRUE, row.names = FALSE)

            solo_uno = true
            for (irow in 1:nrow(df_faltan)) {
               if (df_faltan$codigo == '875' | df_faltan$codigo == '439') {
                  str_update = "update t_cheques set estado = 'cobrado' where num_cheque = %s"
                  str_update = sprintf (str_update,df_faltan$cheque)
                  if (solo_uno == true) {
                     escribir_log (file_conn,paste ("Actualizamos estado de cheque ", str_update, sep = " ")) 
                     solo_uno = false
                  }
                  dbExecute (con,str_update)
               }
               
               if (df_faltan$codigo == '874' | df_faltan$codigo == '438') {
                  str_update = "update t_cheques set estado = 'devuelto' where num_cheque = %s"
                  str_update = sprintf (str_update,df_faltan$cheque)
                  dbExecute (con,str_update)
               }
            }
    
         dbCommit (con) 
         flag_commit = FALSE
      } else {
         escribir_log (file_conn,paste ("Todos los registros ya estan en la BD ", nrow(df_faltan), sep = " "))
         codigoerror = 722
         stop ("Registros repetidos")
      }


	   escribir_log (file_conn,paste ("Proceso carga estado cuenta corto finalizado...", sep = " "))
	   cerrar_log (file_conn) 

      #if (dbIsValid (con)) {   #dejamos conexion abierta para ligar CLCs
      #   dbDisconnect (con)
      #}
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