
abrir_log <- function ()
{
   file_ctrl = file("./mi_log.txt", open='a')
   return (file_ctrl)
}

escribir_log <- function (file_ctrl, arg1) {

   if (is.na(file_ctrl)){
      file_ctrl = abrir_log()   
   }

   mensaje_error <- paste(Sys.time(), arg1, sep = " ")
   writeLines(mensaje_error, file_ctrl)
}

cerrar_log <- function(file_ctrl) {
   close(file_ctrl)
}

dividir_linea <- function(linea, anchos) {   #Se utiliza para cargar edo cuenta, separa una linea de acuerdo a longitudes fijas
      inicio <- 1
      fin <- anchos[1]
      columnas <- c()
  
      for (ii in 1:length(anchos)) {
         columna <- trimws(substr(linea, inicio, fin))
         columnas <- c(columnas, columna)
         inicio <- fin + 1
         fin <- fin + anchos[ii+1]
      }
      return(columnas)
}

abrir_BD <- function ()
{
   tryCatch({
      codigoerror = 0 # logica manejo errores
      val_return = 0

      file_conn = abrir_log()
      escribir_log(file_conn,"Abriendo BD....")
   
      checkini <- list()
      iniFile <- "./cfg_creacheq.ini"
      checkini <- read.ini(iniFile)

      # Conectar a la base de datos
      if (length(checkini) > 0) {
         con_bd <- dbConnect(RPostgres::Postgres(),
                       dbname = checkini$Database$dbname,
                       host = checkini$Database$host,
                       port = checkini$Database$port,
                       user = checkini$Database$user,
                       password = checkini$Database$password)
      }	else {
         con_bd <- dbConnect(RPostgres::Postgres(),
                      dbname = "SistemaNomina",
                      host = "192.168.100.252",
                      port = 5432,
                      user = "postgres",
                      password = "Pjmx3840")
      }
      cerrar_log (file_conn)
      if (!dbIsValid (con_bd)) {
         val_return = -1
         codigoerror = 715
      }
      return (list (val_return=val_return,codigoerror=codigoerror,con_bd=con_bd)) 

   }, error = function(e) {
     mensaje_error <- paste(Sys.time(), ": ", e$message, sep = "")
	  escribir_log (file_conn,mensaje_error)	
     cerrar_log (file_conn)
     #logica manejo de errores.
     val_return = -1
     if (codigoerror == 0) { #significa   que caimos aquí pero no pudimos capturar el error
       codigoerror = 700
     }
     return (list (val_return=val_return,codigoerror=codigoerror,con_bd=con_bd)) 
   })

}

#Busca registro , si lo encuentra regresa el numero de control = ctrl_idx y tambien variable booleaba si ya fue procesada 
busca_nomina_idx <- function(con_bd,ianio,iquincena,inombre)
{
   #con_bd = abrir_BD ()

   tryCatch({
      completa = FALSE

      file_conn = abrir_log()
      if (dbIsValid(con_bd)) {
         sql_query <- sprintf ("select ctrl_idx, carga_completa from nomina_idx where anio = %s and quincena = '%s' and nombre_nomina = '%s' and reg_cancelado = FALSE",ianio,iquincena,inombre)
	      escribir_log (file_conn,paste('Buscando registro con query ',sql_query))	
         result_qry = dbGetQuery (con_bd,sql_query)
	      #result_qry = dbSendQuery(con_bd, sql_query)
         #response = dbFetch (result_qry)
         result_qry <- as.data.frame (result_qry)

         if (nrow(result_qry > 0)) {
 		      escribir_log (file_conn,"Encontre registro ")
		      val_return <- result_qry$ctrl_idx[1]
		      completa <- result_qry$carga_completa[1]
            if (completa == TRUE) {
		         escribir_log (file_conn,paste("Ya fue procesada ", val_return, completa,sep = ' '))
            }	
         } else {
		      escribir_log (file_conn,"No encontre registro")
            val_return = -1 #registro no existe
         }  
	      #dbClearResult(result_qry)
      } else {
     	   escribir_log (file_conn,"Conexion BD invalida")
	      val_return = -1
      } 
	   cerrar_log (file_conn) 
	   return (list (val_return = val_return,completa = completa)) 
   }, error = function(e) {
     mensaje_error <- paste(Sys.time(), ": ", e$message, sep = "")
	  escribir_log (file_conn,mensaje_error)	
     cerrar_log (file_conn)
     return (list (val_return = -1,completa = completa))
   })
}


update_nomina_idx_ok <- function(con_bd, vidx,itipo) {
   
   tryCatch({
      codigoerror = 0

      file_conn = abrir_log()

      if (dbIsValid(con_bd)) {
	   #dbBegin (con_bd) 
         if (itipo == 'Honorarios') {
            str_qry = "select fecha_pago from honorarios where ctrl_idx = %s LIMIT 1"
            str_qry = sprintf (str_qry,vidx)
            df_et = dbGetQuery (con_bd,str_qry)
            vfec_pago = df_et$fecha_pago[1]
         } else {
            str_qry = "select fec_pago from empleados_totales where ctrl_idx = %s LIMIT 1"
            str_qry = sprintf (str_qry,vidx)
            df_et = dbGetQuery (con_bd,str_qry)
            vfec_pago = df_et$fec_pago[1]
         }
         sql_update <- sprintf ("update nomina_idx set carga_completa = TRUE, fin_carga = CURRENT_TIMESTAMP, fec_pago = '%s' where ctrl_idx = %s",vfec_pago,vidx)
	      escribir_log (file_conn, paste("Actualizando registro para completar proceso ", sql_update))
         dbExecute(con_bd, sql_update) 

	   #dbCommit(con_bd)
         val_return = 0
      } else {
      codigoerror = 715  # Conexion a BD invalida
	   val_return = -1
      } 
      cerrar_log(file_conn)
	return (list(val_return=val_return,codigoerror=codigoerror)) 

   }, error = function(e) {
      mensaje_error <- paste(Sys.time(), ": ", e$message, sep = "")
      escribir_log (file_conn,mensaje_error)	
      cerrar_log (file_conn)
     val_return = -1
     if (codigoerror == 0) { #significa   que caimos aquí pero no pudimos capturar el error
       codigoerror = 700
     }
     return (list(val_return=val_return,codigoerror=codigoerror))
   })

}

update_nomina_idx_cancela <- function(con_bd, vidx,vcodigoerror) {
   
   tryCatch({
      codigoerror = 0

      file_conn = abrir_log()

      if (dbIsValid(con_bd)) {
	   #dbBegin (con_bd) 

         sql_update <- sprintf ("update nomina_idx set reg_cancelado = TRUE, fin_carga = CURRENT_TIMESTAMP, codigoerror = %s where ctrl_idx = %s",vcodigoerror,vidx)
	      escribir_log (file_conn, paste("Actualizando para cancelar el registro debido a errores en el proceso ", sql_update))
         dbExecute(con_bd, sql_update) 

	   #dbCommit(con_bd)
         val_return = 0
      } else {
         codigoerror = 715 #Conexion a BD invalida
	      val_return = -1
      } 
      cerrar_log(file_conn)
	return (list (val_return=val_return,codigoerror=codigoerror)) 

   }, error = function(e) {
      mensaje_error <- paste(Sys.time(), ": ", e$message, sep = "")
      escribir_log (file_conn,mensaje_error)	
      cerrar_log (file_conn)
      val_return = -1
      if (codigoerror == 0) { #significa   que caimos aquí pero no pudimos capturar el error
        codigoerror = 700
      }
      return (list(val_return=val_return,codigoerror=codigoerror))
   })

}


crear_nomina_idx <- function(ianio,iquincena,inombre,con_bd)
{
   tryCatch({
      codigoerror = 0
      val_return = 0

      file_conn = abrir_log()
      escribir_log(file_conn,"Creando Registro....")
   
      #con_bd = abrir_BD ()
      
      if (dbIsValid(con_bd)){

         res_busca = busca_nomina_idx (con_bd,ianio,iquincena,inombre) 
         ctrl_idx = res_busca$val_return
         completa = res_busca$completa

         str_qry = "select fecha_ini from cat_quincena where anio = %s and quincena = '%s'"
         str_qry = sprintf (str_qry,ianio,iquincena)
         df_catq = dbGetQuery (con_bd,str_qry)
         if (nrow(df_catq) > 0){
            vfec_pago = df_catq$fecha_ini[1]
         } else {
            codigoerror = 724
            stop (paste("No existe registro para catalogo quincena  ", ianio, " ", iquincena , " no se puede procesar"))
         }  

         if (ctrl_idx != -1) {
            codigoerror = 716
            val_return = -1
            escribir_log (file_conn,"El registro para nomina ya existe con mismos parametros, no puede procesarse")
            #if (completa == FALSE) {  #Dado que la bandera indica que no se ha procesado daremos otra oportunidad de cargarlo
            #   val_return = ctrl_idx
            #   escribir_log (file_conn,"El registro para nomina ya existe y NO ha sido procesado, se permitira procesarse de nuevo")
            #} 
            # El registro ya no podra ocuparse para una segunda carga, mejor sí la carga falla se cancela
         } else {
            #sql_insert <- sprintf ("insert into nomina_idx (anio,quincena,nombre_nomina,fec_pago) values (%s,'%s','%s','%s')",ianio,iquincena,inombre,vfec_pago)
            sql_insert <- sprintf ("insert into nomina_idx (anio,quincena,nombre_nomina) values (%s,'%s','%s')",ianio,iquincena,inombre)
            
            escribir_log(file_conn,paste("Query para insert ",sql_insert))
   
		      dbExecute(con_bd, sql_insert) 

            res_busca = busca_nomina_idx (con_bd,ianio,iquincena,inombre) #buscar el registro recien creado 
            ctrl_idx = res_busca$val_return
            completa = res_busca$completa

            escribir_log(file_conn,paste("Valor ctrl_idx, crear nomina ",ctrl_idx,completa,sep = ' '))
            val_return = ctrl_idx
         }   

      } else {
         codigoerror = 715
	      val_return = -1
      } 

      cerrar_log(file_conn)
	   return (list (val_return=val_return,codigoerror=codigoerror))

   }, error = function(e) {
      mensaje_error <- paste(Sys.time(), ": ", e$message, sep = "")
      escribir_log (file_conn,mensaje_error)	
      cerrar_log (file_conn)
      val_return = -1
      if (codigoerror == 0) { #significa   que caimos aquí pero no pudimos capturar el error
        codigoerror = 700
      }
      return (list (val_return=val_return,codigoerror=codigoerror))
   })

}

crear_idx_edocta <- function(anio,quincena,tipo_carga,vuser,varchivo1,con_bd,verror)
{
   tryCatch({
      codigoerror = 0
      val_return = 0

      file_conn = abrir_log()
      escribir_log(file_conn,"Creando Registro para estado de cuenta...")
   
      #con_bd = abrir_BD ()
      
      if (dbIsValid(con_bd)){
         if (verror != 0) {
            sql_insert <- paste ("insert into edocta_idx (anio,quincena,tipo_carga,reg_cancelado,user_carga,nombre_archivo,fecha_carga,codigoerror) ",
                              "values (%s,'%s','%s',TRUE,'%s','%s',CURRENT_TIMESTAMP,%s)")
         } else {
            sql_insert <- paste ("insert into edocta_idx (anio,quincena,tipo_carga,carga_completa,user_carga,nombre_archivo,fecha_carga,codigoerror) ",
                              "values (%s,'%s','%s',TRUE,'%s','%s',CURRENT_TIMESTAMP,%s)")
         }
         sql_insert <- sprintf (sql_insert,anio,quincena,tipo_carga,vuser,varchivo1,verror)
         
         escribir_log(file_conn,paste("Query para insert edocta o dispersion ",sql_insert))
	      dbExecute(con_bd, sql_insert) 
      } else {
         codigoerror = 715
         escribir_log(file_conn,paste("Hubo un error de conexion a BD al crear registro de carga de edocta",codigoerror))
	      val_return = -1
      } 

      cerrar_log(file_conn)
	   return (list (val_return=val_return,codigoerror=codigoerror))

   }, error = function(e) {
      mensaje_error <- paste(Sys.time(), ": ", e$message, sep = "")
      escribir_log (file_conn,mensaje_error)	
      cerrar_log (file_conn)
      val_return = -1
      if (codigoerror == 0) { #significa   que caimos aquí pero no pudimos capturar el error
        codigoerror = 700
      }
      return (list (val_return=val_return,codigoerror=codigoerror))
   })

}

