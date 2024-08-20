
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


abrir_BD <- function ()
{
   tryCatch({
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
                      host = "192.168.100.215",
                      port = 5432,
                      user = "postgres",
                      password = "Pjmx3840")
      }
      cerrar_log (file_conn)
      return (con_bd) 

   }, error = function(e) {
     mensaje_error <- paste(Sys.time(), ": ", e$message, sep = "")
	  escribir_log (file_conn,mensaje_error)	
     cerrar_log (file_conn)
     return (-1)
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
         result_qry <- as.data.frame result_qry

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

update_nomina_idx_ok <- function(con_bd, vidx) {
   
   tryCatch({
      file_conn = abrir_log()

      if (dbIsValid(con_bd)) {
	   #dbBegin (con_bd) 

         sql_update <- sprintf ("update nomina_idx set carga_completa = TRUE, fin_carga = CURRENT_TIMESTAMP where ctrl_idx = %s",vidx)
	     escribir_log (file_conn, paste("Actualizando registro para completar proceso ", sql_update))
         dbExecute(con_bd, sql_update) 

	   #dbCommit(con_bd)
         val_return = 0
      } else {
	   val_return = -1
      } 
      cerrar_log(file_conn)
	return (val_return) 

   }, error = function(e) {
      mensaje_error <- paste(Sys.time(), ": ", e$message, sep = "")
      escribir_log (file_conn,mensaje_error)	
      cerrar_log (file_conn)
      return (-1)
   })

}

update_nomina_idx_cancela <- function(con_bd, vidx) {
   
   tryCatch({
      file_conn = abrir_log()

      if (dbIsValid(con_bd)) {
	   #dbBegin (con_bd) 

         sql_update <- sprintf ("update nomina_idx set reg_cancelado = TRUE, fin_carga = CURRENT_TIMESTAMP where ctrl_idx = %s",vidx)
	      escribir_log (file_conn, paste("Actualizando para cancelar el registro debido a errores en el proceso ", sql_update))
         dbExecute(con_bd, sql_update) 

	   #dbCommit(con_bd)
         val_return = 0
      } else {
	   val_return = -1
      } 
      cerrar_log(file_conn)
	return (val_return) 

   }, error = function(e) {
      mensaje_error <- paste(Sys.time(), ": ", e$message, sep = "")
      escribir_log (file_conn,mensaje_error)	
      cerrar_log (file_conn)
      return (-1)
   })

}


crear_nomina_idx <- function(ianio,iquincena,inombre,con_bd)
{
   tryCatch({
      file_conn = abrir_log()
      escribir_log(file_conn,"Creando Registro....")
   
      #con_bd = abrir_BD ()
      
      if (dbIsValid(con_bd)){

         res_busca = busca_nomina_idx (con_bd,ianio,iquincena,inombre) 
         ctrl_idx = res_busca$val_return
         completa = res_busca$completa

         if (ctrl_idx != -1) {
            val_return = -1 
            escribir_log (file_conn,"El registro para nomina ya existe con mismos parametros, no puede procesarse")
            #if (completa == FALSE) {  #Dado que la bandera indica que no se ha procesado daremos otra oportunidad de cargarlo
            #   val_return = ctrl_idx
            #   escribir_log (file_conn,"El registro para nomina ya existe y NO ha sido procesado, se permitira procesarse de nuevo")
            #} 
            # El registro ya no podra ocuparse para una segunda carga, mejor sí la carga falla se cancela
         } else {
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
	      val_return = -1	
      } 

      cerrar_log(file_conn)
	   return (val_return)

   }, error = function(e) {
      mensaje_error <- paste(Sys.time(), ": ", e$message, sep = "")
      escribir_log (file_conn,mensaje_error)	
      cerrar_log (file_conn)
      return (-1)
   })

}

