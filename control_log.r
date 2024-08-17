
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

busca_nomina_idx <- function(con_bd,ianio,iquincena,inombre)
{
   #con_bd = abrir_BD ()

   tryCatch({
      file_conn = abrir_log()

      if (dbIsValid(con_bd)) {
         sql_query <- sprintf ("select ctrl_idx, carga_completa from nomina_idx where anio = %s and quincena = '%s' and nombre_nomina = '%s' and reg_cancelado = FALSE",ianio,iquincena,inombre)
	   escribir_log (file_conn,paste('Buscando registro con query ',sql_query))	
	   result_qry = dbSendQuery(con_bd, sql_query)
         response = dbFetch (result_qry)

         if (nrow(response > 0)) {
 		escribir_log (file_conn,"Encontre registro ")
		val_return = response$ctrl_idx[1]
		completa = response$carga_completa[1]
            if (completa == TRUE) {
		   escribir_log (file_conn,"Ya fue procesada")
            }	
         } else {
		escribir_log (file_conn,"No encontre registro")
            val_return = -1 #registro no existe
         }  
	   dbClearResult(result_qry)
      } else {
     	   cerrar_log (file_conn) 
	   val_return = -1
      } 
	cerrar_log (file_conn) 
	return (val_return) 

   }, error = function(e) {
     mensaje_error <- paste(Sys.time(), ": ", e$message, sep = "")
	  escribir_log (file_conn,mensaje_error)	
     cerrar_log (file_conn)
     return (-1)
   })

}

update_nomina_idx_ok <- function(con_bd, vidx) {
   
   tryCatch({
      file_conn = abrir_log()

      if (dbIsValid(con_bd)) {
	   #dbBegin (con_bd) 

         sql_update <- sprintf ("update nomina_idx set carga_completa = TRUE where ctrl_idx = %s",vidx)
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


crear_nomina_idx <- function(ianio,iquincena,inombre,con_bd)
{
   tryCatch({
      file_conn = abrir_log()
      escribir_log(file_conn,"Creando Registro....")
   
      #con_bd = abrir_BD ()
      
      if (dbIsValid(con_bd)){

         ctrl_idx = busca_nomina_idx (con_bd,ianio,iquincena,inombre) 
         if (ctrl_idx != -1) {
            escribir_log (file_conn,"El registro para nomina ya existe, no puede cargarse otra nomina con mismos parametros")
            val_return = -1
         } else {
            sql_insert <- sprintf ("insert into nomina_idx (anio,quincena,nombre_nomina) values (%s,'%s','%s')",ianio,iquincena,inombre)
            escribir_log(file_conn,paste("Query para insert ",sql_insert))
   
		dbExecute(con_bd, sql_insert) 

            ctrl_idx = busca_nomina_idx (con_bd,ianio,iquincena,inombre) #buscar el registro recien creado 
            escribir_log(file_conn,paste("Valor ctrl_idx, crear nomina ",ctrl_idx))
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

