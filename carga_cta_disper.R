

carga_dispersion <- function (ianio,iquincena,tipo_carga,iarchivo1,con)
{

   file_conn = abrir_log ()
   escribir_log (file_conn, "Inicio Carga Cuentas Transferencias Dispersion....")

   tryCatch (
   {
      codigoerror = 0
      flag_commit = FALSE
  	   #Leer valores archivo ini o properties
	   checkini = list()	
      iniFile = "./cfg_creacheq.ini"
      checkini <- read.ini(iniFile)
      

      #result_bd = abrir_BD() 
      #con = result_bd$con_bd
      #codigoerror = result_bd$codigoerror

      if (!dbIsValid (con)) {
     	   escribir_log (file_conn,"Conexion BD invalida")
	      val_return = -1
         codigoerror = 715
         stop ("Conexion DB invalida")
      }

	   # Leer el archivo Excel

      root_dir <- checkini$Directory$drootedo
      file1 <- paste0(root_dir, "Dispersion/", iarchivo1 )

      #file1 = "./excel_disper/ARCHIVO DISPER 1A ENE 24.xls"

      # Verificar si el archivo existe
      if (!file.exists(file1)) {
         #agregar linea en cada stop
         codigoerror = 701  
         stop(paste("Error: El archivo no existe en la ruta especificada:", file1))
      }
      df_disper <- read_excel (file1,col_names = FALSE)
      df_disper = as.data.frame(df_disper)

      query <- checkini$Queries$carga_emp_min
      escribir_log (file_conn, paste ("Se leen registros de empleado", query , sep = " "))
       ###checkini$Queries$carga_emp # Reemplaza con tu consulta
      df_emp <- dbGetQuery(con, query)

      query_emp_tot <- paste (checkini$Queries$carga_emp_tot.1,
                              checkini$Queries$carga_emp_tot.2,
                              checkini$Queries$carga_emp_tot.3)
      query_emp_tot <- sprintf (query_emp_tot,ianio,iquincena)
      escribir_log (file_conn, paste ("Se leen registros de empleados totales", query_emp_tot , sep = " "))

      ###checkini$Queries$carga_emp # Reemplaza con tu consulta
      df_emp_total <- dbGetQuery(con, query_emp_tot)

      if (nrow(df_emp_total) <= 0) {
         codigoerror = 726
         stop ("No existen registros de carga de nomina para la quincena que ha seleccionado")
      }

      str_qry = paste ("select aa.id_empleado from edocta_idx as zz " ,
                       "join dispersion as aa on aa.anio = zz.anio and aa.quincena = zz.quincena " ,
                       "where zz.anio = %s and zz.quincena = '%s' and zz.reg_cancelado = FALSE and ",
                       "zz.carga_completa = TRUE and zz.tipo_carga = 'Dispersion' limit 1 ")
      str_qry = sprintf (str_qry,ianio,iquincena)
      escribir_log (file_conn, paste ("Se leen registros buscando dispersiones de la quincena", str_qry , sep = " "))
      
      df_val_disp <- dbGetQuery(con, str_qry)

      if (nrow(df_val_disp) > 0) {
         codigoerror = 727
         stop ("Ya existen registros de dispersion para la quincena que ha seleccionado, no puede continuar")
      }


      if (ncol(df_disper) == 5 ) {  #Algunos archivos vienen con 5 columnas y otros con 6.
         colnames(df_disper) <- c("id_empleado","num_cuenta", "monto", "nota_desc", "emp_nombre")

      } else 
      {  if (ncol(df_disper) == 6) {
            colnames(df_disper) <- c("id_empleado","num_cuenta", "monto", "nota_desc", "id_emp", "emp_nombre")
            df_disper <- df_disper %>% select (-id_emp)  # eliminamos la columna extra que viene en ocasiones en archivos excel
         } else {
            codigoerror = 718 
            stop ("El numero de columnas en archivo dispersion no es 5 ni 6, no se puede procesar")
         }
      }

      df_disper <- df_disper %>% filter(!is.na(num_cuenta)) #eliminamos filas donde cuenta sea nulo (NA)
      df_disper$id_empleado <- as.integer(df_disper$id_empleado) #convertimos empleado a numerico
      df_disper$monto <- as.numeric(df_disper$monto) #convertimos monto a numerico
      df_disper$monto [is.na(df_disper$monto)] <- 0  #ponemos 0 en todas las celdas donde monto sea NA
      df_disper$nota_desc [is.na(df_disper$nota_desc)] <- 'NO DESC'  #ponemos NO DESC en todas las celdas donde nota_desc sea NA

      # Merge de A y B, pero solo manteniendo los registros que están en A pero no en B
      df_no_emps <- merge(df_disper, df_emp, by = "id_empleado", all.x = TRUE, all.y = FALSE)
      # Filtrar los registros que tienen NA en la columna de B
      df_no_emps <- df_no_emps[is.na(df_no_emps$nombre), ]

      df_merge <- inner_join(df_disper, df_emp, by ='id_empleado')  #Empleados validos que vienen en el excel

      escribir_log (file_conn,paste ("Lineas lectura df_emp ", nrow(df_emp), " df_disper ",nrow(df_disper), 
                                  " df_merge " , nrow (df_merge) , " df no_emp ", nrow(df_no_emps), sep = " "))


      #df_limpio <- df_no_emps %>%    # En caso de quere saber los nulos en base a una columna
      #filter(is.na(monto))

      if (nrow(df_merge) > nrow(df_emp)) {
         escribir_log (file_conn, "El numero de empleados con num cuenta no deberia ser mayor que el numero total de empleado")
         codigoerror = 717
         stop ("Archivo dispersion contiene mas registros que empleados")
      }

      # arma una tabla con el numero de veces que se repite la descripcion
      #conteo_descs <- df_disper %>% count(descripcion)
      #print (conteo_descs)
            
      itotal_empleados = nrow(df_merge) 
      itotal_monto = sum(df_merge$monto)
      itotal_descs = length (unique (df_disper$nota_desc))
      itotal_no_emp = nrow(df_no_emps)
      imonto_no_emp = sum(df_no_emps$monto)

      qry_find_previous <- paste ("select from ctrl_dispersion where total_empleados = %s and ",
                                  "total_monto = %s and total_no_emp = %s and monto_no_emp = %s ")
      qry_find_previous <- sprintf (qry_find_previous,itotal_empleados,itotal_monto,itotal_no_emp,imonto_no_emp)
      escribir_log (file_conn, paste("Se revisa que exista registro con mismos datos: " ,qry_find_previous))

      res_find = dbGetQuery(con, qry_find_previous)

      if (nrow (res_find) > 0) {
         codigoerror = 720
         stop ("Archivo ctrl dispersion ya existe con mismos datos, no puede cargar dispersion nuevamente")
      }  

      s_qry = paste ("select aa.*, zz.quincena from nomina_idx as zz ", 
                     "join honorarios as aa on aa.ctrl_idx = zz.ctrl_idx ",
                     "where zz.anio = %s and zz.quincena = '%s' ",
                     "and zz.reg_cancelado = FALSE and zz.carga_completa = TRUE ")
      s_qry = sprintf (s_qry,ianio,imes)
      df_honor <- dbGetQuery(con, s_qry)  
      
      if (nrow(df_honor) > 0) { 
	      df_honor <- df_honor %>%
  		      mutate(id_empleado = as.integer (identificador))  #convertimos identificador en id_empleado
      }

      dbBegin (con)
         flag_commit = TRUE
         set_insert <- paste("insert into ctrl_dispersion ",
                     "(total_empleados,total_monto,total_descs, total_no_emp, monto_no_emp) values ",
                     "(%s, %s, %s, %s, %s) ") 
         #set_insert <- 
         #   sprintf (set_insert, nrow(df_merge) ,sum(df_merge$monto),length (unique (df_disper$nota_desc)),nrow(df_no_emps),sum(df_no_emps$monto)) 

         set_insert <- 
            sprintf (set_insert, itotal_empleados,itotal_monto,itotal_descs,itotal_no_emp,imonto_no_emp) 

         escribir_log (file_conn, paste("Se inserta registro control con sentencia: " ,set_insert))
         do_insert <- dbExecute(con, set_insert) 
     dbCommit (con) 
     flag_commit = FALSE

         result <- dbGetQuery(con, "select ctrl_id from ctrl_dispersion order by ctrl_id DESC LIMIT 1")
      
         last_id = result$ctrl_id[1]
      
         df_merge <- df_merge %>% select(-nombre)  #Empleados validos
         df_no_emps <- df_no_emps %>% select (-nombre) # Registros que no se encuentran en la tabla empleados.
      
         df_merge <- df_merge %>% mutate(ctrl_id = last_id)
         df_no_emps <- df_no_emps %>% mutate(ctrl_id = last_id)
      
         df_merge <- df_merge %>% mutate(anio = ianio)
         df_no_emps <- df_no_emps %>% mutate(anio = ianio)
      
         df_merge <- df_merge %>% mutate(quincena = iquincena)
         df_no_emps <- df_no_emps %>% mutate(quincena = iquincena)

     dbBegin (con)
         flag_commit = TRUE

         escribir_log (file_conn, paste("Se actualizaran " , nrow(df_merge), " registros"))

         dbWriteTable(con, "dispersion", df_merge, append = TRUE, row.names = FALSE)  #todos los registros con empleado valido
         dbWriteTable(con, "dispersion", df_no_emps, append = TRUE, row.names = FALSE) #todos los regisotrs no validos.

         df_up_empleados <- select (df_emp_total,ctrl_idx,id_empleado)
         df_up_empleados <- inner_join(df_up_empleados,df_merge, by = "id_empleado")  #selecionamos los empleados referentes a la nomina

         df_up_honor <- select (df_honor,ctrl_idx,id_empleado)
         df_up_honor <- inner_join(df_up_honor,df_merge, by = "id_empleado")  #a los que se paga por honorarios los buscamos dentro del excel leido
         
         escribir_log (file_conn, paste("Total de registros a actualizar : " , nrow(df_up_empleados), 
                                    " Emp total: ", nrow(df_emp_total), "Merge:" , nrow(df_merge) ))

         if (nrow(df_up_empleados) > 0) {
            for (ii in 1:nrow(df_up_empleados)) {               
   	         str_update <- 
               sprintf("UPDATE empleados_totales SET pago_trf = TRUE, referencia = '%s' where ctrl_idx = %s and id_empleado = %s", df_up_empleados$num_cuenta[ii],df_up_empleados$ctrl_idx[ii],df_up_empleados$id_empleado[ii])
            
               if (ii == 1) {
                  escribir_log (file_conn, paste("update query para sql ", str_update , sep = " "))
               }            
               dbExecute(con, str_update)
            }
	      }

         if (nrow(df_up_honor) > 0) { #Actualizamos a quienes se les pago por transferencia
            for (ii in 1:nrow(df_up_honor)) {               
   	         str_update <- 
               sprintf("UPDATE honorarios SET pago_trf = TRUE, referencia = '%s' where ctrl_idx = %s and identificador = '%s'", df_up_honor$num_cuenta[ii],df_up_honor$ctrl_idx[ii],df_up_honor$id_empleado[ii])
            
               if (ii == 1) {
                  escribir_log (file_conn, paste("update query para sql ", str_update , sep = " "))
               }            
               dbExecute(con, str_update)
            }
	      }

      dbCommit (con) 
	   
      flag_commit = FALSE

      escribir_log (file_conn,paste ("Proceso carga dispersion finalizado...", sep = " "))
	   cerrar_log (file_conn) 

      #if (dbIsValid (con)) {   #dejamos conexion abierta y el modulo principal la cierra
      #   dbDisconnect (con)
      #}
      return (codigoerror)   

   }, error = function(e) {
               # Formatear el mensaje de error con la fecha y hora
               mensaje_error <- paste(Sys.time(), ": ", e$message, sep = "")
               # Escribir el mensaje en el archivo
	            escribir_log (file_conn, mensaje_error)
		         cerrar_log(file_conn)
               if (dbIsValid (con)) {   #Cerramos base de datos
                  if (flag_commit == TRUE) {
                     dbRollback (con)  # en caso de error deshacemos la transaccion 
                  }
                  #dbDisconnect (con) #dejamos conexion abierta y el modulo principal la cierra
               }

               if (codigoerror == 0) {
                  codigoerror = 700
               }     
               return (codigoerror)        
      }
   )

}  



