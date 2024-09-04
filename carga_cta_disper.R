

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
      file1 <- paste0(root_dir, "/Dispersion/", iarchivo1 )
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


      if (ncol(df_disper) == 5 ) {  #Algunos archivos vienen con 5 columnas y otros con 6.
         colnames(df_disper) <- c("id_empleado","num_cuenta", "monto", "nota_desc", "emp_nombre")

      } else 
      {  if (ncol(df_disper) == 6) {
            colnames(df_disper) <- c("id_empleado","num_cuenta", "monto", "nota_desc", "id_emp", "emp_nombre")
            df_disper <- df_disper %>% select (-id_emp)  # eliminamos la columna extra que viene en ocasiones en archivos excel
         } else {
            codigoerror = 718 
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
         stop ("Archivo contiene mas registros que empleados")
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
      res_find = dbGetQuery(con, qry_find_previous)

      if (nrow (res_find) > 0) {
         codigoerror = 720
         stop ("Archivo ctrl dispersion ya existe con mismos datos, no puede cargar dispersion nuevamente")
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


         escribir_log (file_conn, paste("Se actualizaran " , nrow(df_merge), " registros"))

         dbWriteTable(con, "dispersion", df_merge, append = TRUE, row.names = FALSE)  #todos los registros con empleado valido
         dbWriteTable(con, "dispersion", df_no_emps, append = TRUE, row.names = FALSE) #todos los regisotrs no validos.

      dbCommit (con) 
	   
      flag_commit = FALSE

      escribir_log (file_conn,paste ("Proceso carga dispersion finalizado...", sep = " "))
	   cerrar_log (file_conn) 

      if (dbIsValid (con)) {   #Cerramos base de datos
         dbDisconnect (con)
      }
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
                  dbDisconnect (con)
               }

               if (codigoerror == 0) {
                  codigoerror = 700
               }     
               return (codigoerror)        
      }
   )

}  



