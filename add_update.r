

tabla_empleados <- function (ianio,iquincena,itipo,iarchivo) 
{

file_conn = abrir_log ()
escribir_log (file_conn,"Inicio Carga Empleados....")


tryCatch (
{

	#Leer valores archivo ini o properties
	checkini = list()	
      iniFile = "./cfg_creacheq.ini"
      checkini <- read.ini(iniFile)


	#Conectar a la base de datos
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

#####query <- "SELECT * FROM empleados where empleados.activo = True;"  # Reemplaza con tu consulta

query <- checkini$Queries$carga_emp # Reemplaza con tu consulta
emp_actual <- dbGetQuery(con, query);

####"SELECT * FROM empleados_nomina join empleados on empleados_nomina.id_empleado = empleados.id_empleado and empleados.activo = True;"  # Reemplaza con tu consulta

query <- checkini$Queries$carga_emp_nom
emp_nom_actual <- dbGetQuery(con, query);

query <- checkini$Queries$carga_emp_inactivo # Reemplaza con tu consulta
df_inactivo <- dbGetQuery(con, query);

escribir_log (file_conn, paste ("Numero de registros para emp_actual , emp_nom_actual, inactivos", nrow(emp_actual), nrow(emp_nom_actual) , nrow(df_inactivo) , sep = " ")) 


# datos prueba 
#ianio = 2024
#iquincena = '05'
#itipo = 'Compuesta'
#iarchivo = '52_fini'
#iarchivo2 = '52-azcapotzalco'

      # Leer el archivo Excel
      # Directorio raíz para los archivos Excel
      root_dir <- checkini$Directory$droot
      file1 = paste0(root_dir, "nomina ", itipo, "/Respaldos/", ianio, "/", iquincena, "_", iarchivo, ".xlsx")

      # Verificar si el archivo existe
      if (!file.exists(file1)) {
         stop(paste("Error: El archivo no existe en la ruta especificada:", file1))
      }

      data_52 <- read_excel(file1)


#jma ambiente local --- root_dir = "C:/Users/jemar/OneDrive/Escritorio/raiz/"
#jma ambiente loca -- data_52 <- read_excel (paste0(root_dir,"202405_52_fini.xlsx"))
# Convertir nombres de columnas a minúsculas
names(data_52) <- tolower(names(data_52))

# Definir los campos requeridos en minúsculas
required_fields <- c("id_empleado", "nombre", "apellido_1", "apellido_2",
                     "curp", "id_legal", "id_sexo", "fec_nac", "fec_alta_empleado",
                     "fec_antiguedad", "numero_ss", "dias_lab", "id_reg_issste",
                     "ahorr_soli_porc", "estado", "deleg_municip", "poblacion",
                     "colonia", "direccion", "codigo_postal", "num_interior",
                     "num_exterior", "calle", "n_delegacion_municipio",
                     "ent_federativa", "n_puesto")    #sect_pres
  
missing_columns <- setdiff(required_fields, names(data_52))

if (length(missing_columns) > 0) {
  
  stop(paste("Faltan las siguientes columnas en el archivo:", paste(missing_columns, collapse = ", ")))
  # porner codigo para hacer un insert a la tabla bitacora y a la tabla nomina_ctrl
}

data_filtered <- data_52[, required_fields]

#paso 1. pasamos todos a caracter
data_filtered <- data_filtered %>%
  mutate(across(everything(), as.character))
#paso 2. paso id_empleado a integer
data_filtered$id_empleado <- as.integer(data_filtered$id_empleado)
#paso 3 paso fechas a date


data_filtered$fec_nac <- as.Date(gsub("/", "-", data_filtered$fec_nac),format = "%d-%m-%Y")
data_filtered$fec_antiguedad <- as.Date(gsub("/", "-", data_filtered$fec_antiguedad),format = "%d-%m-%Y")
data_filtered$fec_alta_empleado <- as.Date(gsub("/", "-", data_filtered$fec_alta_empleado),format = "%d-%m-%Y")

#paso4. paso a enteros los que correspondan de acuerdo a la BD debido a que en el primer paso puse todos como caracter
#cambiar las variables leidas al tipo que corresponda de acuerdo a la BD
data_filtered$dias_lab <- as.integer(data_filtered$dias_lab)        
data_filtered$ahorr_soli_porc <- as.double(data_filtered$ahorr_soli_porc)        

#compare_emp <- emp_actual [, required_fields]
compare_emp <- emp_actual %>% select (any_of(c(required_fields)))

#ambiente desarrollo jma tipo_nomina = "Compuesta"

tipo_nomina = itipo

# FINIQUITOS
if (tipo_nomina == "Finiquitos") {
   result = anti_join(data_filtered,compare_emp,by='id_empleado')  

   escribir_log (paste ( "Numero de registros para result y data_filtered ", nrow(result), nrow(data_filtered) , sep = " "))


   if ( nrow(result) == 0) 
   {
      escribir_log (file_conn, paste("El resultado de nrow es igual a cero, podemos proceder con finiquitos ", sep = " "))

  	data_finiquito <- as.data.frame(data_filtered)

      escribir_log (file_conn, paste("Registros finiquito ", data_finiquito , sep = " "))
 
      for (ii in 1:nrow(data_finiquito)) {               
   	   str_update <- sprintf("UPDATE empleados SET activo = FALSE where id_empleado = %s", data_finiquito[ii, 1])

         escribir_log (file_conn, paste("update query para sql ", str_update , sep = " "))
         #dbExecute(con, str_update)
	}

   } else 
   {
      escribir_log (file_conn, paste("El resultado de nrow es diferente de cero, no podemos proceder con finiquitos ", sep = " "))
   }
} else 
{
   #Buscar empledos del excel fuera de la BD
   result = anti_join(data_filtered,compare_emp,by='id_empleado')

   escribir_log (file_conn, paste("Se procesa anti  join para buscar empleados del excel fuera del BD " , nrow(result), sep = " "))

   check_inactivo = df_inactivo$id_empleado %in% result$id_empleado
 
   if (nrow(df_inactivo) > 0 ) {
      stop ("No pueden adicionarse empleados que estan inactivos nuevamente") 
   }
  
   if (nrow(compare_emp) == 0 | nrow(result) > 0) {   #la BD de empleados esta completamente vacia o la hoja excel contiene
      #empleados que no estan en la BD

      if (tipo_nomina == "Extraordinarios") {

         escribir_log (file_conn, paste("No puede procesar extraordinarias para empleados que no existen en la BD, Son ", nrow(result), " empleados inexistentes.", sep = " "))
  
      } else 
      {
	   escribir_log (file_conn, paste("Se escriben registro en la BD de empleados, Son ", nrow(result), " empleados adicionales.", sep = " "))
  	   # Escribir los datos en la tabla empleados en PostgreSQL
  	   dbWriteTable(con, "empleados", result, append = TRUE, row.names = FALSE)
      }
   }   
}

escribir_log (file_conn, paste( "Fuera de finiquitos", sep = " "))

if ((tipo_nomina == "Compuesta") | (tipo_nomina == "Extraordinarios")) { 

   escribir_log (file_conn, paste( "Dentro de check de columnas", sep = " "))


   #+++++++++++ Volvemos a leer la BD después de insertar los pendientes, en esta segunda parte buscaremos diferencias en las columnas
   query <- "SELECT * FROM empleados where empleados.activo = True"  # Reemplaza con tu consulta
   emp_actual <- dbGetQuery(con, query);
   #compare_emp <- emp_actual [, required_fields]
   compare_emp <- emp_actual %>% select (any_of(c(required_fields)))

   #if (exists("compare_emp") && is.data.frame(compare_emp)) {

   escribir_log (file_conn, paste("Tipo variable compare_emp ", str (compare_emp)))
   escribir_log (file_conn, paste("Tipo variable data_filtered ", str (data_filtered)))
   escribir_log (file_conn, paste("Tipo variable compare_emp-id_empleado ", str (compare_emp$id_empleado)))
   compare_emp$id_empleado <- as.integer(compare_emp$id_empleado)
   escribir_log (file_conn, paste("Tipo variable compare_emp-id_empleado ", str (compare_emp$id_empleado)))


   data_filtered <- data_filtered[order(data_filtered$id_empleado), ]
   compare_emp <- compare_emp[order(compare_emp$id_empleado), ]

   #result = inner_join(compare_emp,data_filteredby='id_empleado')
   #merged_df <- merge(data_filtered,compare_emp, by = "id_empleado", all = TRUE)
   result = anti_join(compare_emp,data_filtered,by='id_empleado')


   escribir_log(file_conn,paste("Numero lineas en result ", nrow(result), sep = " "))
   new_tab_emp <- compare_emp

   if (nrow(result) > 0) # numero de lineas que estan en la BD pero no vienen en el excel
   { 
      for (ii in 1:nrow(result)) {  #borramos las lineas del dataframe de empleados que no vengan en el excel
         new_tab_emp <- new_tab_emp[new_tab_emp$id_empleado != result[ii,1], ]
      }
   }
   escribir_log(file_conn,paste( "Numero lineas despues de borrar: ", nrow(data_filtered), nrow(new_tab_emp), sep = " "))

   if (nrow(data_filtered) == nrow(new_tab_emp))
   {
      col_cambios <- data_filtered == new_tab_emp
      col_cambios  <- as.data.frame(col_cambios)
      col_cambios [is.na(col_cambios)] <- TRUE  

      if (is.null(col_cambios) == FALSE) 
      {
         if (any(col_cambios == FALSE))  #hay al menos un cambio en las columnas de datos
         { 
            false_positions <- list()
            for (irow in 1:nrow(col_cambios)) 
            {
               for (icol in 1:ncol(col_cambios)) 
               {
                  if (is.na (col_cambios[irow,icol])) {
                     next
                  }
          
                  if (col_cambios[irow, icol] == FALSE) {
                     # Guardar la posición (fila, columna) de cada valor FALSE
                     false_positions <- c(false_positions, list(c(irow, icol)))
            
                     #print (compare_emp[irow,icol])
                     #print (data_filtered[irow,icol])
                     #print (colnames(data_filtered[icol]))
            
                     str_update <- 
                        sprintf("UPDATE empleados SET %s = '%s' Where id_empleado = %s", colnames(data_filtered[icol]), data_filtered[irow,icol],data_filtered[irow, 1])
                     #print (str_update)
			   escribir_log (file_conn,paste( "update query empleados cambios ", str_update , sep = " "))

                     #dbExecute(con, str_update)

                     str_insert <- 
                        sprintf("insert into bitacora ( id_empleado, campo, valor_inicial, valor_final) values (%s,'%s','%s','%s')",data_filtered[irow, 1], colnames(data_filtered[icol]), new_tab_emp[irow,icol],data_filtered[irow,icol])
                     #print (str_insert)
			   escribir_log (file_conn,paste("insert bitacora ", str_insert , sep = " "))
                     dbExecute(con, str_insert)
                  }  
               }
            }
         }
      }
   } else 
   {
      escribir_log (file_conn,paste("No se puede procesar por ser de diferentes tamaños " , sep = " "))
   }
}  # if tipo_nomina Compuesta o extraordinaria

   escribir_log (file_conn,paste("Cerrando BD", sep = " "))

   dbDisconnect(con)  
   cerrar_log(file_conn)

}, error = function(e) {
              # Formatear el mensaje de error con la fecha y hora
              mensaje_error <- paste(Sys.time(), ": ", e$message, sep = "")
  
   		  escribir_log (file_conn,mensaje_error)

   		  cerrar_log(file_conn)

            }
)


}


