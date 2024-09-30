
hacer_reporte1 <- function (ianio,imes,con)
{

   file_conn = abrir_log ()
   escribir_log (file_conn, "Inicio relacion CLC con Nominas")

   tryCatch (
   {
      codigoerror = 0
      flag_commit = FALSE
      checkini = list()	
      iniFile = "./cfg_creacheq.ini"
      checkini <- read.ini(iniFile)

      result_bd = abrir_BD() 
      con = result_bd$con_bd
      codigoerror = result_bd$codigoerror

      if (!dbIsValid (con)) {
     	   escribir_log (file_conn,"Conexion BD invalida")
	      val_return = -1
         codigoerror = 715
         stop ("Conexion DB invalida")
      }
      root_dir = checkini$Directory$drootedo

      s_qry = paste ("select * from nomina_idx where to_char(fec_pago,'YYYY') = '%s' and to_char(fec_pago,'MM') = '%s'",
                     "and reg_cancelado = FALSE and carga_completa = TRUE ") 
    

      s_qry = sprintf (s_qry,ianio,imes)
      df_idx <- dbGetQuery(con, s_qry)

      if (nrow(df_idx) == 0 ) {
         print ("No hay datos para nomina idx")
      } else {
		print ("continuamos") 
	}

      # Encontrar el primer día del mes
      fecha_origen = as.Date (sprintf ("%s-%s-01",ianio,imes))
      d_from <- floor_date(fecha_origen, "month")
      d_to <- ceiling_date(fecha_origen, "month") - days(1)
      # con intervalo de 5 días damos una ventana para que llegue el recurso, como ejemplo la nomina honorarios de Enero31 y el recurso llego 2Feb 
      d_to <- d_to + days(5)  

      s_qry = paste ("select * from edocta where fecha_val >= '%s' and fecha_val <= '%s'") 
      s_qry = sprintf (s_qry,d_from,d_to)
      df_edocta <- dbGetQuery(con, s_qry)
	   df_edocta <- df_edocta %>% 
  		mutate(nombre_nomina = trimws(nombre_nomina))  #Limpiamos espacios en blanco para que el join funcione correctamente

      s_qry = paste ("select aa.* from nomina_idx as zz ", 
                     "join empleados_totales as aa on aa.ctrl_idx = zz.ctrl_idx ",
                     "where to_char(zz.fec_pago,'YYYY') = '%s' and to_char(zz.fec_pago,'MM') = '%s' ",
                     "and zz.reg_cancelado = FALSE and zz.carga_completa = TRUE ")
      s_qry = sprintf (s_qry,ianio,imes)
      df_emp_total <- dbGetQuery(con, s_qry)    
	   df_emp_total <- df_emp_total %>%
  		mutate(quincena = trimws(quincena))  #Limpiamos espacios en blanco para que el join funcione correctamente

      s_qry = paste ("select aa.*, zz.quincena from nomina_idx as zz ", 
                     "join honorarios as aa on aa.ctrl_idx = zz.ctrl_idx ",
                     "where to_char(zz.fec_pago,'YYYY') = '%s' and to_char(zz.fec_pago,'MM') = '%s' ",
                     "and zz.reg_cancelado = FALSE and zz.carga_completa = TRUE ")
      s_qry = sprintf (s_qry,ianio,imes)
      df_honor <- dbGetQuery(con, s_qry)    
	df_honor <- df_honor %>%
  		mutate(quincena = trimws(quincena))  #Limpiamos espacios en blanco para que el join funcione correctamente
	df_honor <- df_honor %>%
  		mutate(nombre_nomina = trimws(subprograma)) #Ajustamos algunos campos para homologar las columnas del reporte
	df_honor <- df_honor %>%
  		mutate(apellido_1 = trimws(nombre_empleado))  
	df_honor <- df_honor %>%
  		mutate(id_empleado = as.integer (identificador))  
	df_honor <- df_honor %>%
  		mutate(fec_pago = fecha_pago)  	
	df_honor <- df_honor %>%
  		mutate(apellido_2 = '')  	
	df_honor <- df_honor %>%
  		mutate(nombre = '')  	
	#df_honor <- df_honor %>%
  	#	mutate(refer_pago = '')  	
	#df_honor <- df_honor %>%
  	#	mutate(bcheque = '')  	


      s_qry = paste ("select * from empleados ") 
      #s_qry = sprintf (s_qry,ianio,imes)
      df_empleados <- dbGetQuery(con, s_qry)     
     
      s_qry = paste ("select * from empleados_nomina ") 
      df_emp_nom <- dbGetQuery(con, s_qry)     

      s_qry = paste ("select * from cat_universo") 
      df_cat_uni <- dbGetQuery(con, s_qry)     
	df_cat_uni <- df_cat_uni %>%
  		mutate(nombre_nomina = trimws(nombre_nomina)) #Limpiamos espacios en blanco para que el join funcione correctamente

      s_qry = paste ("select * from cat_quincena") 
      df_cat_quin <- dbGetQuery(con, s_qry)     
	df_cat_quin <- df_cat_quin %>%
  		mutate(quincena = trimws(quincena))

      s_qry = paste ("select aa.* from cat_quincena as zz ",
                    "join dispersion as aa on aa.anio = zz.anio and aa.quincena = zz.quincena ",
                    "where zz.anio = %s and zz.mes = '%s' ") 
      s_qry = sprintf (s_qry,ianio,imes)
      df_dispersion <- dbGetQuery(con, s_qry)     
	df_dispersion <- df_dispersion %>%
  		mutate(quincena = trimws(quincena))


      s_qry = paste ("select aa.* from nomina_idx as zz ", 
                     "join t_cheques as aa on aa.ctrl_idx = zz.ctrl_idx ",
                     "where to_char(zz.fec_pago,'YYYY') = '%s' and to_char(zz.fec_pago,'MM') = '%s' ",
                     "and zz.reg_cancelado = FALSE and zz.carga_completa = TRUE ")
      s_qry = sprintf (s_qry,ianio,imes)
      df_cheques <- dbGetQuery(con, s_qry)   


	#Creamos una estructura para los casos donde una nomina se pago con mas de una CLC
      df_edocta_varios <- df_edocta %>% 
			filter(ctrl_idx != 0) %>% 
			group_by(ctrl_idx,nombre_nomina) %>% 
			summarise(n_conceptos = toString(trimws(concepto)),.groups = "drop" )      

      df_reporte <- df_emp_total %>%
                    left_join(select (df_empleados,id_empleado,nombre,apellido_1,apellido_2) , by = "id_empleado") %>%
                    left_join(select (df_emp_nom,id_empleado,id_universo), by = "id_empleado") %>%
			  left_join(select (df_dispersion,id_empleado,num_cuenta,quincena), by = c("id_empleado","quincena")) %>%
                    left_join(select (df_cheques,ctrl_idx,id_empleado,num_cheque,estado), by = c("ctrl_idx","id_empleado")) %>%
                    left_join(select (df_cat_uni,id_universo,nombre_nomina), by = "id_universo") %>%
			  left_join(select (df_edocta_varios,ctrl_idx,nombre_nomina,n_conceptos), by = c("ctrl_idx","nombre_nomina")) %>%
			  left_join(select (df_cat_quin,quincena,fecha_ini,fecha_fin), by = "quincena")

      df_ho_repo <- df_honor %>%
           left_join(select (df_dispersion,id_empleado,num_cuenta,quincena), by = c("id_empleado","quincena")) %>%
           left_join(select (df_cheques,ctrl_idx,id_empleado,num_cheque,estado), by = c("ctrl_idx","id_empleado")) %>%
			  left_join(select (df_edocta_varios,ctrl_idx,nombre_nomina,n_conceptos), by = c("ctrl_idx","nombre_nomina")) %>%
			  left_join(select (df_cat_quin,quincena,fecha_ini,fecha_fin), by = "quincena")	

print (head(df_reporte))
print (head(df_ho_repo)) 
#   df_reporte <- rbind(df_reporte,df_ho_repo) 


	df_reporte <- df_reporte %>%
  		mutate(bcheque = "SIN REF") 

	#df_reporte <- df_reporte %>%
  	#	mutate(bcheque = ifelse (!is.na(df_reporte$num_cuenta),"TRF",df_reporte$b_cheque)) 

	df_reporte <- df_reporte %>%
  		mutate(refer_pago = trimws(num_cuenta))  #se hacer la referencia al numero de transferencia
	
	df_reporte$refer_pago <- ifelse(is.na(df_reporte$refer_pago), df_reporte$num_cheque, df_reporte$refer_pago)

	#df_reporte <- df_reporte %>%
  	#	mutate(bcheque = ifelse (is.na(df_reporte$refer_pago) , "SIN REFER", df_reporte$bcheque )) 

	df_reporte$bcheque <- ifelse(!is.na(df_reporte$num_cheque), "CHE" , df_reporte$bcheque)
	df_reporte$bcheque <- ifelse(!is.na(df_reporte$num_cuenta), "TRF" , df_reporte$bcheque)
	df_reporte$bcheque <- ifelse((!is.na(df_reporte$num_cuenta) & !is.na(df_reporte$num_cheque)), df_reporte$num_cheque , df_reporte$bcheque)

	df_reporte1 <- select (df_reporte,id_empleado,apellido_1,apellido_2,nombre,nombre_nomina,n_conceptos,bcheque,refer_pago,fecha_ini,fecha_fin,liquido,fec_pago) 


#--------------- HO
	df_ho_repo <- df_ho_repo %>%
  		mutate(bcheque = "SIN REF") 
	df_ho_repo <- df_ho_repo %>%
  		mutate(refer_pago = trimws(num_cuenta))  #se hacer la referencia al numero de transferencia
	
	df_ho_repo$refer_pago <- ifelse(is.na(df_ho_repo$refer_pago), df_reporte$num_cheque, df_ho_repo$refer_pago)
	df_ho_repo$bcheque <- ifelse(!is.na(df_ho_repo$num_cheque), "CHE" , df_ho_repo$bcheque)
	df_ho_repo$bcheque <- ifelse(!is.na(df_ho_repo$num_cuenta), "TRF" , df_ho_repo$bcheque)
	df_ho_repo$bcheque <- ifelse((!is.na(df_ho_repo$num_cuenta) & !is.na(df_ho_repo$num_cheque)), df_ho_repo$num_cheque , df_ho_repo$bcheque)

	df_reporte_ho <- select (df_ho_repo,id_empleado,apellido_1,apellido_2,nombre,nombre_nomina,n_conceptos,bcheque,refer_pago,fecha_ini,fecha_fin,liquido,fec_pago)   

	df_reporte1 <- rbind(df_reporte1,df_reporte_ho) 
	#df_reporte1 <- merge(df_reporte1, df_reporte_ho, by = "id_empleado", all = TRUE)

	df_reporte1 <- df_reporte1 %>% arrange(fec_pago,apellido_1)   

	write.csv(df_reporte1, file = "dataframe.csv", row.names = FALSE)

    if (dbIsValid (con)){   #Cerramos conexion a BD que se utilizo a traves de todos los modulos
       dbDisconnect(con)
    }   

   cerrar_log(file_conn)

print (df_edocta)
print (head(df_reporte1)) 

   }, error = function(e) {
            # Formatear el mensaje de error con la fecha y hora
            mensaje_error <- paste(Sys.time(), ": ", e$message, sep = "")
   
            escribir_log (file_conn, mensaje_error)

            if (dbIsValid (con)) {   #Cerramos base de datos
               if (flag_commit == TRUE) {
                  escribir_log (file_conn,"Se hara un rollback...")
                  dbRollback (con)  # en caso de error deshacemos la transaccion 
               }
               #dbDisconnect (con) #dejamos conexion abierta y el modulo principal la cierra
            }
            cerrar_log(file_conn)
            
            if (codigoerror == 0) {
               codigoerror = 700
            }     
            return (codigoerror) 
         }
   )
}