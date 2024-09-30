
carga_relacion_edocta <- function (ianio,con)
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

      #result_bd = abrir_BD() 
      #con = result_bd$con_bd
      #codigoerror = result_bd$codigoerror

      if (!dbIsValid (con)) {
     	   escribir_log (file_conn,"Conexion BD invalida")
	      val_return = -1
         codigoerror = 715
         stop ("Conexion DB invalida")
      }
      root_dir = checkini$Directory$drootedo

      s_qry = paste ("select zz.ctrl_idx, aa.fec_pago, ", 
                     "case when zz.nombre_nomina = 'Compuesta' then cc.nombre_nomina else zz.nombre_nomina end as NOMINA, ",
                     "ROUND (sum(aa.liquido)::numeric,2) as liquido ",
                     "from nomina_idx as zz ",
                     "join empleados_totales as aa on aa.ctrl_idx = zz.ctrl_idx ", 
                     "join empleados_nomina as bb on bb.id_empleado = aa.id_empleado ",
                     "join cat_universo as cc on cc.id_universo = bb.id_universo ",
                     "where zz.anio = '%s' and zz.reg_cancelado = FALSE and zz.carga_completa = TRUE ",
                     "group by zz.ctrl_idx, aa.fec_pago, NOMINA ",
                     "order by zz.ctrl_idx, NOMINA ")

      s_qry_ho = paste ("select zz.ctrl_idx, aa.fecha_pago as fec_pago, " ,  
                     "aa.subprograma as NOMINA, ",
                     "ROUND (sum(aa.liquido)::numeric,2) as liquido ", 
                     "from nomina_idx as zz ",
                     "join honorarios as aa on aa.ctrl_idx = zz.ctrl_idx ", 
                     "where zz.anio = '%s' and zz.reg_cancelado = FALSE and zz.carga_completa = TRUE ",
                     "group by zz.ctrl_idx, aa.fecha_pago, NOMINA ",
                     "order by zz.ctrl_idx, aa.subprograma ")              

      s_qry = sprintf (s_qry,ianio)
      df_totales_nom <- dbGetQuery(con, s_qry)
      
      s_qry_ho = sprintf (s_qry_ho,ianio)
      df_totales_ho <- dbGetQuery(con, s_qry_ho)
      
      df_totales_nom = rbind ( df_totales_nom,df_totales_ho)  #Juntamos honorarios
      dbBegin (con)
         flag_commit = TRUE
         for (irow in 1:nrow(df_totales_nom)) {
            #df_totales_nom$anio[irow]
            fecha_pago = df_totales_nom$fec_pago[irow]
            vliquido = df_totales_nom$liquido[irow]
            vctrlidx = df_totales_nom$ctrl_idx[irow] 
            vnomina = trimws(df_totales_nom$nomina[irow])

      #SE ELIMINARA LA REFERENCIA DE CODIGO = '213' O '548' DEBIDO A QUE LOS ESTADOS DE CUENTA DIARIOS NO CONTIENEN ESE CAMPO
      #ESE CAMPO SOLO VIENE EN LOS ESTADOS DE CUENTA AL FINAL DE CADA MES Y NO NOS DARIA TIEMPO DE IR CUADRANDO POR SEMANA
      #DEBIDO A QUE NO TENEMOS CODIGO AGREGAMOS 'cheque = 0' PARA REDUCIR LA CANTIDAD DE REGISTROS DONDE SE HACE LA BUSQUEDA 

            if (vnomina == "AUTOGENERADOS") {
               #s_qry2 = paste ("update edocta set ctrl_idx = %s, nombre_nomina = '%s' ",
               #               "where codigo = '213' ", 
               #               "and to_char (fecha_val,'MM') = to_char (TO_DATE ('%s','YYYY-MM-DD'),'MM') and ctrl_idx = 0 ", 
               #               "and abono = %s ") 
               s_qry2 = paste ("update edocta set ctrl_idx = %s, nombre_nomina = '%s' ",
                              "WHERE fecha_val between TO_DATE ('%s','YYYY-MM-DD')  - interval '15 day'  and  TO_DATE('%s','YYYY-MM-DD') + interval '5 day' ", 
                              "and ctrl_idx = 0 ",
                              "and cheque = 0 and abono = %s ") 
               s_qry2 = sprintf (s_qry2,vctrlidx,vnomina,fecha_pago,fecha_pago,vliquido)
            } else {
               #s_qry2 = paste ("update edocta set ctrl_idx = %s, nombre_nomina = '%s' ",
               #               "where codigo = '548' ", 
               #               "and to_char (fecha_val,'MM') = to_char (TO_DATE ('%s','YYYY-MM-DD'),'MM') and ctrl_idx = 0 ", 
               #               "and abono = %s ") 
               s_qry2 = paste ("update edocta set ctrl_idx = %s, nombre_nomina = '%s' ",
                              "WHERE fecha_val between TO_DATE ('%s','YYYY-MM-DD')  - interval '15 day'  and  TO_DATE('%s','YYYY-MM-DD') + interval '5 day' ", 
                              "and ctrl_idx = 0 ",
                              "and cheque = 0 and abono = %s ") 
               # con intervalo de 5 días damos una ventana para que llegue el recurso, como ejemplo la nomina honorarios de Enero31 y el recurso llego 2Feb 
               s_qry2 = sprintf (s_qry2,vctrlidx,vnomina,fecha_pago,fecha_pago,vliquido)
            }

            escribir_log (file_conn, paste ("Se hara update con este query ", s_qry2))
            dbExecute(con, s_qry2)
         }
      dbCommit (con) 
      flag_commit = FALSE

      escribir_log (file_conn,paste ("Proceso carga relacion estado cuenta finalizado...", sep = " "))
	   cerrar_log (file_conn) 

      #if (dbIsValid (con)) {   ##dejamos conexion abierta y el modulo principal la cierra
      #   dbDisconnect (con)
      #}

      return (codigoerror) 

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
