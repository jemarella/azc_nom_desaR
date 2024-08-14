
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
