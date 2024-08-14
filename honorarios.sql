/* Este select es para obtener reporte de honorarios se debe proveer 
anio
quincena
nombre_nomina  , que siempre tendría que ser 'Honorarios'
*/


select to_char (zz.anio,'9999')::character varying as "ANIO",  zz.quincena as "QUINCENA", 
aa.unidad_administrativa, aa.nombre_empleado, aa.nombre_puesto, aa.fecha_pago, 
aa.concepto_1_id, aa.desc_concepto1 ,
lpad (TO_CHAR (ROUND (aa.valor_concepto_1::numeric,2), 'FM999,999,999.00'),14,' ') as "PERCEPCIONES",
concepto_2_id, desc_concepto2 ,
lpad (TO_CHAR (ROUND (aa.valor_concepto_2::numeric,2),'FM999,999,999.00'),14,' ') as "DEDUCCIONES",
lpad (TO_CHAR (ROUND (aa.liquido::numeric,2),'FM999,999,999.00'),14,' ') as "LIQUIDO" 
from nomina_ctrl as zz 
join honorarios as aa on aa.ctrl_idx = zz.idx
where zz.anio = 2024 and zz.quincena = '07' and nombre_nomina = 'Honorarios'
order by nombre_empleado