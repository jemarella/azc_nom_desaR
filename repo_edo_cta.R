


/* cheques pagados por quincena */
select 
bb.anio, bb.quincena, 
codigo, 
lpad (TO_CHAR (ROUND (SUM(aa.cargo)::numeric,2), 'FM999,999,999.00'),14,' ') as Cargo,
lpad (TO_CHAR (ROUND (SUM(aa.abono)::numeric,2), 'FM999,999,999.00'),14,' ') as Abono,
lpad (TO_CHAR (ROUND ((SUM(aa.cargo) - SUM(aa.abono))::numeric,2), 'FM999,999,999.00'),14,' ') as Abono,
from edocta as aa
join cat_quincena as bb on bb.anio = CAST (TO_CHAR (aa.fecha_val,'YYYY') as integer) and (aa.fecha_val BETWEEN bb.fecha_ini and bb.fecha_fin)
where TO_CHAR (aa.fecha_val ,'MM') = '01' and cheque <> 0
group by bb.anio, rollup(aa.codigo, bb.quincena)


select zz.quincena, 
case when zz.nombre_nomina = 'Compuesta' then ee.nombre_nomina else zz.nombre_nomina end as NOMINA,
count( bb.id_empleado), 
lpad (TO_CHAR (ROUND (sum(bb.liquido)::numeric,2), 'FM999,999,999.00'),14,' ') , 
lpad (TO_CHAR (ROUND (0::numeric,2), 'FM999,999,999.00'),14,' '), 
lpad (TO_CHAR (ROUND ((sum(bb.liquido) - 0)::numeric,2), 'FM999,999,999.00'),14,' ') as Diferencia
from nomina_idx as zz
join empleados_totales as bb on bb.ctrl_idx = zz.ctrl_idx 
/*join dispersion as cc on cc.id_empleado = bb.id_empleado and cc.anio = zz.anio and cc.real_quincena = zz.quincena*/
join empleados_nomina as dd on dd.id_empleado = bb.id_empleado
join cat_universo as ee on ee.id_universo = dd.id_universo
where zz.anio = 2024 and zz.quincena in ('01','02') and zz.reg_cancelado = FALSE and zz.carga_completa = TRUE
and NOT EXISTS (
    SELECT 1 FROM dispersion as aa
    WHERE aa.id_empleado = bb.id_empleado and aa.anio = zz.anio and aa.real_quincena = zz.quincena
) 
group by zz.quincena, rollup (NOMINA)
order by zz.quincena, NOMINA


select zz.quincena, 
case when zz.nombre_nomina = 'Compuesta' then ee.nombre_nomina else zz.nombre_nomina end as NOMINA,
count( bb.id_empleado), 
lpad (TO_CHAR (ROUND (sum(bb.liquido)::numeric,2), 'FM999,999,999.00'),14,' ') , 
lpad (TO_CHAR (ROUND (sum(cc.monto)::numeric,2), 'FM999,999,999.00'),14,' '), 
lpad (TO_CHAR (ROUND ((sum(bb.liquido) - sum(cc.monto))::numeric,2), 'FM999,999,999.00'),14,' ') as Diferencia
from nomina_idx as zz
join empleados_totales as bb on bb.ctrl_idx = zz.ctrl_idx 
join dispersion as cc on cc.id_empleado = bb.id_empleado and cc.anio = zz.anio and cc.real_quincena = zz.quincena
join empleados_nomina as dd on dd.id_empleado = bb.id_empleado
join cat_universo as ee on ee.id_universo = dd.id_universo
where zz.anio = 2024 and zz.quincena in ('01','02') and zz.reg_cancelado = FALSE and zz.carga_completa = TRUE
group by zz.quincena, rollup( NOMINA)
order by zz.quincena, NOMINA