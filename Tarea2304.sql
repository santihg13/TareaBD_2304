--establecemos un CTE para ejecutar el window function. 
--en este caso, aún no contemplamos los cambios mensuales, sino el cambio entre órdenes
with wf as(
	select o.customer_id, (od.quantity*od.unit_price) as total,
	lag (od.quantity*od.unit_price) over (partition by o.customer_id order by o.order_date) as prev
	from order_details od join orders o on (od.order_id=o.order_id)
)

select wf.customer_id, AVG(wf.total-wf.prev) as dif from wf 
	group by wf.customer_id order by wf.customer_id desc

	
-- versión 2
--generamos un string para relacionar a un cliente con un mes de cierto año.
-- Lo hacemos a través de un CTE 
with dates as (
select customer_id, concat(customer_id, extract(month from order_date),
	extract(year from order_date)) as str from orders o
) ,
--Usamos el resultado para agrupar las compras mensuales y sumarlas 
with sumas as(
select dates.str as str,sum(od.quantity*od.unit_price) as res from order_details od 
	join orders o ON (od.order_id=o.order_id) join 
	dates on (o.customer_id=dates.customer_id)
	group by dates.str
),

with calc as(
	select dates.customer_id, sumas.res as res, 
	lag(sumas.res over (partition by dates.customer_id order by dates.str) as prev
	from dates join sumas on (dates.str=sumas.str))

select dates.customer_id, AVG(sumas.res-calc.prev) as dif from 
	dates join sumas on (dates.str=sumas.str) join calc on (sumas.res=calc.res)
	
--me hice demasiadas bolas con los CTEs, creo que me salió contraproducente 
--no logré establecer la conexión entre calc y dates de manera clara 
	



