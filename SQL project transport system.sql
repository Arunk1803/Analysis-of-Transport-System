create database transport_system;
use transport_system;
create table routes (
route_id int primary key,
route_name varchar(50),
source varchar(50),
destination varchar(50)
);
create table trips (
trip_id int primary key,
route_id int,
trip_date date,
start_time time,
end_time time
);
create table tickets (
    ticket_id int primary key,
    trip_id int,
    fare int,
    ticket_time time
);
create table passenger_logs (
    log_id int primary key,
    trip_id int,
    passenger_count int,
    log_time time
);
insert into routes values
(1, 'route a', 'central', 'airport'),
(2, 'route b', 'central', 'tech park'),
(3, 'route c', 'railway station', 'market');
insert into trips values
(101, 1, '2024-01-10', '07:30:00', '08:30:00'),
(102, 1, '2024-01-10', '18:00:00', '19:00:00'),
(103, 2, '2024-01-10', '08:00:00', '09:00:00'),
(104, 2, '2024-01-10', '17:30:00', '18:30:00'),
(105, 3, '2024-01-10', '12:00:00', '13:00:00');
insert into tickets values
(1, 101, 40, '07:35:00'),
(2, 101, 40, '07:40:00'),
(3, 102, 40, '18:05:00'),
(4, 103, 30, '08:10:00'),
(5, 104, 30, '17:40:00'),
(6, 104, 30, '17:50:00'),
(7, 105, 20, '12:10:00');
insert into passenger_logs values
(1, 101, 45, '08:00:00'),
(2, 102, 60, '18:30:00'),
(3, 103, 55, '08:30:00'),
(4, 104, 70, '18:00:00'),
(5, 105, 20, '12:30:00');

     -- trip details with routr info--
select
r.route_name,
t.trip_id,
t.trip_date,
t.start_time,
t.end_time
from routes r
join trips t
on r.route_id = t.route_id;
       -- total passengers per trip--
select
t.trip_id,
sum(p.passenger_count) as total_passengers
from trips t
join passenger_logs p
on t.trip_id = p.trip_id
group by t.trip_id;
        -- tickets without passenger logs--
select
t.trip_id,
p.passenger_count
from trips t
left join passenger_logs p
on t.trip_id = p.trip_id
where p.passenger_count is null;
		  -- replace null--
select
trip_id,
ifnull(passenger_count, 0) as passenger_count
from passenger_logs;
            -- increase fare during evening peak--
update tickets tk
join trips t
on tk.trip_id = t.trip_id
set tk.fare = tk.fare + 5
where t.start_time between '17:00:00' and '19:00:00';
		   -- safe passenger update --
start transaction;

update passenger_logs
set passenger_count = passenger_count + 10
where trip_id = 101;

commit;
         
         -- case(passanger crowd level)--
select
trip_id,
passenger_count,
case
    when passenger_count >= 70 then 'high crowd'
    when passenger_count between 40 and 69 then 'medium crowd'
    else 'low crowd'
end as crowd_status
from passenger_logs;
           -- error--
delimiter $$

create procedure add_route_safe(
in p_id int,
in p_name varchar(50),
in p_source varchar(50),
in p_dest varchar(50)
)
begin
    declare exit handler for 1062
    begin
        select 'duplicate route id error' as message;
end;


insert into routes
values (p_id, p_name, p_source, p_dest);

    select 'route inserted successfully' as message;
    
end $$

delimiter ;
call add_route_safe(1, 'route a', 'central', 'airport');

          -- CTE and analytics
with route_data as (
    select
        t.trip_id,
        r.route_name,
        p.passenger_count
    from trips t
    join routes r on t.route_id = r.route_id
    join passenger_logs p on t.trip_id = p.trip_id
)
select
route_name,
trip_id,
passenger_count,
sum(passenger_count) over (partition by route_name) as total_route_passengers,
avg(passenger_count) over (partition by route_name) as avg_route_passengers
from route_data;
select*from routes;


