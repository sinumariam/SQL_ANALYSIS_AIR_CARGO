/*Air Cargo Analysis */

create database aircargo;
use aircargo;
show tables;
/* 2. Write a query to create route_details table using suitable data types for the fields, 
such as route_id, flight_num, origin_airport, destination_airport, aircraft_id, and 
distance_miles. Implement the check constraint for the flight number and unique 
constraint for the route_id fields. Also, make sure that the distance miles field is greater than 0.*/

create table route_details (
route_id int , flight_num int, origin_airport varchar(5), 
destination_airport varchar(5), aircraft_id varchar(15),
distance_miles int,
unique key (route_id),
constraint flight_num_check check ( flight_num between 1111 and 1160),
constraint distance_miles_check check(distance_miles>0) );

/*Write a query to display all the passengers (customers) who have travelled in routes 
01 to 25. Take data  from the passengers_on_flights table. */
select customer_id,route_id from passengers_on_flights where route_id between 01 and 25;

/* Write a query to identify the number of passengers and total revenue
 in business class from the ticket_details table */
 
 select count(*),sum(Price_per_ticket) from ticket_details where class_id='bussiness';
 
 /* Write a query to display the full name of the customer by extracting 
 the first name and last name from the customer table. */

 select concat(first_name," ",last_name) as name from customer ;
 
 /*Write a query to extract the customers who have registered
 and booked a ticket. Use data from the customer and ticket_details tables.. */
 
 select distinct ticket_details.customer_id, customer.first_name from ticket_details left join customer 
 on ticket_details.customer_id=customer.customer_id order by customer_id;
 
 /* Write a query to identify the customer’s first name and last name based on their 
 customer ID and brand (Emirates) from the ticket_details table. */
 
 select customer.first_name,customer.last_name,ticket_details.customer_id,ticket_details.brand
 from ticket_details join customer on ticket_details.customer_id=customer.customer_id
 where brand='Emirates';
 
 /* Write a query to identify the customers who have travelled by Economy Plus class using 
 Group By and Having clause on the passengers_on_flights table.. */

 select customer_id,class_id from passengers_on_flights group by class_id, customer_id
 having class_id='Economy Plus' ;

/*Write a query to identify whether the revenue has crossed 10000 using the IF clause 
on the ticket_details table. */

select (if(sum(Price_per_ticket)>10000, "yes, crossed 10000","not crossed 10000")) as revenue_is_crossing 
from ticket_details;

/*Write a query to create and grant access to a new user to perform operations on a database. */
Use aircargo;
create user 'user'@'localhost:3306' identified by '5757';
grant
 select,insert,update,drop,delete,index,create,alter 
 on aircargo.* to 'user'@'localhost:3306';
 
 grant all privileges on aircargo.* to 'user'@'localhost:3306';
 flush privileges;

 /*Query to find the maximum ticket price for each 
 class using window functions on the ticket_details table*/ 
 
 select distinct brand,max(price_per_ticket) over (partition by brand) as MAX_PRICE from ticket_details;
 
  /*Write a query to extract the passengers whose route ID is 4 by improving 
  the speed and performance of the passengers_on_flights table.*/ 
  create index index_for_rote_id on passengers_on_flights(route_id);
  show indexes from passengers_on_flights;
  select customer_id from passengers_on_flights where route_id=4;
  
  /* For the route ID 4, write a query to view the execution plan
  of the passengers_on_flights table.*/
  
  explain select * from passengers_on_flights where route_id=4;
  
  /* Write a query to calculate the total price of all tickets booked
  by a customer across different aircraft IDs using rollup function.*/
  
  select customer_id,aircraft_id,sum(Price_per_ticket) as total_price from ticket_details 
  group by customer_id , aircraft_id  with rollup;
  /* Write a query to create a stored procedure to get the details of all passengers flying 
  between a range of routes defined in run time. Also, return an error message if the table doesn't exist.*/
  DELIMITER $$

CREATE PROCEDURE get_passengers_by_route_range(
  IN p_start_route INT,
  IN p_end_route   INT
)
BEGIN
  -- 1. Check if the table exists in the current database
  IF NOT EXISTS (
    SELECT 1
    FROM information_schema.tables
    WHERE table_schema = DATABASE()
      AND table_name   = 'passengers_on_flights'
  ) THEN
    -- 2. If not, throw a user-defined error
    SIGNAL SQLSTATE '45000'
      SET MESSAGE_TEXT = 'Error: Table passengers_on_flights does not exist.';
  END IF;

  -- 3. If it exists, return all passengers with route_id in the given range
  SELECT *
  FROM passengers_on_flights
  WHERE route_id BETWEEN p_start_route AND p_end_route;
END$$

DELIMITER ;

CALL get_passengers_by_route_range(4, 10);

  /* Write a query to create a stored procedure that extracts all the details from the routes table where the
  travelled distance is more than 2000 miles. */
DELIMITER $$
create procedure routes_details()
Begin
select * from routes where Distance_miles > 2000;
END $$
DELIMITER ; 

call  routes_details();
  /*Write a query to create a stored procedure that groups the distance travelled by each
  flight into three categories. The categories are, short distance travel (SDT) for >=0 AND <= 2000 miles, 
  intermediate distance travel (IDT) for >2000 AND <=6500, and long-distance travel (LDT) for >6500. */
  
  DELIMITER $$
  create procedure dddd()

  Begin
	select flight_num,distance_miles,
    case
    when distance_miles between 0 and 2000 then 'SDT'
    when distance_miles>2000 and distance_miles<=2000 then 'IDT'
    when distance_miles<6500 then 'LDT'
    else 'Unknown'
    end as category from routes;
  
  End $$
  
DELIMITER ;
  
call dddd();

  /*Write a query to extract ticket purchase date, 
  customer ID, class ID and specify if the complimentary services are provided 
  for the specific class using a stored function in stored procedure on the ticket_details table.
Condition:
If the class is Business and Economy Plus, then complimentary services are given as Yes, else it is No */

-- stored function 
DELIMITER $$

create function complentary_services_fn(class_id varchar(20))
returns varchar(5)
deterministic

begin
declare complementary_services_available varchar(5);

if class_ID in ('Bussiness','Economy Plus') then set complementary_services_available='Yes';
else set complementary_services_available='No';
end if ;
return complementary_services_available;
end $$
DELIMITER ;

-- stored procedure using stored function
DELIMITER $$
create procedure complentary_services()
begin
select p_date,customer_ID,class_ID ,complentary_services_fn(class_id) from ticket_details;

end $$
DELIMITER ;

call complentary_services();

/*  Write a query to extract the first record of the customer whose last name ends with Scott using
 a cursor from the customer table.*/
 
 DELIMITER $$

CREATE PROCEDURE scott_customer()
BEGIN
  DECLARE done        INT DEFAULT FALSE;
  DECLARE v_id        INT;
  DECLARE v_first     VARCHAR(100);
  DECLARE v_last      VARCHAR(100);

  -- Declare a cursor over all customers whose last name ends with 'Scott'
  DECLARE cur_scott CURSOR FOR
    SELECT customer_id, first_name, last_name
    FROM customer
    WHERE last_name LIKE '%Scott'
    ORDER BY customer_id;  -- or any ordering you prefer

  -- Handler to set done flag when no more rows
  DECLARE CONTINUE HANDLER FOR NOT FOUND
    SET done = TRUE;

  -- Open the cursor and fetch the first matching row
  OPEN cur_scott;
  FETCH cur_scott INTO v_id, v_first, v_last;
  CLOSE cur_scott;

  -- Return the fetched row (or a message if none found)
  IF done THEN
    SELECT 'No customer found with last name ending in Scott' AS message;
  ELSE
    SELECT
      v_id   AS customer_id,
      v_first AS first_name,
      v_last  AS last_name;
  END IF;
END$$

DELIMITER ;
-- To invoke:
CALL scott_customer();