--Creating Soure Table
CREATE TABLE emp(  
    id int not null,
    name varchar(50) not null
);
--Creating Target Table
create table scd1 (  
    id int not null,
    name varchar(50) not null
);

--Inserting same data into source table
INSERT into emp values(1, 'John'),
(2, 'Jane'),
(3, 'Doe');

--Inserting same data into target table
INSERT into scd1 values(1, 'John'),
(2, 'Jane'),
(3, 'Doe');

--Selecting all records from the source table
select * from emp;
--Selecting all records from the target table
select * from scd1;

--Updating the name of the employee with id = 1 in the source table
update emp set name = 'John Doe' where id = 1;

--After updating rechecking again to check SCD1 implementation completed or not
select * from emp;
select * from scd1;

--Now adding new rows to source table
insert into emp values(4, 'Alice'),
(5, 'Bob');

--After adding new rows to source table, rechecking the records
select * from emp;  
--After implementing SCD1, we need to check if the target table is updated accordingly
select * from scd1;