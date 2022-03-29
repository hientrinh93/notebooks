/* SQL TEST TASK - postgreSQL 14 */
/* --------------------------------------------------------------------------------- 
-- TASK 1: Load Dataset into PostgreSQL using FOREIGN TABLE as topups table
--------------------------------------------------------------------------------- */
/* create a log */

/* install file_fdw as an extension */
/* file_fdw module provides the foreign-data wrapper file_fdw,
A foreign table created using the wrapper file_fdw */
CREATE EXTENSION file_fdw;

/* create a foreign server named topups */
CREATE SERVER topups FOREIGN DATA WRAPPER file_fdw;

/* create a foreign table */
CREATE FOREIGN TABLE foreign_topups(
    seq varchar(20),
    id_user varchar(20),
    topup_date varchar(20),
    topup_value varchar(20))
    SERVER topups
    OPTIONS (format 'csv', header 'true', filename 'C:\Users\hient\OneDrive - National University of Ireland, Galway\Personal documents\Job\Sonra\topups.tsv', delimiter E'\t');

/* load as topups table */
CREATE TABLE topups AS SELECT * FROM foreign_topups;
SELECT * FROM topups;

/* change data type for each column */
ALTER TABLE topups
	ALTER COLUMN seq TYPE INTEGER USING (seq::INTEGER),
	ALTER COLUMN id_user TYPE INTEGER USING (id_user::INTEGER),
	ALTER COLUMN topup_date TYPE DATE USING to_date(topup_date, 'YYYY-MM-DD'),
	ALTER COLUMN topup_value TYPE INTEGER USING (topup_value::INTEGER);

/* --------------------------------------------------------------------------------- 
-- TASK 2: 
Can this be solved in a single SELECT statement, without Window Aggregates or subqueries? 
Answer: It would not be a good idead if we declare the variable and use the loop (while)
--------------------------------------------------------------------------------- */
SELECT id_user, SUM(topup_value) AS total_topup FROM topups 
WHERE id_user IN (
	SELECT id_user FROM topups WHERE topup_value = 15 GROUP BY id_user HAVING COUNT(id_user) >= 1 
)
GROUP BY id_user;

/* --------------------------------------------------------------------------------- 
-- TASK 3: 
--------------------------------------------------------------------------------- */
SELECT * FROM (
    SELECT *, 
			ROW_NUMBER() OVER (PARTITION BY id_user ORDER BY topup_date DESC, topup_value DESC) AS date_rank           
	FROM topups
) RANKS WHERE date_rank <= 5;

/* --------------------------------------------------------------------------------- 
-- TASK 4: 
--------------------------------------------------------------------------------- */
SELECT * FROM (
    SELECT *, RANK() over (PARTITION BY id_user ORDER BY topup_value DESC) AS rrank           
	FROM topups
) RANKS WHERE rrank <= 5;

/* --------------------------------------------------------------------------------- 
-- TASK 5: 
--------------------------------------------------------------------------------- */
/* create a temporary table tem_task5 to avoid multi-subqueries */
/* save to the final table: task5 */
CREATE TABLE task5 AS
WITH tem_task5 AS (SELECT t1.id_user, t1.topup_date, t1.topup_value, /* tem_task5: CTE */
		t1.prv_topup_dt, t1.promo_ind, t3.previous_qual_topup_dt,
		t1."topup_date"::date - t1."prv_topup_dt"::date AS days_since,
		ROUND (t1.topup_value * 1. / t2.min_topup, 1) AS to_1st_ratio FROM (
	SELECT *,
        LEAD(topup_date) OVER (PARTITION BY id_user ORDER BY topup_date DESC) AS prv_topup_dt,
		CASE WHEN topup_value >= 20 THEN 'Y' ELSE 'N' END AS promo_ind
		FROM topups) t1
		
LEFT JOIN (
	SELECT DISTINCT ON (id_user) id_user, topup_value AS min_topup  
	/* change t2.topup_value to avoid the same name t1.topup_value */ 
	FROM topups ORDER BY id_user, topup_date) t2 
ON t1.id_user = t2.id_user

LEFT JOIN (
	SELECT id_user, topup_date AS previous_qual_topup_dt 
	FROM topups WHERE topup_value >= 20 ORDER BY id_user, topup_date DESC) t3
ON t1.id_user = t3.id_user 
AND t1.topup_date > t3.previous_qual_topup_dt)

SELECT id_user, topup_date, topup_value, 
		prv_topup_dt, days_since, promo_ind, 
		MAX(previous_qual_topup_dt) AS previous_qual_topup_dt, to_1st_ratio FROM tem_task5  
GROUP BY id_user, topup_date, topup_value, 
		prv_topup_dt, days_since, promo_ind, to_1st_ratio
ORDER BY id_user, topup_date DESC;

-- Second method:
/*SELECT id_user, topup_date, topup_value, 
		prv_topup_dt, days_since, promo_ind, 
		previous_qual_topup_dt, to_1st_ratio FROM (
		SELECT *, 
				ROW_NUMBER() over (
					PARTITION BY id_user, topup_date ORDER BY topup_date DESC, previous_qual_topup_dt DESC) 
				AS prerank FROM tem_task5
) RANKS WHERE prerank = 1; */

/* view table task 5 */
SELECT * FROM task5;

/* --------------------------------------------------------------------------------- 
-- TASK 6: 
--------------------------------------------------------------------------------- */
/* create a table to be queried */ 
CREATE TABLE tem1_task6 AS SELECT id_user, topup_date AS promo_start, topup_date + 28 AS promo_end 
FROM task5 WHERE topup_value >= 20 ORDER BY id_user, topup_date;

SELECT * FROM tem1_task6;

/* save to the final table: task6 */
CREATE TABLE task6 AS WITH tem2_task6 AS (SELECT t1.id_user, t1.promo_start, t2.promo_end FROM tem1_task6 AS t1
										  
LEFT JOIN tem1_task6 AS t2  
ON t1.promo_end <= t2.promo_end
AND t1.id_user = t2.id_user
										  
INNER JOIN tem1_task6 AS t3      
ON t1.id_user = t3.id_user 
GROUP BY t1.id_user, t1.promo_start, t2.promo_end 
HAVING COUNT(CASE WHEN (t3.promo_start < t1.promo_start AND t1.promo_start <= t3.promo_end)  
                        OR (t3.promo_start <= t2.promo_end AND t2.promo_end < t3.promo_end) THEN 1 END) = 0)

SELECT id_user, promo_start, MIN(promo_end) AS promo_end FROM tem2_task6  
GROUP BY id_user, promo_start;

/* view table task 6 */
SELECT * FROM task6;


/* --------------------------------------------------------------------------------- 
-----------------------------------FOR BLOGS----------------------------------------
--------------------------------------------------------------------------------- */
-- sum1.sql
SELECT *, SUM(topup_value) OVER (PARTITION BY id_user ORDER BY id_user, topup_date) AS tot_topup FROM topups;

-- sum2.sql
SELECT id_user, SUM(topup_value) AS tot_topup FROM topups GROUP BY id_user ORDER BY id_user;



