-- SECTION 1: Explore Data
SELECT *
FROM share_elec_ren;

SELECT *
FROM renewable_prod
WHERE entity LIKE 'w%';

SELECT DISTINCT (parameter)
FROM global_ev;

SELECT *
FROM global_sustainable_energy;

SELECT *
FROM global_ev
WHERE parameter = 'Electricity Demand' AND category = 'Historical';

SELECT *
FROM global_ev
WHERE parameter = 'EV stock share' AND category = 'Historical';
# EV Stock Share would be good to area graph combined with EV Sales share

SELECT *
FROM global_ev
WHERE parameter = 'EV sales share' AND category = 'Historical';

SELECT *
FROM global_ev
WHERE parameter = 'EV sales' AND category = 'Historical';

SELECT *
FROM global_ev
WHERE parameter = 'EV Stock' AND category = 'Historical';
#Make a tree graph from this data 

SELECT *
FROM global_ev
WHERE parameter = 'Oil displacement, million lge' AND category = 'Historical';

        
UPDATE global_ev 
SET region = 'United States'
WHERE region = 'USA';

SELECT *
FROM global_ev
WHERE parameter = 'Electricity demand' AND category = 'Historical';
-- filtered to historical to prevent duplications with predictions

SELECT 
    region, `mode`, SUM(`value`) AS Total_GWh
FROM
    global_ev
WHERE
    parameter = 'Electricity demand'
        AND category = 'Historical'
GROUP BY region , `mode`
ORDER BY `mode`; 



-- SECTION 2: Create table showing Demand Vs Generation with WINDOW FUNCTION 
#CTE, #Window Function # Inner Join

WITH El_D_DeDup AS 
	(
    SELECT region, `year`, SUM(`value`) OVER(PARTITION BY region, `year`) AS "Demand_GWh",
    ROW_NUMBER() OVER (
		PARTITION BY region, `year` ORDER BY `year`) 
        AS RN     
	FROM global_ev
	WHERE parameter = "Electricity demand" AND category = "Historical"
    )
SELECT 
    edd.region,
    edd.`year`,
    edd.Demand_GWh,
    (rp.`Electricity from wind (TWh)` + rp.`Electricity from hydro (TWh)` + rp.`Electricity from solar (TWh)` + rp.`Other renewables including bioenergy (TWh)`) AS 'Renew_Energy_Gen_GWh'
FROM
    El_D_DeDup edd
        JOIN
    renewable_prod rp ON edd.region = rp.Entity
        AND edd.`year` = rp.`year`
WHERE
    RN = 1;

CREATE TABLE `EV_Demand_and_Renew_Supply` (
    `region` TEXT,
    `year` INT,
    `Demand_GWh` INT,
    `Renew_Energy_Gen_GWh` INT
)  ENGINE=INNODB DEFAULT CHARSET=UTF8MB4 COLLATE = UTF8MB4_0900_AI_CI;

INSERT INTO EV_Demand_and_Renew_Supply 
	(
	WITH El_D_DeDup AS 
		(
		SELECT region, `year`, SUM(`value`) OVER(PARTITION BY region, `year`) AS "Demand_GWh",
		ROW_NUMBER() OVER (
			PARTITION BY region, `year` ORDER BY `year`) 
			AS RN     
		FROM global_ev
		WHERE parameter = "Electricity demand" AND category = "Historical"
		)
		SELECT 
		edd.region, 
		edd.`year`, 
		edd.Demand_GWh,  
		(rp.`Electricity from wind (TWh)` + rp.`Electricity from hydro (TWh)` + rp.`Electricity from solar (TWh)` + rp.`Other renewables including bioenergy (TWh)`)
		AS "Renew_Energy_Gen_GWh"
		FROM El_D_DeDup edd
		JOIN renewable_prod rp ON edd.region = rp.Entity AND edd.`year` = rp.`year`
		WHERE RN = 1
	)
;

SELECT 
    *
FROM
    EV_Demand_and_Renew_Supply;

-- SECTION 3: Create new table showing EV demand vs generation as a percentage with GROUP BY 
#CTE #Inner Join # Group by

WITH EV_Demand_SUM AS 
	(
	SELECT region, `year`, SUM(`value`) AS `value`
	FROM global_ev 
	WHERE parameter = "Electricity demand" AND category = "Historical"
	GROUP BY region, `year`
	),
	Total_Demand AS 
	(
	SELECT 
	evd.region, 
	evd.`year`, 
	evd.`value` AS Demand_GWh, 
	(rp.`Electricity from wind (TWh)` + rp.`Electricity from hydro (TWh)` + rp.`Electricity from solar (TWh)` + rp.`Other renewables including bioenergy (TWh)`) 
	AS "Renew_Energy_Gen"
	FROM EV_Demand_SUM evd
	JOIN renewable_prod rp ON evd.region = rp.Entity AND evd.`year` = rp.`year`
	)
SELECT region, Total_Demand.`year`, (Demand_GWh/Renew_Energy_Gen)*100 AS EV_Demand_Vs_Renew_Supply_Ratio
FROM Total_Demand;

CREATE TABLE `EV_Demand_Vs_Renew_Supply_Ratio` (
    `region` TEXT,
    `year` INT,
    `EV_Demand_Vs_Renew_Supply_Ratio` DOUBLE
)  ENGINE=INNODB DEFAULT CHARSET=UTF8MB4 COLLATE = UTF8MB4_0900_AI_CI;

# insert data into new table
INSERT INTO EV_Demand_Vs_Renew_Supply_Ratio
	(
		WITH EV_Demand_SUM AS 
			(
			SELECT region, `year`, SUM(`value`) AS `value`
			FROM global_ev 
			WHERE parameter = "Electricity demand" AND category = "Historical"
			GROUP BY region, `year`
			),
		Total_Demand AS 
			(
			SELECT 
				evd.region, 
				evd.`year`, 
				evd.`value` AS Demand_GWh, 
				(rp.`Electricity from wind (TWh)` + rp.`Electricity from hydro (TWh)` + rp.`Electricity from solar (TWh)` + rp.`Other renewables including bioenergy (TWh)`) 
				AS "Renew_Energy_Gen"
			FROM EV_Demand_SUM evd
			JOIN renewable_prod rp ON evd.region = rp.Entity AND evd.`year` = rp.`year`
			)
		SELECT region, Total_Demand.`year`, (Demand_GWh/Renew_Energy_Gen)*100 AS EV_Demand_Vs_Renew_Supply_Ratio
		FROM Total_Demand
	)
;

SELECT 
    *
FROM
    `EV_Demand_Vs_Renew_Supply_Ratio`;
    
   
   
 -- SECTION 4: EV Charging Points Per Region and Year
   
   
SELECT ev.region, ev.`year`, SUM(ev.`value`)
FROM global_ev ev
INNER JOIN EV_Demand_Vs_Renew_Supply_Ratio evr ON evr.region = ev.region AND evr.`year` = ev.`year`
WHERE parameter = 'EV charging points' AND category = 'Historical'
GROUP BY ev.region, ev.`year`;

CREATE TABLE `EV_Charging_Points` (
    `region` TEXT,
    `year` INT,
    `EV_Charging_Points` DOUBLE
)  ENGINE=INNODB DEFAULT CHARSET=UTF8MB4 COLLATE = UTF8MB4_0900_AI_CI;


INSERT INTO EV_Charging_Points
	(
	SELECT ev.region, ev.`year`, SUM(ev.`value`)
	FROM global_ev ev
	INNER JOIN EV_Demand_Vs_Renew_Supply_Ratio evr ON evr.region = ev.region AND evr.`year` = ev.`year`
	WHERE parameter = 'EV charging points' AND category = 'Historical'
	GROUP BY ev.region, ev.`year`
	)
;

SELECT *
FROM EV_Charging_Points;



-- Section 5: Data for powertrain treegraph
SELECT region, powertrain, `year`, `value`
FROM global_ev
WHERE parameter = 'EV Stock' AND category = 'Historical'
LIMIT 5000;
#Make a tree graph from this data 

SELECT DISTINCT powertrain
FROM global_ev
WHERE parameter = 'EV Stock' AND category = 'Historical'
LIMIT 5000;
#Make a tree graph from this data 


SELECT region, powertrain, `year`, SUM(`value`)
FROM global_ev
WHERE parameter = 'EV Stock' AND category = 'Historical'
GROUP BY region, `year`, powertrain;


-- Section 6: Data for EV Sales and Stock Share

SELECT region, `mode`, `year`, `value`
FROM global_ev
WHERE parameter = 'EV stock share' AND category = 'Historical';

SELECT region, `mode`, `year`, `value`
FROM global_ev
WHERE parameter = 'EV sales share' AND category = 'Historical';

WITH CTE_EV_sales_share AS
(
SELECT *
FROM global_ev
WHERE parameter = 'EV sales share' AND category = 'Historical'
),
 CTE_EV_stock_share AS
(
SELECT *
FROM global_ev
WHERE parameter = 'EV stock share' AND category = 'Historical' 
)
SELECT *
FROM CTE_EV_sales_share
LEFT JOIN CTE_EV_stock_share ON CTE_EV_sales_share.region = CTE_EV_stock_share.region AND CTE_EV_sales_share.`mode` = CTE_EV_stock_share.`mode` AND CTE_EV_sales_share.`year` = CTE_EV_stock_share.`year`;  





