-- ---------------------------------------------------------------------------------------------------------------------------------------------
-- ---------------------------------------------------------------------------------------------------------------------------------------------
-- ---------------------------------------------------------------------------------------------------------------------------------------------
-- ---------------------------------------------------------------------------------------------------------------------------------------------

-- 1. How many countries are captured in [owid_energy_data]?
-- The country field contains continents and other groups of countries. Thus, need to exclude them in the count to prevent double counting. 
-- eg: Africa (OWID_AFR), Antartica (blank iso code), Asia Pacific (blank iso code), CIS (blank iso code), Czechoslovakia (blank iso code)  
select distinct(country), iso_code
from owid_energy_data;

-- Verify if all countries with blank iso_code should be excluded:
-- Greenland is a country but have blank iso_code, thus we need specifically include them. After evaluating records on Greenland, some records have the "GRL" iso_code while others are blank. Thus, there is not need to specifically include them again or else there will be double counting. 
-- Pertaining to Kosovo, it is a partially recognised country and thus will be excluded in the count. 
select distinct(country), iso_code
from owid_energy_data
where iso_code like "";

select country, iso_code
from owid_energy_data 
where country = "Greenland";

select country, iso_code
from owid_energy_data 
where country = "Kosovo";

-- Final output: 217 countries
select count(distinct(country))
from owid_energy_data
where iso_code != "" and 
iso_code not like "OWID%";

-- ---------------------------------------------------------------------------------------------------------------------------------------------
-- ---------------------------------------------------------------------------------------------------------------------------------------------
-- ---------------------------------------------------------------------------------------------------------------------------------------------
-- ---------------------------------------------------------------------------------------------------------------------------------------------

-- 2. Find the earliest and latest year in [owid_energy_data]. What are the countries having a record in <owid_energy_data> every year throughout the entire period (from the earliest year to the latest year)? 

-- Changing the datatype of the year attribute to integer in owid_energy_data table:
alter table owid_energy_data
modify column year INT;

-- Creating a view containing countries as identified in Q1.
CREATE VIEW COUNTRIES_ONLY AS
select * from owid_energy_data
where iso_code !="" and iso_code not like "%OWID%";

-- output: Earliest year: 1900, Latest year: 2021
SELECT min(year), max(year) 
FROM COUNTRIES_ONLY;

SELECT iso_code, country, count(*) 
FROM COUNTRIES_ONLY
GROUP BY iso_code, COUNTRY
HAVING min(year) = (select min(year) from countries_only)
AND max(year) = ( select max(year) from countries_only)
and count(*) = max(year)-min(year)+1;



-- 3. Specific to Singapore, in which year does <fossil_share_energy> stop being the full source of energy (i.e., <100)? Accordingly, show the new sources of energy.

-- There are many sources of energy such as: Biofuel, Coal, Fossil fuel (Sum of Coal,Oil and Gas), Natural gas, Hydro power, Low carbon (Sum of Renewables and Nuclear), Nuclear, Oil, Other renewables, Renewables, Solar, Wind
-- Since fossil fuel is a sum of coal, oil and gas, we should exclude the individual coal, oil and natural gas columns to prevent double counting. 
-- Since low carbon is a sum of renewables and nuclear, we should exclude low carbon column to prevent double counting and to observe if there is change in share energy for nuclear and renewables separately.


-- Output: in 1986, fossil_share_energy stop being the full source of energy
-- Aside from fossil fuel which contributes 99.857% of energy, the remaining 0.143% is contributed by renewables/ other renewables. In the code book for the dataset, the composition of the 2 columns are not clearly stated. 
-- Since the total share should add up to 100%, our team assumes that the Other renewables column is a subset of the Renewables column.
-- Thus, the alterative source of energy in Singapore is "Renewables" 

select country,year, biofuel_share_energy,fossil_share_energy,hydro_share_energy,nuclear_share_energy,other_renewables_share_elec_exc_biofuel,other_renewables_share_energy,renewables_share_energy,solar_share_energy,wind_share_energy
from owid_energy_data
where country = "Singapore"
and fossil_share_energy !=100
order by year;

select country, biofuel_share_energy,fossil_share_energy,hydro_share_energy,nuclear_share_energy,other_renewables_share_elec_exc_biofuel,other_renewables_share_energy,renewables_share_energy,solar_share_energy,wind_share_energy
from owid_energy_data
where country = "Singapore" and 
year in (
	select min(year)
	from owid_energy_data
	where country = "Singapore" and 
	fossil_share_energy != 100);



-- 4. 
-- Creating a view containing only records from 2000 to 2021 for ASEAN countries
create view asean_countries as
select * 
from owid_energy_data
where country in ("Brunei","Cambodia", "Indonesia", "Laos", "Malaysia","Myanmar", "Philippines","Singapore","Thailand","Vietnam")
and year >= 2000 and year <= 2021;

-- There are countries with blank values in the gdp attribute, eg: Brunei, Cambodia(2019,2020),Myanmar(2019,2020) etc 
-- Since replacing the blank values will significantly affect the average gdp computation, the blank values will be excluded and ignored.
-- Additionally, for countries like Brunei where gdp field is entirely blank, stating that their gdp value is 0 is an ill representation of the country. In this case, Brunei is removed from the list.
select country,year,gdp
from asean_countries;

select country, cast(avg(gdp) as decimal(25,5)) as AverageGDP
from asean_countries
where gdp <> ""
group by country
order by AverageGDP desc;



-- Finding the oil_consumption moving average:

-- Creating a view that of data of ASEAN countries from 2000-2021 and numbering the records of each country (rn)
create view moving_avg as (
select country, year,oil_consumption,gdp,row_number() over (partition by country order by year asc) as rn
from asean_countries)
;

-- First nested select statement: calculating the moving average for records that have data from all 3 years [thus, starting from the 3rd earliest year]
-- Second nested select statement: using lag() function to create column of moving average from previous year and finding the difference between the 2 moving average values.
-- Third nested select statement: selecting records that have negative change
-- Creating a view from the output so that it can be used to identify years for gdp moving average calculation later.
create view oilconsumption_ma as(
select *
from(
select *,lag(_3yearmovingavg) over (partition by country
											order by country, year asc) as prevyear_movingavg, 
                                            (_3yearmovingavg- (lag(_3yearmovingavg) over (partition by country order by country, year asc))) as diff
from(
select country, year, oil_consumption,
case 
when rn>=3 then AVG(oil_consumption) OVER (PARTITION BY country 
									ORDER BY year asc ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)
else NULL
END as _3yearmovingavg
from moving_avg
where oil_consumption != "") as t1) as t2 
where diff <0)
;

-- Viewing the output of codes above:
select * 
from oilconsumption_ma;


-- Finding the gdp moving average:
select * 
from (
select *,lag(_3yearmovingavg) over (partition by country
											order by country, year asc) as prevyear_movingavg
from(
select country, year, gdp,
case 
when rn>=3 then AVG(gdp) OVER (PARTITION BY country 
									ORDER BY year asc ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)
else NULL
END as _3yearmovingavg
from moving_avg
where gdp != "") as t1) as t2
where (country,year) in (
select country, year 
from oilconsumption_ma);


 
-- Changing the datatype of year attribute to integer:
alter table exportsofenergyproducts
modify column year INT;

alter table importsofenergyproducts
modify column year INT;

-- Changing the datatype of value_ktoe attribute to decimal:
alter table exportsofenergyproducts
modify column value_ktoe decimal(10,2);

alter table importsofenergyproducts
modify column value_ktoe decimal(10,2);

-- 12 products are imported while only 9 products are exported. 
-- Need to include the value_ktoe of products that are only imported and not exported.
select distinct(sub_products) 
from exportsofenergyproducts;

select distinct(sub_products) 
from importsofenergyproducts;

-- Since there all exported items are also imported but some imported items are not exported, left join should be used where primary table is the importsofenergyproducts.
select imp.energy_products, imp.sub_products, avg(imp.value_ktoe) as imp_valuektoe, avg(exp.value_ktoe) as exp_valuektoe
from importsofenergyproducts imp left join exportsofenergyproducts exp
on exp.sub_products = imp.sub_products
and exp.energy_products = imp.energy_products
and exp.year = imp.year
group by imp.sub_products,imp.energy_products ;


-- QYESTION 7
​​
-- ref about replacing null with 0 using coalesce: https://stackoverflow.com/questions/16840522/replacing-null-with-0-in-a-sql-server-query
-- Creating a view containing all import and exports information
create view netimport as(
select imp.year, imp.energy_products, imp.sub_products,imp.value_ktoe as imp_valuektoe , coalesce(exp.value_ktoe,0) as exp_valuektoe
from importsofenergyproducts imp left join exportsofenergyproducts exp
on exp.sub_products = imp.sub_products
and exp.energy_products = imp.energy_products
and exp.year = imp.year);

-- Output: 2014 has 5 instances of export value > import value
select year, count(*) 
from(
select *, (imp_valuektoe - exp_valuektoe) as difference 
from netimport
where (imp_valuektoe - exp_valuektoe)<0 ) as t1
group by year
having count(*) > 4;

-- QUESTION 9
-- There are some records with kwh_per_acc value as "s". 
-- Since this value does not seem to have any meaning, the records with these values are ignored and excluded in the computation of the average kwh_acc.
-- ref about converting datatype of year and month attribute to integer and kwh_per_acc attribute to decimal: https://stackoverflow.com/questions/12126991/cast-from-varchar-to-int-mysql
-- Creating a view which excludes the records with "s\r" in kwh_per_acc:
select * 
from householdelectricityconsumption
where kwh_per_acc like "s\r";

-- In the month field, there is "annual" which will result in double counting of the kwh_per_acc value 
-- Need to exclude this during the computation.
select distinct(month) 
from householdelectricityconsumption;

-- In the dwelling_type field, there is "overall" which will result in double counting of the kwh_per_acc value 
-- Need to exclude this during computation:
select distinct(dwelling_type)
from householdelectricityconsumption;

create view householdelectricity as
select dwelling_type,cast(year as unsigned) as Year, cast(month as unsigned) as Month, Region, Description, cast(kwh_per_acc as decimal(7,3)) as kwh_per_acc
from householdelectricityconsumption
where kwh_per_acc not like "s\r"
and Month != "Annual"
and dwelling_type != "Overall";

select year, Region, sum(kwh_per_acc)
from householdelectricity
where Region not like "Overall"
group by year, Region
;

-- QUESTION 10
select region, year, quarter_number, avg(kwh_per_acc) as avg_kwh_per_acc
from (
	select region, 
	year, 
	cast(month as double) as month, 
	floor((cast(month as unsigned)-1)/3)+1 as quarter_number,
	cast(kwh_per_acc as double) as kwh_per_acc
	from householdelectricityconsumption
	where region != "Overall" 
	and month != 0
	and kwh_per_acc !=0
) as t1
where kwh_per_acc != 0 
group by year, region, quarter_number
order by region, year, quarter_number;


-- QUESTION 11
select cast(year as unsigned) as year, 
    cast(month as unsigned) as month,
        avg(cast(avg_mthly_hh_tg_consp_kwh as unsigned)) as quarterly_average,
        floor((cast(month as unsigned)-1)/3)+1 as quarter_number,
        housing_type,
        sub_housing_type
from householdtowngasconsumption
where sub_housing_type!="Overall" and month!=0
group by  year, sub_housing_type, quarter_number
order by sub_housing_Type, year, month;




-- QUESTION 12


CREATE VIEW Ireland_ENERGY_CONSUMPTION AS
SELECT country, YEAR, population, gdp, energy_per_capita, energy_per_gdp, 
(energy_per_capita - LAG(energy_per_capita) OVER( order by year)) *100/ energy_per_capita as 'Δ%Energy_per_capita',
(energy_per_gdp - LAG(energy_per_gdp) OVER( order by year)) *100/ energy_per_gdp as 'Δ%Energy_per_gdp',
coal_consumption,
coal_cons_per_capita,
gas_consumption,
(gas_consumption/population)* 1000000000 as gas_cons_per_capita,
oil_consumption,
(oil_consumption/population)*1000000000 as oil_cons_per_capita,
solar_consumption,
(solar_consumption/population)*1000000000 as solar_cons_per_capita
from countries_only
where country = "Ireland";


CREATE VIEW Ireland_ENERGY_CONSUMPTION AS
SELECT country, YEAR, population, gdp, energy_per_capita, energy_per_gdp, 
(energy_per_capita - LAG(energy_per_capita) OVER( order by year)) *100/ energy_per_capita as 'Δ%Energy_per_capita',
(energy_per_gdp - LAG(energy_per_gdp) OVER( order by year)) *100/ energy_per_gdp as 'Δ%Energy_per_gdp',
coal_consumption,
coal_cons_per_capita,
gas_consumption,
(gas_consumption/population)* 1000000000 as gas_cons_per_capita,
oil_consumption,
(oil_consumption/population)*1000000000 as oil_cons_per_capita,
solar_consumption,
(solar_consumption/population)*1000000000 as solar_cons_per_capita
from countries_only
where country = "Singapore";


CREATE VIEW Ireland_ENERGY_CONSUMPTION AS
SELECT country, YEAR, population, gdp, energy_per_capita, energy_per_gdp, 
(energy_per_capita - LAG(energy_per_capita) OVER( order by year)) *100/ energy_per_capita as 'Δ%Energy_per_capita',
(energy_per_gdp - LAG(energy_per_gdp) OVER( order by year)) *100/ energy_per_gdp as 'Δ%Energy_per_gdp',
coal_consumption,
coal_cons_per_capita,
gas_consumption,
(gas_consumption/population)* 1000000000 as gas_cons_per_capita,
oil_consumption,
(oil_consumption/population)*1000000000 as oil_cons_per_capita,
solar_consumption,
(solar_consumption/population)*1000000000 as solar_cons_per_capita
from countries_only
where country = "Luxembourg";

select *
from singapore_energy_consumption, luxembourg_energy_consumption, ireland_energy_consumption
where singapore_energy_consumption.year = luxembourg_energy_consumption.year
and singapore_energy_consumption.year = ireland_energy_consumption.year;
	

-- QUESTION 13

select country, avg(change_in_gdp) from(
select country, year, renewables_share_energy, ((gdp/population) - LAG(gdp/population) OVER( order by  country, year)) *100/ (gdp/population) as 'change_in_gdp'
from 
(select *,  (cast(gdp as double)/cast(population as double)) as gdp_per_capita
from countries_only where 
year>=2000 and
country in (( select * from (
select country from countries_only
where year>=2000
group by country
order by avg(cast(renewables_share_energy as double)) DESC
limit 10) as t1 order by country))
UNION
select *,  (cast(gdp as double)/cast(population as double)) as gdp_per_capita
from countries_only where 
year>=2000 and
country in (( select * from (
select country from countries_only
where year>=2000 and renewables_share_energy<1
group by country
order by avg(cast(carbon_intensity_elec as double)) DESC
limit 10) as t2)) order by country, year) as final_table) as ft
group by country;



-- using external data set relating to renewables and their jobs
-- creating table to import new datset
CREATE TABLE `country_renewables_employment` (
  `Country` text,
  `Technology` text,
  `Jobs (thousand)` double DEFAULT NULL,
  `Source` text,
  `Notes` text,
  `Year` int DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci

-- import the data using "Table Data Import Wizard" and importing the csv 'countries.csv' file.



-- Hydropower countries
select country, floor(((sum(`Jobs (thousand)`)*1000))) as jobs from country_renewables_employment
where country!="All World"
and technology="Hydropower" 
group by country
order by `Jobs (thousand)` desc;

-- Wind Energy Jobs
select country, floor(((sum(`Jobs (thousand)`)*1000))) as jobs from country_renewables_employment
where country!="All World"
and technology="Wind Energy"
group by country
order by `Jobs (thousand)` desc;

select country, floor(((sum(`Jobs (thousand)`)*1000))) as jobs from country_renewables_employment
where country!="All World"
and technology like "%Solar%"
group by country
order by `Jobs (thousand)` desc;



-- QUESTION 14

-- primary energy consumption (total energy demand of the country)
-- figure 14.1
-- blank value for 2021 so evaluation is done for years before 2020 (inclusive)

select year, primary_energy_consumption
from owid_energy_data
where country = "Singapore"
and year <= 2020;

-- no energy generated from wind,nuclear,hydro and biofuel
-- blank values for 2021 so evaluation is done for years before 2020 (inclusive)
select year, biofuel_share_energy,fossil_share_energy,hydro_share_energy,nuclear_share_energy,other_renewables_share_elec_exc_biofuel,other_renewables_share_energy,renewables_share_energy,solar_share_energy,wind_share_energy
from owid_energy_data
where country = "Singapore"
and year <= 2020;

-- evaluate change in energy sources aside from fossil fuels:
-- blank values for 2021 so evaluation is done for years before 2020 (inclusive)
-- figure 14.2
select year,other_renewables_share_energy,renewables_share_energy,solar_share_energy
from owid_energy_data
where country = "Singapore"
and year <= 2020;

-- evaluate change in usage of fossil fuels:
-- blank values for 2021 so evaluation is done for years before 2020 (inclusive)
-- figure 14.3
select year,fossil_share_energy
from owid_energy_data
where country = "Singapore"
and year <= 2020;

-- how much of Singapore's energy demand (in TWH) is produced by solar energy:
-- figure 14.4
select year, primary_energy_consumption,solar_share_energy
from owid_energy_data
where country = "Singapore"
and year <= 2020;

-- Consider the change in amount of imported energy products if coal is replaced with nuclear fuel
-- yearly amount of imported coal 
-- figure 14.5
select * 
from importsofenergyproducts
where energy_products = "Coal and Peat";

-- total amount of imported energy product in 2020
-- output: 151,230.90ktoe
-- figure 14.6
select sum(value_ktoe) as total
from importsofenergyproducts
where year = 2020;


-- QUESTION 15

-- identifying the different categories of ghg in the dataset:
-- output: 10 different categories
-- the category "greenhouse_gas_ghgs_emissions_including_indirect_co2_without_lulucf_in_kilotonne_co2_equivalent" is the column containing the summed amount of all the other ghg category values
-- "greenhouse_gas_ghgs_emissions_including_indirect_co2_without_lulucf_in_kilotonne_co2_equivalent" column will be used to evaluation
select distinct(category)
from greenhouse_gas_inventory_data_data;

-- Computing the yearly GHG emissions and cumulative emissions
-- figure 15.1: average yearly GHG emissions
-- figure 15.2: cumulative GHG emissions
-- output: yearly and cumulative ghg emissions from 1990 to 2014 (25 rows)
select year, sum(value) as ghg_emissions, sum(sum(value)) over (order by year) as cumulative_emissions
from greenhouse_gas_inventory_data_data
where category = "greenhouse_gas_ghgs_emissions_including_indirect_co2_without_lulucf_in_kilotonne_co2_equivalent"
group by year;

-- Identifying change in cumulative ghg emissions and land temperature:
-- Figure 15.3 
-- Creating view with yearly average land temperatures:
Create view temp as
(select year(recordedDate) as Year, recordedDate, cast(LandAverageTemperature as decimal(10,5)) as AvgLandTemp
from globaltemperatures);

-- output: yearly and cumulative ghg emissions, average temperature from 1990 to 2014 (25 rows)
select ghg.year, sum(value) as ghg_emission,sum(sum(value)) over (order by year) as cumulative_emission, avg_temp
from greenhouse_gas_inventory_data_data ghg inner join (
select Year, avg(AvgLandTemp) as avg_temp
from temp
group by year) as t1
on ghg.year = t1.year
where category = "greenhouse_gas_ghgs_emissions_including_indirect_co2_without_lulucf_in_kilotonne_co2_equivalent"
group by year;

-- Changing the datatype of seaice table attributes (year, extent):
alter table seaice 
modify column Year INT;

alter table seaice 
modify column Extent decimal(6,3);

-- Identifying change in cumulative ghg emissions and sea ice extent:
-- Figure 15.4
-- output: yearly and cumulative ghg emissions, seaice extent from 1990 to 2014 (25 rows)
select seaice.year, avg(extent) as seaice_extent, ghg_emission, sum(ghg_emission) over (order by year) as cumulative_emission
from seaice inner join (
select year, sum(value) as ghg_emission
from greenhouse_gas_inventory_data_data
where category = "greenhouse_gas_ghgs_emissions_including_indirect_co2_without_lulucf_in_kilotonne_co2_equivalent"
group by year) as t1
on seaice.year = t1.year
group by seaice.year;

-- Focusing on the change in average sea ice extent over the years:
-- figure 15.5
-- output: yearly sea ice extent from 1978 to 2019 (42 rows)
select year, avg(Extent) as average_extent 
from seaice
group by year;

-- Identifying change in cumulative ghg emissions and glacier mass balance:
-- there are values in area that are blank which will affect the evaluation later on. Therefore, they are ignored and removed from the view.
create view glacier_mass as (
select cast(WGMS_ID as unsigned) as WGMS_ID,cast(year as unsigned) as year, cast(area as decimal(10,4)) as area
from(
select WGMS_ID,substring(SURVEY_DATE,1,4) as year,AREA
from mass_balance_data)as t1
where area <> 0  )
;

-- all glaciers that have yearly records from 2005 to 2014 are extracted and evaluated. 
-- This time frame is used becauseas the latest year in the greenhouse_gas_inventory table is 2014.
-- output:  54 glaciers
select count(*) as glacier_count
from(
select WGMS_ID
from glacier_mass
where year <=2014 and year >=2005
group by WGMS_ID
having count(*) = 10) as t1
;

-- evalute change in sum of area of all glaciers & the change in cumulative ghg emissions over the decade:
-- figure 15.6
-- output: yearly and cumulative ghg emissions, glacier area from 2005 to 2014 (10 rows)
select * , sum(ghg_emission) over (order by t1.year) as cumulative_emission
from (
select year, sum(area) as total_area
from glacier_mass 
where WGMS_ID in(
select WGMS_ID
from glacier_mass
where year <=2014 and year >=2005
group by WGMS_ID
having count(*) = 10)
and year <=2014 and year >=2005
group by year) as t1 inner join (
select year, sum(value) as ghg_emission
from greenhouse_gas_inventory_data_data
where category = "greenhouse_gas_ghgs_emissions_including_indirect_co2_without_lulucf_in_kilotonne_co2_equivalent"
group by year) as t2
on t1.year = t2.year;

-- using external data set relating to disasters
-- creating table to import new datset
create table disaster (
Year INT NOT NULL, 
Disaster text,
Country text,
ISO text, 
Region text,
Total_death int NOT NULL, 
Total_affected int NOT NULL);

-- import the data using "Table Data Import Wizard" and importing the csv file.

-- evalute records relating to droughts, extreme temperatures, floods and storms
-- observe the trend in the count of natural disaster over the years:
-- figure 15.7
-- output: yearly count of natural disasters from 1970 to 2021
select year,  count(*) as disaster_count
from disaster
where disaster = "Drought" or disaster = "Extreme temperature" or disaster = "Flood" or disaster = "Storm"
group by year
;

-- evaluating the change in cumulative ghg emission and the change in number of natural disasters:
-- figure 15.8
-- output: yearly and cumulative ghg emissions, disaster count from 1990 to 2014 (25 rows)
select * , sum(ghg_emissions) over (order by ghg.year) as cumulative_emission
from (
select year, count(*) as disaster_count
from disaster
where disaster = "Drought" or disaster = "Extreme temperature" or disaster = "Flood" or disaster = "Storm"
group by year) as disaster1
inner join (
select year, sum(value) as ghg_emissions
from greenhouse_gas_inventory_data_data 
where category = "greenhouse_gas_ghgs_emissions_including_indirect_co2_without_lulucf_in_kilotonne_co2_equivalent"
group by year) as ghg
on disaster1.year = ghg.year
order by ghg.year;

