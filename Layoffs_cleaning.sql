
-- Data Cleaning

rename table `layoffs (5)` to layoffs;

SELECT 
    *
FROM
    layoffs;
    
-- Remove duplicates
-- Standardize the data
-- Null Values or blank values
-- Remove any unnecessary columns

create table layoffs_staging like layoffs;

SELECT 
    *
FROM
    layoffs_staging;

insert into layoffs_staging 
SELECT 
    *
FROM
    layoffs;


-- Finding and Removing duplicates
    
   SELECT 
   *, Row_Number() over(
   Partition by
   company, industry, total_laid_off, percentage_laid_off, `date`) as row_num
FROM
    layoffs_staging; 


with duplicate_cte as 
(
SELECT 
   *, Row_Number() over(
   Partition by
   company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) as row_num
FROM
    layoffs_staging
)
select * from duplicate_cte where row_num > 1;


SELECT 
    *
FROM
    layoffs_staging
WHERE
    company = 'Casper';
    

with duplicate_cte as 
(
SELECT 
   *, Row_Number() over(
   Partition by
   company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) as row_num
FROM
    layoffs_staging
)
select * 
from duplicate_cte 
where row_num > 1;






CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` text,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` text,
  `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


select * from layoffs_staging2;


insert into layoffs_staging2
SELECT 
   *, Row_Number() over(
   Partition by
   company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) as row_num
FROM
    layoffs_staging
;


-- Standardizing Data

select company,(trim(company))
from layoffs_staging2;

update layoffs_staging2
set company = trim(company);

SELECT distinct(industry)
FROM
    layoffs_staging2
    order by 1;
    
    
select distinct(industry) from layoffs_staging2
where industry like "Crypto%";

update layoffs_staging2
set industry = "Crypto"
where industry like "Crypto%";

SELECT 
    *
FROM
    layoffs_staging2
where country = "United States";


update layoffs_staging2
set country = "United States"
where industry like "United States%";
    
SELECT 
    distinct(country), trim(trailing '.' from country)
FROM
    layoffs_staging2
    order by 1;

Update layoffs_staging2
set country = trim(trailing '.' from country)
where country like "United States%";

SELECT 
    `Date`, str_to_date(`date`,'%m/%d/%Y')
FROM
    layoffs_staging2;

SELECT
   distinct(date)
FROM
    layoffs_staging2;

    
Update layoffs_staging2
set `date` =  str_to_date(`date`,'%m/%d/%Y');

SELECT 
    `date`, str_to_date(`date`,'%m/%d/%Y')
FROM
    layoffs_staging2;


SELECT `date`,STR_TO_DATE(`date`, '%m/%d/%Y')
FROM layoffs_staging2
WHERE STR_TO_DATE(`date`, '%m/%d/%Y') IS NULL;


update layoffs_staging2
set

    order by 2;

Update layoffs_staging2
set `date` = 03/16/2022
where `date` = 16/03/2022;


UPDATE layoffs_staging2
SET `date` = CASE
    WHEN STR_TO_DATE(`date`, '%m/%d/%Y') IS NOT NULL THEN DATE_FORMAT(STR_TO_DATE(`date`, '%m/%d/%Y'), '%Y-%m-%d')
    WHEN STR_TO_DATE(`date`, '%d/%m/%Y') IS NOT NULL THEN DATE_FORMAT(STR_TO_DATE(`date`, '%d/%m/%Y'), '%Y-%m-%d')
    ELSE `date`
END;





SELECT 
    *
FROM
    layoffs_staging2
    where `date` = '0.002637652489119683' ;
    
UPDATE layoffs_staging2
SET `date` = STR_TO_DATE(`date`, '%d/%m/%Y')
WHERE `date` = '16/03/2022';

    

UPDATE layoffs_staging2
SET `date` = STR_TO_DATE('16/03/2022', '%d/%m/%Y')
WHERE `date` = '16/03/2022';


SELECT `date`
FROM layoffs_staging2
LIMIT 10;

DESCRIBE layoffs_staging2;

UPDATE layoffs_staging2
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

UPDATE layoffs_staging2
SET `date` = DATE_FORMAT(STR_TO_DATE(`date`, '%m/%d/%Y'), '%Y-%m-%d')
WHERE `date` = '2022-12-16';


SELECT 
    `date`
FROM
    layoffs_staging2
where `date` like '2022-12-16';

select `date`,
STR_TO_DATE(`date`, '%m/%d/%Y')
FROM layoffs_staging2
order by 2;



update layoffs_staging2
set `date` = STR_TO_DATE(`date`, '%m/%d/%Y')

ALTER TABLE layoffs_staging2 ADD COLUMN converted_date DATE;



UPDATE layoffs_staging2
SET converted_date = 
  CASE 
    WHEN date LIKE '____-__-__' THEN STR_TO_DATE(date, '%Y-%m-%d')
    WHEN date LIKE '__/__/____' THEN STR_TO_DATE(date, '%m/%d/%Y')
    ELSE NULL
  END;

SELECT date, converted_date
FROM layoffs_staging2
LIMIT 100;


UPDATE layoffs_staging2
SET converted_date = 
  CASE 
    WHEN date LIKE '____-__-__' THEN STR_TO_DATE(date, '%Y-%m-%d')
    WHEN date LIKE '__/__/____' THEN STR_TO_DATE(date, '%m/%d/%Y')
    WHEN date LIKE '_/_/____' THEN STR_TO_DATE(date, '%m/%d/%Y')
    WHEN date LIKE '__/_/____' THEN STR_TO_DATE(date, '%m/%d/%Y')
    WHEN date LIKE '_/__/____' THEN STR_TO_DATE(date, '%m/%d/%Y')
    ELSE NULL
  END;


ALTER TABLE layoffs_staging2
DROP COLUMN date,
CHANGE COLUMN converted_date date DATE;

OPTIMIZE TABLE layoffs_staging2;

SELECT 
    `date`
FROM
    layoffs_staging2
    order by 1;
    



-- Null and Blank Values
    


SELECT 
    *
FROM
    layoffs_staging2
WHERE
    industry is null; 
  


    SELECT 
    *
FROM
    layoffs_staging2
WHERE
company = 'Airbnb';



SELECT 
    t1.industry, t2.industry
FROM
    layoffs_staging2 t1
JOIN layoffs_staging2 t2
ON t1.company = t2.company 
AND t1.location = t2.location
where (t1.industry is null or t1.industry = '')
and t2.industry is not null;

update layoffs_staging2 t1
    JOIN layoffs_staging2 t2
ON t1.company = t2.company 
set t1.industry = t2.industry
where (t1.industry is null)
and t2.industry is not null;

update layoffs_staging2
set industry = null
where industry = ''
or industry = "none";

SELECT 
    *
FROM
    layoffs_staging2;
    and percentage_laid_off = "none";
    
update layoffs_staging2
set total_laid_off = null
where total_laid_off = "none";


update layoffs_staging2
set percentage_laid_off = null
where percentage_laid_off = "none";


SELECT 
    *
FROM
    layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null;

delete FROM
    layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null;


alter table layoffs_staging2
drop column row_num;



