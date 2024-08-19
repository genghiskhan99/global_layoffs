-- Exploratory Data Analysis



select count(*) as total_layoff_events
from layoffs_staging2;

SELECT 
    Min(date) as earliest_date,
    Max(date) as latest_date
FROM
    layoffs_staging2;
    


SELECT 
    country, count(*) as layoff_events
FROM
    layoffs_staging2
group by country
order by layoff_events DESC
limit 5;



-- Aggregate Layoffs since 2020


SELECT 
    DATE_FORMAT(`date`, '%Y-%m') AS Month,
    SUM(`total_laid_off`) AS Total_Layoffs
FROM 
    layoffs_staging2
WHERE 
    `date` >= '2020-01-01'
GROUP BY 
    Month
ORDER BY 
    Month;
    

-- Industries which have been affected the most

SELECT 
    `industry`, sum(`total_laid_off`) as total_layoffs
FROM
    layoffs_staging2
where `date` >= '2020-01-01'
group by `industry`
order by total_layoffs desc ;


-- Which countries have been affected most (geographical patterns)

SELECT 
    `country`, `location`, sum(`total_laid_off`) as total_layoffs
FROM
    layoffs_staging2
group by `country`, `location`
order by  total_layoffs desc;

select * from layoffs_staging2;



-- Company size vs Layoff Numbers

SELECT 
    `company`, 
    count(`total_laid_off`) as total_layoffs,
    sum(`total_laid_off`) as company_size
    FROM
    layoffs_staging2
    group by `company`
    order by company_size desc;


-- Seasonal Patterns
SELECT 
    date_format(`date`,'%m') as month,
    sum(`total_laid_off`) as total_layoffs
FROM
    layoffs_staging2
    group by month
    order by total_layoffs;






