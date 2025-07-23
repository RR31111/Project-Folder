-- Data Cleaning

select *
from layoffs;

-- 1. check for duplicates and remove any
-- 2. standardize data and fix errors
-- 3. Look at null values and see what 
-- 4. remove any columns and rows that are not necessary - few ways

create table layoffs_staging
like layoffs;

select *
from layoffs_staging;

insert layoffs_staging
select *
from layoffs;

select *,
row_number() over(
partition by company, location, industry, total_laid_off, percentage_laid_off, `date`, stage,country,funds_raised_millions) as row_num
from layoffs_staging;

with duplicate_cte as
(
select *,
row_number() over(
partition by company, location, industry, total_laid_off, percentage_laid_off, `date`, stage,country,funds_raised_millions) as row_num
from layoffs_staging
)
select *
from duplicate_cte
where row_num > 1;

select*
from layoffs_staging
where company='Casper';

with duplicate_cte as
(
select *,
row_number() over(
partition by company, location, industry, total_laid_off, percentage_laid_off, `date`, stage,country,funds_raised_millions) as row_num
from layoffs_staging
)
delete 
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
  `row_num` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

select * 
from layoffs_staging2 ;

insert into layoffs_staging2
select *,
row_number() over(
partition by company, location, industry, total_laid_off, percentage_laid_off, `date`, stage,country,funds_raised_millions) as row_num
from layoffs_staging;

delete 
from layoffs_staging2 
where row_num >1 ;

select *
from layoffs_staging2;

-- Standardizing data : finding issue in your data and fix it
select company, trim(company)
from layoffs_staging2;

update layoffs_staging2
set company = trim(company);

select distinct industry
from layoffs_staging2
order by 1;


select *
from layoffs_staging2
where industry like 'Crypto%';

update layoffs_staging2
set industry = 'Cryto'
where industry like  'Crypto%';

select distinct industry
from layoffs_staging2;

select distinct location
from layoffs_staging2
order By 1;


select distinct country
from layoffs_staging2
order By 1;

select distinct country, trim(trailing '.' from country)
from layoffs_staging2
order By 1;

update layoffs_staging2
set country = trim(trailing '.' from country)
where country like 'United States%';

-- date 是text格式，要修改

select `date`
from layoffs_staging2;

SELECT *
FROM layoffs_staging2
WHERE `date` = 'None';

UPDATE layoffs_staging2
SET `date` = NULL
WHERE `date` = 'None';

update layoffs_staging2
set `date` = str_to_date(`date`,'%m/%d/%Y'); 

alter table layoffs_staging2
modify column`date` date;


SELECT industry
FROM layoffs_staging2;

select * 
from layoffs_staging2
where total_laid_off = 'None'
and percentage_laid_off = 'None';

select *
from layoffs_staging2
where industry is null
or industry ='';

select * 
from layoffs_staging2
where company = 'Airbnb';

update layoffs_staging2
set industry = null 
where industry='';

select *
from layoffs_staging2 as t1
join layoffs_staging2 as t2
	on t1.company= t2.company
    and t1.location = t2.location
where (t1.industry is null or t1.industry =''  )
and t2.industry is not null;

update layoffs_staging2 as t1
join layoffs_staging2 as t2
	on t1.company= t2.company
set t1.industry = t2.industry
where t1.industry is null 
and t2.industry is not null;

select * 
from layoffs_staging2
where total_laid_off = 'None'
and percentage_laid_off = 'None';

update layoffs_staging2
set total_laid_off = null 
where total_laid_off = 'None';

update layoffs_staging2
set percentage_laid_off = null 
where percentage_laid_off = 'None';

delete	
from layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null;

select *
from layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null;

alter table layoffs_staging2
drop column row_num;

select * 
from layoffs_staging2;


