-- Data Cleaning

-- 1) Remove Duplicates
-- 2) Standarize the Data
-- 3) Null/Blank Values
-- 4) Remove columns if needed
#Not to apply changes on the raw data

-- REMOVING DUPLICATES

CREATE TABLE world_layoffs.layoffs_staging
LIKE world_layoffs.layoffs; 

SELECT *
FROM world_layoffs.layoffs_staging;

INSERT INTO world_layoffs.layoffs_staging
SELECT *
FROM world_layoffs.layoffs;

WITH duplicate_cte AS 
(
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
    FROM world_layoffs.layoffs_staging 
)
SELECT *
FROM duplicate_cte
WHERE row_num > 1;

CREATE TABLE world_layoffs.layoffs_staging2 (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` INT 
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

INSERT INTO world_layoffs.layoffs_staging2
SELECT *,
           ROW_NUMBER() OVER (PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
    FROM world_layoffs.layoffs_staging
     ;
SELECT *
FROM world_layoffs.layoffs_staging2;

SET SQL_SAFE_UPDATES = 0;
DELETE 
FROM world_layoffs.layoffs_staging2 
WHERE row_num >1 ; 

SELECT *
FROM world_layoffs.layoffs_staging2;

-- Standarize the Data

SELECT company,TRIM(company)
FROM world_layoffs.layoffs_staging2;


update world_layoffs.layoffs_staging2
SET company=TRIM(company) ;


SELECT *
FROM world_layoffs.layoffs_staging2
WHERE industry LIKE 'Crypto%' ;

update world_layoffs.layoffs_staging2
SET industry='Crypto' WHERE industry LIKE 'Crypto%';

SELECT distinct country 
FROM world_layoffs.layoffs_staging2
ORDER BY 1;

update world_layoffs.layoffs_staging2
SET country=TRIM(Trailing '.' FROM country);

SELECT `date`
FROM world_layoffs.layoffs_staging2;

UPDATE world_layoffs.layoffs_staging2
SET `date`=str_to_date(`date`,'%m/%d/%Y');

ALTER TABLE world_layoffs.layoffs_staging2 
MODIFY COLUMN `date` DATE ;

-- Null/Blank Values

SELECT *
FROM world_layoffs.layoffs_staging2
WHERE industry is  null OR industry = '' ;

SELECT *
FROM world_layoffs.layoffs_staging2
WHERE  company="Bally's Interactive" OR company='Carvana' OR company='Juul';


SELECT t1.industry, t2.industry
FROM world_layoffs.layoffs_staging2 t1
JOIN world_layoffs.layoffs_staging2 t2
     ON t1.company = t2.company 
     AND t1.location = t2.location
WHERE (t1.industry IS NULL OR t1.industry='') AND (t2.industry IS NOT NULL) ;

UPDATE world_layoffs.layoffs_staging2
SET industry=NULL WHERE industry='';

UPDATE world_layoffs.layoffs_staging2 t1
JOIN world_layoffs.layoffs_staging2 t2
     ON t1.company = t2.company 
     AND t1.location = t2.location
SET t1.industry=t2.industry
WHERE (t1.industry IS NULL ) AND (t2.industry IS NOT NULL);

-- Removing rows and columns
DELETE 
FROM  world_layoffs.layoffs_staging2
WHERE 
    total_laid_off IS NULL 
    AND percentage_laid_off IS NULL ;

SELECT *
FROM  world_layoffs.layoffs_staging2
WHERE 
    total_laid_off IS NULL 
    AND percentage_laid_off IS NULL ;
    

ALTER TABLE world_layoffs.layoffs_staging2
DROP COLUMN row_num;