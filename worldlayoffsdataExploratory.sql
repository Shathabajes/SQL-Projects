-- Exploratort Data Analysis
SELECT * FROM world_layoffs.layoffs_staging2
WHERE percentage_laid_off =1 
ORDER BY funds_raised_millions;


WITH Rolling_Total AS (
SELECT SUBSTRING(`date`,1,7) AS `MONTH` , SUM(total_laid_off) AS total_off
FROM world_layoffs.layoffs_staging2
WHERE SUBSTRING(`date`,1,7) IS NOT NULL
GROUP BY `MONTH` ORDER BY 1 
)

SELECT `MONTH` ,SUM(total_off) OVER (ORDER BY `MONTH`) AS rolling_total
FROM Rolling_Total 
;
#First CTE
WITH company_year AS (

SELECT company ,YEAR(`date`) AS years ,SUM(total_laid_off) as total_laid_off
FROM world_layoffs.layoffs_staging2
GROUP BY company ,YEAR(`date`)
), company_year_rank AS         #Another CTE to rank the first one

(SELECT * ,DENSE_RANK() OVER(PARTITION BY years ORDER BY total_laid_off DESC) AS ranking
FROM company_year
WHERE years IS NOT NULL)

SELECT * #Filtering the rank
FROM company_year_rank
WHERE ranking <=5;
