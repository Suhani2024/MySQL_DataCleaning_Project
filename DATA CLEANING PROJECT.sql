-- DATA cleaning 

SELECT *
FROM layoffs;

-- 1. REMOVE DUPLICATES 
-- 2. STANDARDIZE THE DATA 
-- 3. NULL VALUES OR BLANK VALUES
-- 4. REMOVE ANY COLUMNS  

CREATE TABLE layoffs_staging
LIKE layoffs;

SELECT *
FROM layoffs_staging;

INSERT layoffs_staging
SELECT *
FROM layoffs;

SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location , industry, total_laid_off , percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging;

WITH duplicate_cte AS
(
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location , industry, total_laid_off , percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging
)
SELECT *
FROM duplicate_cte
WHERE row_num > 1;

SELECT *
FROM layoffs_staging
WHERE company = 'oda';

CREATE TABLE `layoffs_staging2` (
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

SELECT *
FROM layoffs_staging2
WHERE row_num > 1;

INSERT INTO layoffs_staging2
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location , industry, total_laid_off , percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging;

DELETE 
FROM layoffs_staging2
WHERE row_num > 1;

SET SQL_SAFE_UPDATES = 0;

-- STANDARDIZING DATA  

SELECT company , TRIM(company)
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET company = TRIM(company);

UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';

UPDATE layoffs_staging2
SET country = 'United States'
WHERE industry LIKE 'United States%';

SELECT `DATE`
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET `DATE` = str_to_date(`DATE` , '%m/%d/%Y');

ALTER TABLE layoffs_staging2
MODIFY COLUMN `DATE` DATE;

SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

SELECT *
FROM layoffs_staging2
WHERE industry IS NULL
OR industry = '' ;

UPDATE layoffs_staging2
SET industry = NULL
WHERE industry = '';

SELECT *
FROM layoffs_staging2 T1
JOIN layoffs_staging2 T2
	ON T1.company = T2.company
WHERE (T1.industry IS NULL OR T1.industry = '' )
AND T2.industry IS NOT NULL;

UPDATE layoffs_staging2 T1
JOIN layoffs_staging2 T2
	ON T1.company = T2.company
SET T1.industry = T2.industry
WHERE T1.industry IS NULL 
AND T2.industry IS NOT NULL;

DELETE 
FROM layoffs_staging2
WHERE total_laid_off IS NULL 
AND percentage_laid_off IS NULL;

ALTER TABLE layoffs_staging2
DROP COLUMN row_num;


-- EXPLORATORY DATA ANALYSIS

SELECT *
FROM layoffs_staging2;

SELECT *
FROM layoffs_staging2
WHERE percentage_laid_off = 1
ORDER BY funds_raised_millions DESC;

SELECT company, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY company
ORDER BY 2 DESC;

SELECT MIN(date), MAX(date) 
FROM layoffs_staging2;

SELECT industry, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY industry
ORDER BY 2 DESC;

SELECT country, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY country
ORDER BY 2 DESC;

SELECT date, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY date
ORDER BY 2 DESC;

SELECT year(date), SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY year(date)
ORDER BY 1 DESC;

SELECT stage, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY stage
ORDER BY 1 DESC;

SELECT SUBSTRING(date, 1, 7) AS MONTH , SUM(total_laid_off)
FROM layoffs_staging2
WHERE date IS NOT NULL
GROUP BY MONTH
ORDER BY 1 ASC;

WITH Rolling_Total AS
(
SELECT SUBSTRING(date, 1, 7) AS MONTH , SUM(total_laid_off) AS total_off
FROM layoffs_staging2
WHERE date IS NOT NULL
GROUP BY MONTH
ORDER BY 1 ASC
)
SELECT MONTH, total_off
,SUM(total_off) OVER(ORDER BY MONTH)
FROM Rolling_Total;


SELECT company, SUM(total_laid_off), YEAR(date)
FROM layoffs_staging2
GROUP BY company, YEAR(date)
ORDER BY 2 DESC;

WITH Company_Year (company, years, total_laid_off) AS
(
SELECT company, YEAR(date),SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY company, YEAR(date)
) , Company_Year_Rank AS
(SELECT * , 
dense_rank() OVER (PARTITION BY years ORDER BY total_laid_off DESC ) AS RANKING 
FROM Company_Year
WHERE years IS NOT NULL
ORDER BY RANKING ASC
)
SELECT *
FROM Company_Year_Rank
WHERE RANKING <= 5;










