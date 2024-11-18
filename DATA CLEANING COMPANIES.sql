Select * FROM layoffs;


-- 1. Remove Duplicates
-- 2. Standarise the data
-- 3. Look at the null values or blank values
-- 4. Remove any columns


CREATE TABLE layoffs_staging LIKE layoffs;

SELECT * 
FROM layoffs_staging;

INSERT layoffs_staging
Select *
From layoffs;

SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, 'date', stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging;

WITH duplicate_cte AS
(
 SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, 'date', stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging
)
SELECT * 
FROM duplicate_cte
WHERE row_num > 1;


SELECT * 
FROM layoffs_staging
WHERE company = 'Casper';


CREATE TABLE layoffs_staging2 (
    company TEXT,
    location TEXT,
    industry TEXT,
    total_laid_off INT DEFAULT NULL,
    percentage_laid_off TEXT,
    date TEXT,
    stage TEXT,
    country TEXT,
    funds_raised_millions INT DEFAULT NULL,
    row_num INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

INSERT INTO layoffs_staging2
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, 'date', stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging;

SELECT * 
FROM layoffs_staging2
WHERE row_num > 1;

SELECT * 
FROM layoffs_staging2
WHERE company = 'Better.com';

SET SQL_SAFE_UPDATES = 0;

SELECT *
FROM layoffs_staging2
WHERE row_num = 2
  AND (
      (company = 'Casper' AND location = 'New York City' AND date = '9/14/2021')
      OR (company = 'Cazoo' AND location = 'London' AND date = '6/7/2022')
      OR (company = 'Hibob' AND location = 'Tel Aviv' AND date = '3/30/2020')
      OR (company = 'Wildlife Studios' AND location = 'Sao Paulo' AND date = '11/28/2022')
      OR (company = 'Yahoo' AND location = 'SF Bay Area' AND date = '2/9/2023')
  );
  
  
DELETE
FROM layoffs_staging2
WHERE row_num = 2
  AND (
      (company = 'Casper' AND location = 'New York City' AND date = '9/14/2021')
      OR (company = 'Cazoo' AND location = 'London' AND date = '6/7/2022')
      OR (company = 'Hibob' AND location = 'Tel Aviv' AND date = '3/30/2020')
      OR (company = 'Wildlife Studios' AND location = 'Sao Paulo' AND date = '11/28/2022')
      OR (company = 'Yahoo' AND location = 'SF Bay Area' AND date = '2/9/2023')
  );
   
   SELECT * 
FROM layoffs_staging2
WHERE row_num > 1;

   SELECT * 
FROM layoffs_staging2;


-- Standardising data
   
SELECT company, TRIM(company)
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET company = TRIM(company);

SELECT DISTINCT industry
FROM layoffs_staging2;

SELECT *
FROM layoffs_staging2
WHERE industry LIKE 'Crypto%';

UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'CryptoCurrency';

UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto Currency';
   
SELECT DISTINCT country
FROM layoffs_staging2
Order by 1;

SELECT DISTINCT country, TRIM(TRAILING '.' FROM country)
FROM layoffs_staging2
ORDER BY 1;
   
UPDATE layoffs_staging2
SET country = TRIM(TRAILING '.' FROM country)
WHERE country like 'United States.';
   
SELECT `date`,
str_to_date(`date`, '%m/%d/%Y')
FROM layoffs_staging2;
   
UPDATE layoffs_staging2
SET `date` = str_to_date(`date`, '%m/%d/%Y');

SELECT `date`
FROM layoffs_staging2;
   
ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;

SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off is NULL;

UPDATE layoffs_staging2
SET industry = NULL
WHERE industry = '';


SELECT *
FROM layoffs_staging2
WHERE industry IS NULL
OR industry = '';

SELECT *
FROM layoffs_staging2
WHERE company = 'Airbnb';


SELECT *
FROM layoffs_staging2 t1
JOIN layoffs_staging2 t2
ON t1.company = t2.company
WHERE (t1.industry IS NULL OR t1.industry = '')
AND t2.industry IS NOT NULL;

UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE (t1.industry IS NULL)
AND t2.industry IS NOT NULL;

SELECT *
FROM layoffs_staging2
WHERE company LIKE 'Bally%';

SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off is NULL;

DELETE
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off is NULL;

ALTER TABLE layoffs_staging2
DROP COLUMN row_num;

SELECT *
FROM layoffs_staging2;

   