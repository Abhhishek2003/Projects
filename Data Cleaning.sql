SELECT * FROM Layoffs;
CREATE TABLE layoffs_staging
LIKE layoffs;
SELECT * FROM layoffs_staging;
INSERT layoffs_staging
SELECT * FROM layoffs;
SELECT * FROM layoffs_staging;


WITH duplicate_cte AS
(
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location , industry, total_laid_off, percentage_laid_off, `date` , stage , country , funds_raised) AS row_num
FROM layoffs_staging
)
SELECT * 
FROM duplicate_cte
WHERE row_num > 1;

CREATE TABLE `world_layoffs`.`layoffs_staging2` (
`company` text,
`location`text,
`industry`text,
`total_laid_off` text,
`percentage_laid_off` text,
`date` text,
`stage`text,
`country` text,
`funds_raised` int,
row_num INT
);

INSERT INTO `world_layoffs`.`layoffs_staging2`
(`company`,
`location`,
`industry`,
`total_laid_off`,
`percentage_laid_off`,
`date`,
`stage`,
`country`,
`funds_raised`,
`row_num`)
SELECT `company`,
`location`,
`industry`,
`total_laid_off`,
`percentage_laid_off`,
`date`,
`stage`,
`country`,
`funds_raised`,
ROW_NUMBER() OVER (
PARTITION BY company, location, industry, total_laid_off,percentage_laid_off,`date`, stage, country, funds_raised
) AS row_num
FROM 
layoffs_staging;

SELECT * FROM layoffs_staging2
WHERE row_num >1;
SET SQL_SAFE_UPDATES = 0;
DELETE FROM layoffs_staging2
WHERE row_num > 1;

SELECT * FROM layoffs_staging2;

-- Standarizing data
SELECT company, TRIM(company)
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET company = TRIM(company);

SELECT company, TRIM(company)
FROM layoffs_staging2;

SELECT DISTINCT industry 
FROM layoffs_staging2
ORDER BY 1;

SELECT DISTINCT location 
FROM layoffs_staging2
ORDER BY 1;

SELECT DISTINCT country
FROM layoffs_staging2
ORDER BY 1;

SELECT * FROM layoffs_staging2;

ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;


UPDATE layoffs_staging2
SET `date` = STR_TO_DATE(`date`, '%Y-%m-%d');

DESCRIBE layoffs_staging2;

SELECT `date` FROM layoffs_staging2;

SELECT * 
FROM layoffs_staging2
WHERE location='SF Bay Area';

SELECT t1.industry,t2.industry
FROM layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company
WHERE (t1.industry = '')
AND t2.industry IS NOT NULL;

SELECT * FROM layoffs_staging2
WHERE total_laid_off = ''
AND percentage_laid_off = '';

DELETE  FROM layoffs_staging2
WHERE total_laid_off = ''
AND percentage_laid_off = '';

SELECT * FROM layoffs_staging2;

ALTER TABLE layoffs_staging2
DROP COLUMN row_num;

SELECT * FROM layoffs_staging2
WHERE location ='SF Bay Area';

SELECT * FROM layoffs_staging2
WHERE total_laid_off = ''
AND percentage_laid_off = '';

SELECT * FROM layoffs_staging2;