DROP TABLE IF EXISTS walmartsales.sales;
CREATE TABLE IF NOT EXISTS walmartsales.sales(
	Invoice_id varchar(30) NOT NULL PRIMARY KEY ,
	branch VARCHAR(5) NOT NULL,
    city VARCHAR(30) NOT NULL,
    customer_type VARCHAR(30) NOT NULL,
    gender VARCHAR(30) NOT NULL,
    product_line VARCHAR(100) NOT NULL,
    unit_price DECIMAL(10,2) NOT NULL,
    quantity INT NOT NULL,
    tax_pct FLOAT(6,4) NOT NULL,
    total DECIMAL(12, 4) NOT NULL,
    date DATETIME NOT NULL,
    time TIME NOT NULL,
    payment VARCHAR(15) NOT NULL,
    cogs DECIMAL(10,2) NOT NULL,
    gross_margin_pct FLOAT(11,9),
    gross_income DECIMAL(12, 4),
    rating FLOAT(2, 1)
);

-- -- -- --  Generic -- -- -- --
# Adding time_of_day column to the table 

SELECT `time`,
   ( CASE 
	when `time` between '00:00:00' and '12:00:00' then 'Morning'
	when `time` between '12:01:00' and '16:00:00' then 'Afternoon' 
	else 'Evening'
    End ) As time_of_day
FROM walmartsales.sales ;

ALTER TABLE walmartsales.sales
ADD COLUMN time_of_day VARCHAR(10);

UPDATE  walmartsales.sales 
SET time_of_day= (CASE 
	when `time` between '00:00:00' and '12:00:00' then 'Morning'
	when `time` between '12:01:00' and '16:00:00' then 'Afternoon' 
	else 'Evening'
    End ) ;

#Adding column day_name

ALTER TABLE walmartsales.sales ADD COLUMN day_name VARCHAR(20);

UPDATE walmartsales.sales
SET day_name = dayname(`date`) ;

#Adding month_name 

ALTER TABLE walmartsales.sales ADD COLUMN month_name VARCHAR(20);

UPDATE walmartsales.sales 
SET month_name = MONTHNAME(`date`);

-- ---------------------------------------------------------------------------------------
-- -- -- -- Product -- -- -- -- 

#أHow many unique Cities does tha data have ?
SELECT COUNT(DISTINCT(city)) 
FROM walmartsales.sales ;

#Which branch is in which city 

SELECT DISTINCT(city) , branch 
FROM walmartsales.sales; 


#How many unique product lines does the data have?
SELECT COUNT(DISTINCT(product_line))
FROM walmartsales.sales;

#What is the most common payment method?

SELECT 
	payment, COUNT(payment) as payment_count
FROM walmartsales.sales
GROUP BY payment
ORDER BY payment_count DESC
 ; 

#The most selling product line
SELECT 
	product_line, COUNT(product_line) as productLine_count
FROM walmartsales.sales
GROUP BY product_line
ORDER BY productLine_count DESC
 ; 
 
 #Total Revenue BY month 
 
SELECT month_name , SUM(total) As total_revenue
FROM walmartsales.sales
GROUP BY month_name
ORDER BY total_revenue DESC;

#What month has the largest COGS ?

WITH cogs_by_month AS (
    SELECT month_name, SUM(cogs) AS total_cogs
    FROM walmartsales.sales
    GROUP BY month_name
)
SELECT month_name, total_cogs
FROM cogs_by_month
WHERE total_cogs = (SELECT MAX(total_cogs) FROM cogs_by_month);


# what product_line has largest revenue?

SELECT product_line , SUM(total) As total_revenue
FROM walmartsales.sales
GROUP BY product_line
ORDER BY total_revenue DESC;

##OR using CTE
WITH revenue_by_productline AS (
    SELECT product_line , SUM(total) As total_revenue
    FROM walmartsales.sales
    GROUP BY product_line
)
SELECT product_line, total_revenue
FROM revenue_by_productline
WHERE total_revenue = (SELECT MAX(total_revenue) FROM revenue_by_productline);


#Which branch sold more products than average product sold?

SELECT branch , SUM(quantity) as unit_sold
FROM walmartsales.sales 
GROUP BY branch 
HAVING unit_sold > (SELECT AVG(quantity) FROM walmartsales.sales);


#What is the most common product line by gender?

SELECT product_line , gender ,COUNT(gender)
FROM walmartsales.sales
GROUP BY gender ,product_line 
ORDER BY COUNT(gender) DESC;

#What is the average rating of each product line?
SELECT product_line , ROUND(AVG(rating),2) as average_rating
FROM walmartsales.sales
GROUP BY product_line;

#Fetch each product line and add a column to those product line showing "Good", "Bad". Good if its greater than average sales

ALTER TABLE walmartsales.sales add column category VARCHAR(20) ;

WITH average_quantity_per_product_line AS (
    SELECT product_line, AVG(quantity) AS avg_quantity
    FROM walmartsales.sales
    GROUP BY product_line
)
UPDATE walmartsales.sales
SET category = (
    CASE WHEN quantity > (
        SELECT avg_quantity
        FROM average_quantity_per_product_line
        WHERE product_line = walmartsales.sales.product_line
    ) THEN 'GOOD'
    ELSE 'BAD'
    END
);

-- -----------------------------------------------------------------------------------------------
-- -- -- -- Sales -- -- -- --

#Number of sales made in each time of the day per weekday
select day_name ,time_of_day ,COUNT(*) AS total_sales
FROM walmartsales.sales 
GROUP BY time_of_day ,day_name
ORDER BY day_name ASC;

#Which of the customer types brings the most revenue?
SELECT customer_type , SUM(total) as total_rev
FROM walmartsales.sales 
GROUP BY customer_type
ORDER BY total_rev DESC;

#Which city has the largest tax percent/ VAT (Value Added Tax)?
SELECT city , AVG(tax_pct)
FROM walmartsales.sales
GROUP BY city
ORDER BY AVG(tax_pct) DESC ;

-- -----------------------------------------------
-- -- -- -- Customer -----------

#How many unique customer types does the data have?
SELECT DISTINCT(customer_type)
FROM walmartsales.sales;

#How many unique payment methods does the data have?
SELECT DISTINCT(payment)
FROM walmartsales.sales;

#What is the most common customer type?
SELECT customer_type ,COUNT(customer_type)
FROM walmartsales.sales
GROUP BY customer_type
ORDER BY 2 DESC
LIMIT 1;

#Which customer type buys the most?
SELECT customer_type , SUM(total)
FROM walmartsales.sales
GROUP BY customer_type
ORDER BY 2 DESC
LIMIT 1;

#What is the gender of most of the customers?
SELECT gender , COUNT(gender)
FROM walmartsales.sales
GROUP BY 1
ORDER BY 2 DESC
LIMIT 1;

#What is the gender distribution per branch?
SELECT branch ,gender, COUNT(gender)
FROM walmartsales.sales
GROUP BY 1,2
ORDER BY 1,3 DESC;

#Which time of the day do customers give most ratings?
SELECT time_of_day ,AVG(rating)
FROM walmartsales.sales
GROUP BY 1
ORDER BY 2 DESC
LIMIT 1;

#Which time of the day do customers give most ratings per branch?
SELECT time_of_day ,branch ,AVG(rating)
FROM walmartsales.sales
GROUP BY 1,2
ORDER BY 3 DESC
LIMIT 1;

#Which day fo the week has the best avg ratings?
SELECT day_name , AVG(rating)
FROM walmartsales.sales
GROUP BY 1
ORDER BY 2 DESC
LIMIT 1;

#Which day of the week has the best average ratings per branch?
WITH avg_ratings_per_day AS (
    SELECT 
        branch,
        DAYNAME(date) AS day_of_week,
        AVG(rating) AS avg_rating
    FROM walmartsales.sales
    GROUP BY branch, DAYNAME(date)
)
SELECT 
    branch, 
    day_of_week, 
    avg_rating
FROM avg_ratings_per_day
WHERE (branch, avg_rating) IN (
    SELECT 
        branch,
        MAX(avg_rating)
    FROM avg_ratings_per_day
    GROUP BY branch
);




