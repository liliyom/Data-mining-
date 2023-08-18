SELECT * FROM project0.onlineretail_final_1;
#What is the distribution of order values across all customers in the dataset?

SELECT
    CustomerID,
    AVG(Quantity*UnitPrice) AS Averageoforders,
    MAX(Quantity*UnitPrice) AS maximumofOrders,
    MIN(Quantity*UnitPrice) AS MinimumofOrders
FROM
    onlineretail_final_1
GROUP BY
    CustomerID
LIMIT 0, 1000;

#•	How many unique products has each customer purchased?
SELECT InvoiceNo, COUNT(DISTINCT StockCode) AS unique_products_count
FROM project0.onlineretail_final_1
GROUP BY InvoiceNo;

# Which customers have only made a single purchase from the company?
SELECT CustomerId, COUNT(DISTINCT StockCode) AS SinglePurchases
FROM project0.onlineretail_final_1
GROUP BY CustomerID
HAVING COUNT(DISTINCT StockCode) = 1;

#Which products are most commonly purchased together by customers in the dataset?
SELECT t1.Description AS Product1, t2.Description AS Product2, COUNT(*) AS Frequency
FROM project0.onlineretail_final_1 t1
JOIN project0.onlineretail_final_1 t2 ON t1.CustomerID = t2.CustomerID AND t1.InvoiceNo = t2.InvoiceNo AND t1.Description < t2.Description
WHERE t1.Description IS NOT NULL AND t2.Description IS NOT NULL
GROUP BY t1.Description, t2.Description
ORDER BY Frequency desc
LIMIT 15;

#1.	Customer Segmentation by Purchase Frequency
#Group customers into segments based on their purchase frequency, such as high, medium, and low frequency customers. This can help you identify your most loyal customers and those who need more attention.

SELECT
    CustomerID,
    SUM(CASE WHEN Purchased >= 100 THEN CustomerID ELSE 0 END) AS HighFrequency,
    SUM(CASE WHEN Purchased >= 20 AND Purchased < 100 THEN CustomerID ELSE 0 END) AS MediumFrequency,
    SUM(CASE WHEN Purchased < 20 THEN CustomerID ELSE 0 END) AS LowFrequency,
    Purchased
FROM (
    SELECT
        CustomerID,
        COUNT(CustomerID) AS Purchased
    FROM
      project0.onlineretail_final_1
    GROUP BY
        CustomerID
) AS PurchaseFrequency
GROUP BY
    CustomerID
LIMIT 100;
#2. Average Order Value by Country
#Calculate the average order value for each country to identify where your most valuable customers are located.
SELECT Country, AVG(TotalAmount) As AverageOrderValue FROM
(SELECT Country,InvoiceNo,Sum(Quantity*UnitPrice) As TotalAmount
from project0.onlineretail_final_1
group by Country,InvoiceNo) As TotalOrder
GROUP BY Country
order by AverageOrderValue desc;

# Identify customers who haven't made a purchase in a specific period (e.g., last 6 months) to assess churn.
SELECT DISTINCT project0.onlineretail_final_1.CustomerID
FROM project0.onlineretail_final_1
LEFT JOIN (
    SELECT CustomerID, MAX(InvoiceDate) AS last_purchase
    FROM project0.onlineretail_final_1
    GROUP BY CustomerID
) last_purchase_per_customer
ON project0.onlineretail_final_1.CustomerID = last_purchase_per_customer.CustomerID
WHERE last_purchase_per_customer.last_purchase IS NULL
   OR last_purchase_per_customer.last_purchase <= DATE_SUB(CURRENT_DATE, INTERVAL 6 MONTH);

#Determine which products are often purchased together by calculating the correlation between product purchases.
SELECT Description, SUM(Quantity) AS Often_purchased_TotalQuantity
FROM onlineretail_final_1
WHERE  StockCode
GROUP BY Description;

SELECT
    MONTH(InvoiceDate) AS month,
    COUNT(DISTINCT CustomerID) AS unique_customers,
    SUM(InvoiceNo) AS total_sales
FROM
    project0.onlineretail_final_1
GROUP BY
    MONTH(InvoiceDate)
ORDER BY
    month;

SELECT
    quarter(InvoiceDate) AS quarter,
    COUNT(DISTINCT CustomerID) AS unique_customers,
    SUM(InvoiceNo) AS total_sales
FROM
    project0.onlineretail_final_1
GROUP BY
    quarter(InvoiceDate)
ORDER BY
    quarter;










