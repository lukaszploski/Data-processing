QUERY 1

SELECT subcategories.SubCategoryName as SubCategory, ROUND(SUM(LineTotal), 2) AS TotalSales
FROM `sales-analysis-401122.shop_data_bucket_dataset.orders` orders
LEFT JOIN `sales-analysis-401122.shop_data_bucket_dataset.products` products ON orders.ProductID = products.ProductID
LEFT JOIN `sales-analysis-401122.shop_data_bucket_dataset.productsubcategories` subcategories ON products.SubCategoryID = subcategories.SubCategoryID
GROUP BY subcategories.SubCategoryName
ORDER BY TotalSales ASC;


QUERY 2

SELECT EXTRACT(YEAR FROM PARSE_DATE("%m/%d/%Y", OrderDate)) AS Year, EXTRACT(MONTH FROM PARSE_DATE("%m/%d/%Y", OrderDate)) AS Month, 
ROUND(SUM(LineTotal), 2) AS TotalSales
FROM `sales-analysis-401122.shop_data_bucket_dataset.orders` 
GROUP BY Year, Month
ORDER BY Year, Month ASC; 


QUERY 3

WITH RankedCategories AS (
  SELECT categories.CategoryName as Category,orders.SalesOrderID AS OrderID,
    ROUND(SUM(LineTotal),2) as MaxOrderSale,
    ROW_NUMBER() OVER (PARTITION BY categories.CategoryName ORDER BY SUM(LineTotal) DESC) as ranking_within_category
    FROM `sales-analysis-401122.shop_data_bucket_dataset.orders` orders
    LEFT JOIN `sales-analysis-401122.shop_data_bucket_dataset.products` products ON orders.ProductID = products.ProductID
    LEFT JOIN `sales-analysis-401122.shop_data_bucket_dataset.productsubcategories` subcategories ON products.SubCategoryID = subcategories.SubCategoryID
    LEFT JOIN `sales-analysis-401122.shop_data_bucket_dataset.productcategories` categories ON subcategories.CategoryID = categories.CategoryID
    GROUP BY categories.CategoryName, orders.SalesOrderID
)

SELECT category, MaxOrderSale
FROM RankedCategories
WHERE ranking_within_category=1
ORDER BY MaxOrderSale ASC;