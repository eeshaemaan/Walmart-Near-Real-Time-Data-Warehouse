CREATE DATABASE dataWP;
USE dataWP;

-- Dimension: Time
CREATE TABLE Dim_Time (
    Date_Key DATE NOT NULL PRIMARY KEY, -- YYYY-MM-DD
    Full_Date VARCHAR(100) NOT NULL,
    Month INT NOT NULL,
    Quarter INT NOT NULL,
    Year INT NOT NULL,
    Day_Name VARCHAR(10) NOT NULL,
    Is_Weekend BOOLEAN NOT NULL
);

-- Dimension: Customer
CREATE TABLE Dim_Customer (
    Customer_ID INT NOT NULL PRIMARY KEY,
    Gender CHAR(1) NOT NULL,
    Age VARCHAR(10) NOT NULL,
    Occupation VARCHAR(20),
    City_Category CHAR(1) NOT NULL,
    Stay_In_Current_City_Years VARCHAR(5) NOT NULL,
    Marital_Status VARCHAR(5)
);

-- Dimension: Product
CREATE TABLE Dim_Product (
    Product_ID VARCHAR(20) NOT NULL PRIMARY KEY,
    Product_Category VARCHAR(50) NOT NULL,
    Product_Name VARCHAR(255)
);

-- Dimension: Store
CREATE TABLE Dim_Store (
    Store_ID VARCHAR(20) NOT NULL PRIMARY KEY,
    Store_Name VARCHAR(255) NOT NULL
);

-- Dimension: Supplier
CREATE TABLE Dim_Supplier (
    Supplier_ID VARCHAR(20) NOT NULL PRIMARY KEY,
    Supplier_Name VARCHAR(255) NOT NULL
);

-- Fact Table: Fact_Sales
CREATE TABLE Fact_Sales (
    Order_ID INT NOT NULL,
    Product_ID_FK VARCHAR(20) NOT NULL,
    
    Customer_ID_FK INT NOT NULL,
    Store_ID_FK VARCHAR(20) NOT NULL,     
    Supplier_ID_FK VARCHAR(20) NOT NULL,
    Date_Key_FK DATE NOT NULL,             

    Quantity_Sold INT NOT NULL,
    Unit_Price DECIMAL(10, 2) NOT NULL,
    Revenue DECIMAL(10, 2) NOT NULL,

    PRIMARY KEY (Order_ID, Product_ID_FK),

    FOREIGN KEY (Customer_ID_FK) REFERENCES Dim_Customer(Customer_ID),
    FOREIGN KEY (Product_ID_FK) REFERENCES Dim_Product(Product_ID),
    FOREIGN KEY (Store_ID_FK) REFERENCES Dim_Store(Store_ID),
    FOREIGN KEY (Supplier_ID_FK) REFERENCES Dim_Supplier(Supplier_ID),
    FOREIGN KEY (Date_Key_FK) REFERENCES Dim_Time(Date_Key)
);

CREATE INDEX idx_customer_fk ON Fact_Sales (Customer_ID_FK);
CREATE INDEX idx_store_fk ON Fact_Sales (Store_ID_FK);
CREATE INDEX idx_supplier_fk ON Fact_Sales (Supplier_ID_FK);
CREATE INDEX idx_date_fk ON Fact_Sales (Date_Key_FK);

SELECT * FROM Fact_Sales;

SELECT * FROM Dim_Product;

SELECT * FROM Dim_Customer;

-- query 1 - Identifies the top 5 products by revenue, split by weekdays and weekends, with monthly breakdowns for a year

WITH Fact_Enriched AS (
    SELECT 
        T.Year,
        T.Month,
        F.Product_ID_FK,
        F.Revenue,
        CASE 
            WHEN T.Is_Weekend = 1 THEN 'Weekend'
            ELSE 'Weekday'
        END AS Day_Type
    FROM Fact_Sales F
    JOIN Dim_Time T ON F.Date_Key_FK = T.Date_Key
    WHERE T.Year = 2015
),

Monthly_Product_Revenue AS (
    SELECT 
        Year, Month, Day_Type, Product_ID_FK,
        SUM(Revenue) AS Total_Revenue
    FROM Fact_Enriched
    GROUP BY Year, Month, Day_Type, Product_ID_FK
)

SELECT curr.*
FROM Monthly_Product_Revenue curr
LEFT JOIN Monthly_Product_Revenue comp
    ON curr.Year = comp.Year
   AND curr.Month = comp.Month
   AND curr.Day_Type = comp.Day_Type
   AND curr.Total_Revenue < comp.Total_Revenue
GROUP BY curr.Year, curr.Month, curr.Day_Type, curr.Product_ID_FK, curr.Total_Revenue
HAVING COUNT(comp.Product_ID_FK) < 5
ORDER BY curr.Month, curr.Day_Type, curr.Total_Revenue DESC;


-- query 2 - Analyzes total purchase amounts by gender and age, detailed by city category

SELECT C.Gender, C.Age, C.City_Category, SUM(F.Revenue) AS Total_Purchase_Amount
FROM Fact_Sales F
JOIN Dim_Customer C ON F.Customer_ID_FK = C.Customer_ID
GROUP BY C.Gender, C.Age, C.City_Category
ORDER BY C.Gender, C.Age, C.City_Category;

-- query 3 - Examines total sales for each product category based on customer occupation

SELECT C.Occupation, P.Product_Category, SUM(F.Revenue) AS Total_Sales
FROM Fact_Sales F
JOIN Dim_Customer C ON F.Customer_ID_FK = C.Customer_ID
JOIN Dim_Product P ON F.Product_ID_FK = P.Product_ID
GROUP BY C.Occupation, P.Product_Category
ORDER BY C.Occupation, P.Product_Category;

-- query 4 - Tracks purchase amounts by gender and age across quarterly periods for the current year

SELECT C.Gender, C.Age, T.Quarter, SUM(F.Revenue) AS Total_Purchase_Amount
FROM Fact_Sales F
JOIN Dim_Time T ON F.Date_Key_FK = T.Date_Key
JOIN Dim_Customer C ON F.Customer_ID_FK = C.Customer_ID
WHERE T.Year = (SELECT MAX(Year) FROM Dim_Time)    
GROUP BY C.Gender, C.Age, T.Quarter
ORDER BY C.Gender, C.Age, T.Quarter;

-- query 5 - Highlights the top 5 occupations driving sales within each product category

WITH Category_Occupation_Sales AS (
    SELECT 
        P.Product_Category,
        C.Occupation,
        SUM(F.Revenue) AS Total_Revenue
    FROM Fact_Sales F
    JOIN Dim_Customer C ON F.Customer_ID_FK = C.Customer_ID
    JOIN Dim_Product P ON F.Product_ID_FK = P.Product_ID
    GROUP BY P.Product_Category, C.Occupation
)

SELECT curr.*
FROM Category_Occupation_Sales curr
LEFT JOIN Category_Occupation_Sales comp
    ON curr.Product_Category = comp.Product_Category
   AND curr.Total_Revenue < comp.Total_Revenue
GROUP BY curr.Product_Category, curr.Occupation, curr.Total_Revenue
HAVING COUNT(comp.Occupation) < 5
ORDER BY curr.Product_Category, curr.Total_Revenue DESC;

-- query 6 - Assesses purchase amounts by city category and marital status over the past 6 months

SELECT C.City_Category, C.Marital_Status, T.Year, T.Month, SUM(F.Revenue) AS Total_Purchase_Amount
FROM Fact_Sales F
JOIN Dim_Time T ON F.Date_Key_FK = T.Date_Key
JOIN Dim_Customer C ON F.Customer_ID_FK = C.Customer_ID
WHERE 
    (T.Year > (SELECT MAX(Year) FROM Dim_Time) - 1)
    OR
    (T.Year = (SELECT MAX(Year) FROM Dim_Time) AND T.Month >= (SELECT MAX(Month) - 5 FROM Dim_Time))
GROUP BY C.City_Category, C.Marital_Status, T.Year, T.Month
ORDER BY T.Year, T.Month, C.City_Category, C.Marital_Status;

-- query 7 - Calculates average purchase amount based on years stayed in the city and gender

SELECT C.Stay_In_Current_City_Years, C.Gender, AVG(F.Revenue) AS Avg_Purchase_Amount
FROM Fact_Sales F
JOIN Dim_Customer C ON F.Customer_ID_FK = C.Customer_ID
GROUP BY C.Stay_In_Current_City_Years, C.Gender
ORDER BY C.Stay_In_Current_City_Years, C.Gender;

-- query 8 - Ranks the top 5 city categories by revenue, grouped by product category

WITH City_Product_Revenue AS (
    SELECT 
        P.Product_Category,
        C.City_Category,
        SUM(F.Revenue) AS Total_Revenue
    FROM Fact_Sales F
    JOIN Dim_Customer C ON F.Customer_ID_FK = C.Customer_ID
    JOIN Dim_Product P ON F.Product_ID_FK = P.Product_ID
    GROUP BY P.Product_Category, C.City_Category
)

SELECT cpr1.*
FROM City_Product_Revenue cpr1
WHERE (
    SELECT COUNT(*)
    FROM City_Product_Revenue cpr2
    WHERE cpr2.Product_Category = cpr1.Product_Category
      AND cpr2.Total_Revenue > cpr1.Total_Revenue
) < 5
ORDER BY Product_Category, Total_Revenue DESC;

-- query 9 - Measures month-over-month sales growth percentage for each product category in the current year

WITH Monthly_Sales AS (
    SELECT 
        P.Product_Category,
        T.Year,
        T.Month,
        SUM(F.Revenue) AS Monthly_Total
    FROM Fact_Sales F
    JOIN Dim_Product P ON F.Product_ID_FK = P.Product_ID
    JOIN Dim_Time T ON F.Date_Key_FK = T.Date_Key
    WHERE T.Year = (SELECT MAX(Year) FROM Dim_Time)
    GROUP BY P.Product_Category, T.Year, T.Month
),

Sales_With_Prev AS (
    SELECT 
        curr.Product_Category,
        curr.Month,
        curr.Monthly_Total,
        prev.Monthly_Total AS Prev_Month_Total
    FROM Monthly_Sales curr
    LEFT JOIN Monthly_Sales prev
        ON curr.Product_Category = prev.Product_Category
       AND curr.Month = prev.Month + 1
)

SELECT 
    Product_Category,
    Month,
    Monthly_Total,
    ROUND(((Monthly_Total - Prev_Month_Total) / Prev_Month_Total) * 100, 2) AS MoM_Growth
FROM Sales_With_Prev
ORDER BY Product_Category, Month;

-- query 10 - Compares total sales by age group for weekends versus weekdays in the current year

SELECT C.Age,
    CASE WHEN T.Is_Weekend = 1 THEN 'Weekend' ELSE 'Weekday' END AS Day_Type,
    SUM(F.Revenue) AS Total_Revenue
FROM Fact_Sales F
JOIN Dim_Customer C ON F.Customer_ID_FK = C.Customer_ID
JOIN Dim_Time T ON F.Date_Key_FK = T.Date_Key
WHERE T.Year = (SELECT MAX(Year) FROM Dim_Time)  -- latest year
GROUP BY C.Age, Day_Type
ORDER BY C.Age, Day_Type;

-- query 11 - Find the top 5 products that generated the highest revenue, separated by weekday andweekend sales, with results grouped by month for a specified year

WITH Fact_Enriched AS (
    SELECT 
        T.Year,
        T.Month,
        F.Product_ID_FK,
        F.Revenue,
        CASE 
            WHEN T.Is_Weekend = 1 THEN 'Weekend'
            ELSE 'Weekday'
        END AS Day_Type
    FROM Fact_Sales F
    JOIN Dim_Time T ON F.Date_Key_FK = T.Date_Key
    WHERE T.Year = 2015
),

Monthly_Product_Revenue AS (
    SELECT 
        Year, Month, Day_Type, Product_ID_FK,
        SUM(Revenue) AS Total_Revenue
    FROM Fact_Enriched
    GROUP BY Year, Month, Day_Type, Product_ID_FK
)

SELECT curr.*
FROM Monthly_Product_Revenue curr
LEFT JOIN Monthly_Product_Revenue comp
    ON curr.Year = comp.Year
   AND curr.Month = comp.Month
   AND curr.Day_Type = comp.Day_Type
   AND curr.Total_Revenue < comp.Total_Revenue
GROUP BY curr.Year, curr.Month, curr.Day_Type, curr.Product_ID_FK, curr.Total_Revenue
HAVING COUNT(comp.Product_ID_FK) < 5
ORDER BY curr.Month, curr.Day_Type, curr.Total_Revenue DESC;

-- query 12 - Calculate the revenue growth rate for each store on a quarterly basis for 2017

WITH Store_Quarterly_Sales AS (
    SELECT 
        S.Store_ID,
        S.Store_Name,
        T.Quarter,
        SUM(F.Revenue) AS Quarterly_Revenue
    FROM Fact_Sales F
    JOIN Dim_Store S ON F.Store_ID_FK = S.Store_ID
    JOIN Dim_Time T ON F.Date_Key_FK = T.Date_Key
    WHERE T.Year = 2017
    GROUP BY S.Store_ID, S.Store_Name, T.Quarter
)

SELECT 
    curr.Store_ID,
    curr.Store_Name,
    curr.Quarter,
    curr.Quarterly_Revenue,
    ROUND(((curr.Quarterly_Revenue - prev.Quarterly_Revenue) / prev.Quarterly_Revenue) * 100, 2) AS Growth_Percent
FROM Store_Quarterly_Sales curr
LEFT JOIN Store_Quarterly_Sales prev
    ON curr.Store_ID = prev.Store_ID
   AND curr.Quarter = prev.Quarter + 1
ORDER BY curr.Store_ID, curr.Quarter;

-- query 13 - Detailed Supplier Sales Contribution by Store and Product Name

SELECT S.Store_Name, Sup.Supplier_Name, P.Product_Name, SUM(F.Revenue) AS Total_Sales
FROM Fact_Sales F
JOIN Dim_Store S ON F.Store_ID_FK = S.Store_ID
JOIN Dim_Supplier Sup ON F.Supplier_ID_FK = Sup.Supplier_ID
JOIN Dim_Product P ON F.Product_ID_FK = P.Product_ID
GROUP BY S.Store_Name, Sup.Supplier_Name, P.Product_Name
ORDER BY S.Store_Name, Sup.Supplier_Name, P.Product_Name;

-- query 14 - Seasonal Analysis of Product Sales Using Dynamic Drill-Down
SELECT P.Product_Name,
    CASE
        WHEN T.Month IN (3,4,5) THEN 'Spring'
        WHEN T.Month IN (6,7,8) THEN 'Summer'
        WHEN T.Month IN (9,10,11) THEN 'Fall'
        WHEN T.Month IN (12,1,2) THEN 'Winter'
    END AS Season, SUM(F.Revenue) AS Total_Revenue
FROM Fact_Sales F
JOIN Dim_Product P ON F.Product_ID_FK = P.Product_ID
JOIN Dim_Time T ON F.Date_Key_FK = T.Date_Key
WHERE T.Year = (SELECT MAX(Year) FROM Dim_Time)  -- latest year
GROUP BY P.Product_Name, Season
ORDER BY Season, Total_Revenue DESC;

-- query 15 - Store-Wise and Supplier-Wise Monthly Revenue Volatility

WITH Monthly_Revenue AS (
    SELECT 
        S.Store_ID,
        S.Store_Name,
        Sup.Supplier_ID,
        Sup.Supplier_Name,
        T.Year,
        T.Month,
        SUM(F.Revenue) AS Monthly_Revenue
    FROM Fact_Sales F
    JOIN Dim_Store S ON F.Store_ID_FK = S.Store_ID
    JOIN Dim_Supplier Sup ON F.Supplier_ID_FK = Sup.Supplier_ID
    JOIN Dim_Time T ON F.Date_Key_FK = T.Date_Key
    GROUP BY S.Store_ID, S.Store_Name, Sup.Supplier_ID, Sup.Supplier_Name, T.Year, T.Month
)

SELECT 
    curr.*,
    ROUND(
        ((curr.Monthly_Revenue - prev.Monthly_Revenue) / prev.Monthly_Revenue) * 100,
        2
    ) AS Volatility
FROM Monthly_Revenue curr
LEFT JOIN Monthly_Revenue prev
    ON curr.Store_ID = prev.Store_ID
   AND curr.Supplier_ID = prev.Supplier_ID
   AND (curr.Year * 12 + curr.Month) = (prev.Year * 12 + prev.Month + 1)
ORDER BY curr.Store_ID, curr.Supplier_ID, curr.Year, curr.Month;

-- query 16 - Top 5 Products Purchased Together Across Multiple Orders

WITH Product_Pairs AS (
    SELECT 
        f1.Product_ID_FK AS Product1,
        f2.Product_ID_FK AS Product2,
        f1.Order_ID
    FROM Fact_Sales f1
    JOIN Fact_Sales f2 
        ON f1.Order_ID = f2.Order_ID
       AND f1.Product_ID_FK < f2.Product_ID_FK
),

Pair_Frequency AS (
    SELECT Product1, Product2,
        COUNT(DISTINCT Order_ID) AS Frequency
    FROM Product_Pairs
    GROUP BY Product1, Product2
)

SELECT * FROM Pair_Frequency
ORDER BY Frequency DESC
LIMIT 5;

-- query 17 - Yearly Revenue Trends by Store, Supplier, and Product with ROLLUP

SELECT S.Store_Name, Sup.Supplier_Name, P.Product_Name,
       T.Year, SUM(F.Revenue) AS Total_Revenue
FROM Fact_Sales F
JOIN Dim_Store S ON F.Store_ID_FK = S.Store_ID
JOIN Dim_Supplier Sup ON F.Supplier_ID_FK = Sup.Supplier_ID
JOIN Dim_Product P ON F.Product_ID_FK = P.Product_ID
JOIN Dim_Time T ON F.Date_Key_FK = T.Date_Key
GROUP BY S.Store_Name, Sup.Supplier_Name, P.Product_Name, T.Year WITH ROLLUP;

-- query 18 - Revenue and Volume-Based Sales Analysis for Each Product for H1 and H2

SELECT P.Product_Name,
    SUM(CASE WHEN T.Month BETWEEN 1 AND 6 THEN F.Revenue ELSE 0 END) AS Revenue_H1,
    SUM(CASE WHEN T.Month BETWEEN 7 AND 12 THEN F.Revenue ELSE 0 END) AS Revenue_H2,
    SUM(F.Revenue) AS Revenue_Yearly,
    SUM(CASE WHEN T.Month BETWEEN 1 AND 6 THEN F.Quantity_Sold ELSE 0 END) AS Quantity_H1,
    SUM(CASE WHEN T.Month BETWEEN 7 AND 12 THEN F.Quantity_Sold ELSE 0 END) AS Quantity_H2,
    SUM(F.Quantity_Sold) AS Quantity_Yearly
FROM Fact_Sales F 
JOIN Dim_Product P ON F.Product_ID_FK = P.Product_ID
JOIN Dim_Time T ON F.Date_Key_FK = T.Date_Key
WHERE T.Year = (SELECT MAX(Year) FROM Dim_Time)  -- latest year
GROUP BY P.Product_Name ORDER BY P.Product_Name;

-- query 19 - Identify High Revenue Spikes in Product Sales and Highlight Outliers

WITH Daily_Product_Sales AS (
    SELECT 
        F.Product_ID_FK,
        T.Date_Key,
        SUM(F.Revenue) AS Daily_Revenue
    FROM Fact_Sales F
    JOIN Dim_Time T ON F.Date_Key_FK = T.Date_Key
    GROUP BY F.Product_ID_FK, T.Date_Key
),

Average_Sales AS (
    SELECT 
        Product_ID_FK,
        AVG(Daily_Revenue) AS Avg_Daily_Revenue
    FROM Daily_Product_Sales
    GROUP BY Product_ID_FK
)

SELECT 
    dps.Product_ID_FK,
    dps.Date_Key,
    dps.Daily_Revenue,
    avgS.Avg_Daily_Revenue
FROM Daily_Product_Sales dps
JOIN Average_Sales avgS 
    ON dps.Product_ID_FK = avgS.Product_ID_FK
WHERE dps.Daily_Revenue > 2 * avgS.Avg_Daily_Revenue
ORDER BY dps.Product_ID_FK, dps.Date_Key;

-- query 20 - Create a View STORE_QUARTERLY_SALES for Optimized Sales Analysis

CREATE VIEW STORE_QUARTERLY_SALES AS
SELECT S.Store_Name, T.Year, T.Quarter, SUM(F.Revenue) AS Total_Revenue
FROM Fact_Sales F
JOIN Dim_Store S ON F.Store_ID_FK = S.Store_ID
JOIN Dim_Time T ON F.Date_Key_FK = T.Date_Key
GROUP BY S.Store_Name, T.Year, T.Quarter;
