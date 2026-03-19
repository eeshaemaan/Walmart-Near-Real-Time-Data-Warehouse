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

-- query 2
SELECT C.Gender, C.Age, C.City_Category, SUM(F.Revenue) AS Total_Purchase_Amount
FROM Fact_Sales F
JOIN Dim_Customer C ON F.Customer_ID_FK = C.Customer_ID
GROUP BY C.Gender, C.Age, C.City_Category
ORDER BY C.Gender, C.Age, C.City_Category;

-- query 3
SELECT C.Occupation, P.Product_Category, SUM(F.Revenue) AS Total_Sales
FROM Fact_Sales F
JOIN Dim_Customer C ON F.Customer_ID_FK = C.Customer_ID
JOIN Dim_Product P ON F.Product_ID_FK = P.Product_ID
GROUP BY C.Occupation, P.Product_Category
ORDER BY C.Occupation, P.Product_Category;

-- query 4
SELECT C.Gender, C.Age, T.Quarter, SUM(F.Revenue) AS Total_Purchase_Amount
FROM Fact_Sales F
JOIN Dim_Time T ON F.Date_Key_FK = T.Date_Key
JOIN Dim_Customer C ON F.Customer_ID_FK = C.Customer_ID
WHERE T.Year = (SELECT MAX(Year) FROM Dim_Time)    
GROUP BY C.Gender, C.Age, T.Quarter
ORDER BY C.Gender, C.Age, T.Quarter;

-- query 5

-- query 6
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

-- query 7
SELECT C.Stay_In_Current_City_Years, C.Gender, AVG(F.Revenue) AS Avg_Purchase_Amount
FROM Fact_Sales F
JOIN Dim_Customer C ON F.Customer_ID_FK = C.Customer_ID
GROUP BY C.Stay_In_Current_City_Years, C.Gender
ORDER BY C.Stay_In_Current_City_Years, C.Gender;

-- query 8

-- query 9

-- query 10
SELECT C.Age,
    CASE WHEN T.Is_Weekend = 1 THEN 'Weekend' ELSE 'Weekday' END AS Day_Type,
    SUM(F.Revenue) AS Total_Revenue
FROM Fact_Sales F
JOIN Dim_Customer C ON F.Customer_ID_FK = C.Customer_ID
JOIN Dim_Time T ON F.Date_Key_FK = T.Date_Key
WHERE T.Year = (SELECT MAX(Year) FROM Dim_Time)  -- latest year
GROUP BY C.Age, Day_Type
ORDER BY C.Age, Day_Type;

-- query 11

-- query 13
SELECT S.Store_Name, Sup.Supplier_Name, P.Product_Name, SUM(F.Revenue) AS Total_Sales
FROM Fact_Sales F
JOIN Dim_Store S ON F.Store_ID_FK = S.Store_ID
JOIN Dim_Supplier Sup ON F.Supplier_ID_FK = Sup.Supplier_ID
JOIN Dim_Product P ON F.Product_ID_FK = P.Product_ID
GROUP BY S.Store_Name, Sup.Supplier_Name, P.Product_Name
ORDER BY S.Store_Name, Sup.Supplier_Name, P.Product_Name;

-- query 14
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

-- query 16
SELECT F1.Product_ID_FK AS Product1, F2.Product_ID_FK AS Product2,
    COUNT(*) AS Times_Purchased_Together
FROM Fact_Sales F1
JOIN Fact_Sales F2 ON F1.Order_ID = F2.Order_ID
    AND F1.Product_ID_FK < F2.Product_ID_FK
GROUP BY F1.Product_ID_FK, F2.Product_ID_FK
ORDER BY Times_Purchased_Together DESC
LIMIT 5;

-- query 17
SELECT S.Store_Name, Sup.Supplier_Name, P.Product_Name, T.Year, SUM(F.Revenue) AS Total_Revenue
FROM Fact_Sales F
JOIN Dim_Store S ON F.Store_ID_FK = S.Store_ID
JOIN Dim_Supplier Sup ON F.Supplier_ID_FK = Sup.Supplier_ID
JOIN Dim_Product P ON F.Product_ID_FK = P.Product_ID
JOIN Dim_Time T ON F.Date_Key_FK = T.Date_Key
GROUP BY S.Store_Name, Sup.Supplier_Name, P.Product_Name, T.Year WITH ROLLUP
ORDER BY Store_Name, Supplier_Name, Product_Name, T.Year;

-- query 18
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

-- query 19

-- query 20
CREATE VIEW STORE_QUARTERLY_SALES AS
SELECT S.Store_Name, T.Year, T.Quarter, SUM(F.Revenue) AS Total_Revenue
FROM Fact_Sales F
JOIN Dim_Store S ON F.Store_ID_FK = S.Store_ID
JOIN Dim_Time T ON F.Date_Key_FK = T.Date_Key
GROUP BY S.Store_Name, T.Year, T.Quarter
ORDER BY S.Store_Name, T.Year, T.Quarter;


