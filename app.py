import streamlit as st
import pandas as pd
import time

# Page config
st.set_page_config(page_title="Walmart Data Warehouse :) ", layout="wide")

# Custom CSS for dark theme with purple/pink accents
st.markdown("""
<style>
    /* Main background - dark */
    .stApp {
        background-color: #0e1117;
    }
    
    /* Title styling */
    .main-title {
        text-align: center;
        padding: 1rem;
        background: linear-gradient(90deg, #9b59b6, #e84393);
        border-radius: 15px;
        color: white;
        margin-bottom: 1.5rem;
    }
    
    /* Success message box */
    .status-box {
        background: linear-gradient(90deg, #2a1f3d, #3d1f2f);
        padding: 1rem;
        border-radius: 10px;
        border-left: 5px solid #e84393;
        margin-bottom: 1rem;
        color: #e0e0e0;
    }
    
    /* Button styling */
    .stButton > button {
        background: linear-gradient(90deg, #9b59b6, #e84393);
        color: white;
        border: none;
        border-radius: 8px;
        padding: 0.5rem 1rem;
        font-weight: bold;
        transition: all 0.3s ease;
    }
    
    .stButton > button:hover {
        transform: translateY(-2px);
        box-shadow: 0 5px 15px rgba(232, 67, 147, 0.3);
    }
    
    /* Sidebar styling - dark */
    [data-testid="stSidebar"] {
        background-color: #1a1c24;
        border-right: 2px solid #e84393;
    }
    
    [data-testid="stSidebar"] .stMarkdown {
        color: #e0e0e0;
    }
    
    /* Selectbox styling */
    .stSelectbox > div > div {
        background-color: #1e1e2e;
        border-radius: 8px;
        border: 1px solid #9b59b6;
        color: #e0e0e0;
    }
    
    /* Info box styling */
    .stInfo {
        background-color: #1a1f2e;
        border-left: 3px solid #9b59b6;
        border-radius: 8px;
        color: #e0e0e0;
    }
    
    /* Dataframe styling */
    [data-testid="stDataFrame"] {
        border-radius: 10px;
        overflow: hidden;
        border: 1px solid #e84393;
    }
    
    /* Divider styling */
    hr {
        background: linear-gradient(90deg, #9b59b6, #e84393, #9b59b6);
        height: 2px;
        border: none;
    }
    
    /* Caption styling */
    .stCaption {
        color: #e84393;
        font-style: italic;
    }
    
    /* Warning box */
    .stWarning {
        background-color: #2a1f1f;
        border-left-color: #e84393;
    }
    
    /* Error box */
    .stError {
        background-color: #2a1f1f;
        border-left-color: #e84393;
    }
    
    /* Headers */
    h1, h2, h3, h4, h5, h6 {
        color: #e84393;
    }
    
    /* Labels */
    .stTextInput label, .stSelectbox label {
        color: #e0e0e0 !important;
    }
    
    /* Text input */
    .stTextInput > div > div > input {
        background-color: #1e1e2e;
        color: #e0e0e0;
        border: 1px solid #9b59b6;
    }
    
    /* Success message */
    .stSuccess {
        background-color: #1a2a1f;
        color: #e0e0e0;
    }
    
    /* Footer styling */
    .footer {
        position: fixed;
        bottom: 0;
        left: 0;
        width: 100%;
        background: linear-gradient(90deg, #1a1c24, #2a1f3d);
        color: #e84393;
        text-align: center;
        padding: 0.5rem;
        font-size: 0.8rem;
        border-top: 1px solid #e84393;
        z-index: 999;
    }
</style>
""", unsafe_allow_html=True)

# Title
st.markdown('<div class="main-title"><h1>Walmart Data Warehouse :) </h1><p>Near-Real-Time Analytics Dashboard</p></div>', unsafe_allow_html=True)

# Query definitions
QUERIES = {
    "Q1: Top 5 Products by Revenue (Weekday/Weekend, Monthly 2015)": {
        "desc": "Shows top 5 products by revenue split by weekdays and weekends with monthly breakdown for 2015",
        "sql": """
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
            SELECT curr.Month, curr.Day_Type, curr.Product_ID_FK, P.Product_Name, curr.Total_Revenue
            FROM Monthly_Product_Revenue curr
            LEFT JOIN Monthly_Product_Revenue comp
                ON curr.Year = comp.Year
               AND curr.Month = comp.Month
               AND curr.Day_Type = comp.Day_Type
               AND curr.Total_Revenue < comp.Total_Revenue
            JOIN Dim_Product P ON curr.Product_ID_FK = P.Product_ID
            GROUP BY curr.Month, curr.Day_Type, curr.Product_ID_FK, P.Product_Name, curr.Total_Revenue
            HAVING COUNT(comp.Product_ID_FK) < 5
            ORDER BY curr.Month, curr.Day_Type, curr.Total_Revenue DESC
            LIMIT 50
        """
    },
    
    "Q2: Customer Demographics by Purchase Amount with City Category": {
        "desc": "Analyzes total purchase amounts by gender, age, and city category",
        "sql": """
            SELECT C.Gender, C.Age, C.City_Category, 
                   SUM(F.Revenue) AS Total_Purchase_Amount,
                   COUNT(*) AS Transaction_Count
            FROM Fact_Sales F
            JOIN Dim_Customer C ON F.Customer_ID_FK = C.Customer_ID
            GROUP BY C.Gender, C.Age, C.City_Category
            ORDER BY Total_Purchase_Amount DESC
            LIMIT 100
        """
    },
    
    "Q3: Product Category Sales by Occupation": {
        "desc": "Examines total sales for each product category based on customer occupation",
        "sql": """
            SELECT C.Occupation, P.Product_Category, 
                   SUM(F.Revenue) AS Total_Sales,
                   COUNT(*) AS Transaction_Count
            FROM Fact_Sales F
            JOIN Dim_Customer C ON F.Customer_ID_FK = C.Customer_ID
            JOIN Dim_Product P ON F.Product_ID_FK = P.Product_ID
            GROUP BY C.Occupation, P.Product_Category
            ORDER BY C.Occupation, Total_Sales DESC
        """
    },
    
    "Q4: Total Purchases by Gender and Age (Quarterly Trend)": {
        "desc": "Tracks purchase amounts by gender and age across quarterly periods for current year",
        "sql": """
            SELECT C.Gender, C.Age, T.Quarter, 
                   SUM(F.Revenue) AS Total_Purchase_Amount
            FROM Fact_Sales F
            JOIN Dim_Time T ON F.Date_Key_FK = T.Date_Key
            JOIN Dim_Customer C ON F.Customer_ID_FK = C.Customer_ID
            WHERE T.Year = (SELECT MAX(Year) FROM Dim_Time)    
            GROUP BY C.Gender, C.Age, T.Quarter
            ORDER BY C.Gender, C.Age, T.Quarter
        """
    },
    
    "Q5: Top 5 Occupations per Product Category": {
        "desc": "Highlights top 5 occupations driving sales within each product category",
        "sql": """
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
            SELECT curr.Product_Category, curr.Occupation, curr.Total_Revenue
            FROM Category_Occupation_Sales curr
            LEFT JOIN Category_Occupation_Sales comp
                ON curr.Product_Category = comp.Product_Category
               AND curr.Total_Revenue < comp.Total_Revenue
            GROUP BY curr.Product_Category, curr.Occupation, curr.Total_Revenue
            HAVING COUNT(comp.Occupation) < 5
            ORDER BY curr.Product_Category, curr.Total_Revenue DESC
        """
    },
    
    "Q6: City Category Performance by Marital Status (6 Months)": {
        "desc": "Assesses purchase amounts by city category and marital status over past 6 months",
        "sql": """
            SELECT C.City_Category, C.Marital_Status, T.Year, T.Month, 
                   SUM(F.Revenue) AS Total_Purchase_Amount
            FROM Fact_Sales F
            JOIN Dim_Time T ON F.Date_Key_FK = T.Date_Key
            JOIN Dim_Customer C ON F.Customer_ID_FK = C.Customer_ID
            WHERE T.Date_Key_FK >= DATE_SUB(CURDATE(), INTERVAL 6 MONTH)
            GROUP BY C.City_Category, C.Marital_Status, T.Year, T.Month
            ORDER BY T.Year DESC, T.Month DESC, C.City_Category
        """
    },
    
    "Q7: Average Purchase Amount by Stay Duration and Gender": {
        "desc": "Calculates average purchase amount based on years stayed in city and gender",
        "sql": """
            SELECT C.Stay_In_Current_City_Years, C.Gender, 
                   ROUND(AVG(F.Revenue), 2) AS Avg_Purchase_Amount,
                   COUNT(*) AS Transaction_Count
            FROM Fact_Sales F
            JOIN Dim_Customer C ON F.Customer_ID_FK = C.Customer_ID
            GROUP BY C.Stay_In_Current_City_Years, C.Gender
            ORDER BY C.Stay_In_Current_City_Years, C.Gender
        """
    },
    
    "Q8: Top 5 City Categories by Revenue per Product Category": {
        "desc": "Ranks top 5 city categories by revenue grouped by product category",
        "sql": """
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
            SELECT cpr1.Product_Category, cpr1.City_Category, cpr1.Total_Revenue
            FROM City_Product_Revenue cpr1
            WHERE (
                SELECT COUNT(*)
                FROM City_Product_Revenue cpr2
                WHERE cpr2.Product_Category = cpr1.Product_Category
                  AND cpr2.Total_Revenue > cpr1.Total_Revenue
            ) < 5
            ORDER BY Product_Category, Total_Revenue DESC
        """
    },
    
    "Q9: Monthly Sales Growth by Product Category": {
        "desc": "Measures month-over-month sales growth percentage for each product category",
        "sql": """
            WITH Monthly_Sales AS (
                SELECT 
                    P.Product_Category,
                    T.Month,
                    SUM(F.Revenue) AS Monthly_Total
                FROM Fact_Sales F
                JOIN Dim_Product P ON F.Product_ID_FK = P.Product_ID
                JOIN Dim_Time T ON F.Date_Key_FK = T.Date_Key
                WHERE T.Year = (SELECT MAX(Year) FROM Dim_Time)
                GROUP BY P.Product_Category, T.Month
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
                CASE 
                    WHEN Prev_Month_Total IS NULL OR Prev_Month_Total = 0 THEN NULL
                    ELSE ROUND(((Monthly_Total - Prev_Month_Total) / Prev_Month_Total) * 100, 2)
                END AS MoM_Growth_Percent
            FROM Sales_With_Prev
            ORDER BY Product_Category, Month
        """
    },
    
    "Q10: Weekend vs Weekday Sales by Age Group": {
        "desc": "Compares total sales by age group for weekends vs weekdays",
        "sql": """
            SELECT C.Age,
                CASE WHEN T.Is_Weekend = 1 THEN 'Weekend' ELSE 'Weekday' END AS Day_Type,
                SUM(F.Revenue) AS Total_Revenue,
                COUNT(*) AS Transaction_Count,
                ROUND(AVG(F.Revenue), 2) AS Avg_Transaction
            FROM Fact_Sales F
            JOIN Dim_Customer C ON F.Customer_ID_FK = C.Customer_ID
            JOIN Dim_Time T ON F.Date_Key_FK = T.Date_Key
            WHERE T.Year = (SELECT MAX(Year) FROM Dim_Time)
            GROUP BY C.Age, Day_Type
            ORDER BY C.Age, Day_Type
        """
    },
    
    "Q11: Top Revenue Products on Weekdays/Weekends (Monthly)": {
        "desc": "Top products by revenue for weekdays/weekends with monthly breakdown",
        "sql": """
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
            SELECT curr.Month, curr.Day_Type, curr.Product_ID_FK, P.Product_Name, curr.Total_Revenue
            FROM Monthly_Product_Revenue curr
            LEFT JOIN Monthly_Product_Revenue comp
                ON curr.Year = comp.Year
               AND curr.Month = comp.Month
               AND curr.Day_Type = comp.Day_Type
               AND curr.Total_Revenue < comp.Total_Revenue
            JOIN Dim_Product P ON curr.Product_ID_FK = P.Product_ID
            GROUP BY curr.Month, curr.Day_Type, curr.Product_ID_FK, P.Product_Name, curr.Total_Revenue
            HAVING COUNT(comp.Product_ID_FK) < 5
            ORDER BY curr.Month, curr.Day_Type, curr.Total_Revenue DESC
            LIMIT 50
        """
    },
    
    "Q12: Store Revenue Growth Rate Quarterly (2017)": {
        "desc": "Calculates revenue growth rate for each store quarterly for 2017",
        "sql": """
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
                CASE 
                    WHEN prev.Quarterly_Revenue IS NULL OR prev.Quarterly_Revenue = 0 THEN NULL
                    ELSE ROUND(((curr.Quarterly_Revenue - prev.Quarterly_Revenue) / prev.Quarterly_Revenue) * 100, 2)
                END AS Growth_Percent
            FROM Store_Quarterly_Sales curr
            LEFT JOIN Store_Quarterly_Sales prev
                ON curr.Store_ID = prev.Store_ID
               AND curr.Quarter = prev.Quarter + 1
            ORDER BY curr.Store_ID, curr.Quarter
        """
    },
    
    "Q13: Supplier Sales Contribution by Store and Product": {
        "desc": "Shows total sales contribution of each supplier by store and product",
        "sql": """
            SELECT S.Store_Name, Sup.Supplier_Name, P.Product_Name, SUM(F.Revenue) AS Total_Sales
            FROM Fact_Sales F
            JOIN Dim_Store S ON F.Store_ID_FK = S.Store_ID
            JOIN Dim_Supplier Sup ON F.Supplier_ID_FK = Sup.Supplier_ID
            JOIN Dim_Product P ON F.Product_ID_FK = P.Product_ID
            GROUP BY S.Store_Name, Sup.Supplier_Name, P.Product_Name
            ORDER BY S.Store_Name, Sup.Supplier_Name, Total_Sales DESC
        """
    },
    
    "Q14: Seasonal Analysis of Product Sales": {
        "desc": "Presents total sales for each product by seasonal periods (Spring, Summer, Fall, Winter)",
        "sql": """
            SELECT P.Product_Name,
                CASE
                    WHEN T.Month IN (3,4,5) THEN 'Spring'
                    WHEN T.Month IN (6,7,8) THEN 'Summer'
                    WHEN T.Month IN (9,10,11) THEN 'Fall'
                    WHEN T.Month IN (12,1,2) THEN 'Winter'
                END AS Season, 
                SUM(F.Revenue) AS Total_Revenue
            FROM Fact_Sales F
            JOIN Dim_Product P ON F.Product_ID_FK = P.Product_ID
            JOIN Dim_Time T ON F.Date_Key_FK = T.Date_Key
            WHERE T.Year = (SELECT MAX(Year) FROM Dim_Time)
            GROUP BY P.Product_Name, Season
            ORDER BY Season, Total_Revenue DESC
        """
    },
    
    "Q15: Store and Supplier Monthly Revenue Volatility": {
        "desc": "Calculates month-to-month revenue volatility for each store-supplier pair",
        "sql": """
            WITH Monthly_Revenue AS (
                SELECT 
                    S.Store_Name,
                    Sup.Supplier_Name,
                    T.Year,
                    T.Month,
                    SUM(F.Revenue) AS Monthly_Revenue
                FROM Fact_Sales F
                JOIN Dim_Store S ON F.Store_ID_FK = S.Store_ID
                JOIN Dim_Supplier Sup ON F.Supplier_ID_FK = Sup.Supplier_ID
                JOIN Dim_Time T ON F.Date_Key_FK = T.Date_Key
                GROUP BY S.Store_Name, Sup.Supplier_Name, T.Year, T.Month
            )
            SELECT 
                curr.Store_Name,
                curr.Supplier_Name,
                curr.Year,
                curr.Month,
                curr.Monthly_Revenue,
                CASE 
                    WHEN prev.Monthly_Revenue IS NULL OR prev.Monthly_Revenue = 0 THEN NULL
                    ELSE ROUND(((curr.Monthly_Revenue - prev.Monthly_Revenue) / prev.Monthly_Revenue) * 100, 2)
                END AS Volatility_Percent
            FROM Monthly_Revenue curr
            LEFT JOIN Monthly_Revenue prev
                ON curr.Store_Name = prev.Store_Name
               AND curr.Supplier_Name = prev.Supplier_Name
               AND (curr.Year * 12 + curr.Month) = (prev.Year * 12 + prev.Month + 1)
            ORDER BY curr.Store_Name, curr.Supplier_Name, curr.Year, curr.Month
        """
    },
    
    "Q16: Top 5 Products Purchased Together": {
        "desc": "Identifies top 5 products frequently bought together in same transaction",
        "sql": """
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
                SELECT 
                    Product1, 
                    Product2,
                    COUNT(DISTINCT Order_ID) AS Frequency
                FROM Product_Pairs
                GROUP BY Product1, Product2
            )
            SELECT 
                P1.Product_Name AS Product1_Name,
                P2.Product_Name AS Product2_Name,
                pf.Frequency
            FROM Pair_Frequency pf
            JOIN Dim_Product P1 ON pf.Product1 = P1.Product_ID
            JOIN Dim_Product P2 ON pf.Product2 = P2.Product_ID
            ORDER BY pf.Frequency DESC
            LIMIT 5
        """
    },
    
    "Q17: Yearly Revenue Trends with ROLLUP": {
        "desc": "Uses ROLLUP for yearly revenue aggregation by store, supplier, product",
        "sql": """
            SELECT 
                COALESCE(S.Store_Name, 'ALL STORES') AS Store_Name,
                COALESCE(Sup.Supplier_Name, 'ALL SUPPLIERS') AS Supplier_Name,
                COALESCE(P.Product_Name, 'ALL PRODUCTS') AS Product_Name,
                T.Year,
                SUM(F.Revenue) AS Total_Revenue
            FROM Fact_Sales F
            JOIN Dim_Store S ON F.Store_ID_FK = S.Store_ID
            JOIN Dim_Supplier Sup ON F.Supplier_ID_FK = Sup.Supplier_ID
            JOIN Dim_Product P ON F.Product_ID_FK = P.Product_ID
            JOIN Dim_Time T ON F.Date_Key_FK = T.Date_Key
            GROUP BY S.Store_Name, Sup.Supplier_Name, P.Product_Name, T.Year WITH ROLLUP
            LIMIT 200
        """
    },
    
    "Q18: H1 vs H2 Revenue and Volume Analysis": {
        "desc": "Calculates H1 and H2 revenue and quantity totals for each product",
        "sql": """
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
            WHERE T.Year = (SELECT MAX(Year) FROM Dim_Time)
            GROUP BY P.Product_Name 
            ORDER BY Revenue_Yearly DESC
        """
    },
    
    "Q19: High Revenue Spike Detection (Outliers)": {
        "desc": "Flags days where sales exceed twice the daily average as potential outliers",
        "sql": """
            WITH Daily_Product_Sales AS (
                SELECT 
                    P.Product_Name,
                    T.Date_Key,
                    SUM(F.Revenue) AS Daily_Revenue
                FROM Fact_Sales F
                JOIN Dim_Time T ON F.Date_Key_FK = T.Date_Key
                JOIN Dim_Product P ON F.Product_ID_FK = P.Product_ID
                GROUP BY P.Product_Name, T.Date_Key
            ),
            Average_Sales AS (
                SELECT 
                    Product_Name,
                    AVG(Daily_Revenue) AS Avg_Daily_Revenue,
                    STDDEV(Daily_Revenue) AS StdDev_Daily_Revenue
                FROM Daily_Product_Sales
                GROUP BY Product_Name
            )
            SELECT 
                dps.Product_Name,
                dps.Date_Key,
                ROUND(dps.Daily_Revenue, 2) AS Daily_Revenue,
                ROUND(avgS.Avg_Daily_Revenue, 2) AS Avg_Daily_Revenue,
                CASE 
                    WHEN dps.Daily_Revenue > 2 * avgS.Avg_Daily_Revenue THEN 'SPIKE DETECTED'
                    ELSE 'Normal'
                END AS Spike_Status
            FROM Daily_Product_Sales dps
            JOIN Average_Sales avgS ON dps.Product_Name = avgS.Product_Name
            WHERE dps.Daily_Revenue > 2 * avgS.Avg_Daily_Revenue
            ORDER BY (dps.Daily_Revenue / avgS.Avg_Daily_Revenue) DESC
            LIMIT 50
        """
    },
    
    "Q20: Store Quarterly Sales View": {
        "desc": "Queries the view for quarterly sales by store",
        "sql": """
            SELECT * FROM STORE_QUARTERLY_SALES 
            ORDER BY Year DESC, Quarter, Store_Name
            LIMIT 50
        """
    },
}

# Sidebar for credentials
with st.sidebar:
    st.markdown("### Database Connection")
    host = st.text_input("Host", value="localhost")
    user = st.text_input("User", value="root")
    password = st.text_input("Password", type="password")
    database = st.text_input("Database", value="eesha")
    connect_btn = st.button("Connect", use_container_width=True)

    if connect_btn:
        try:
            import mysql.connector
            conn = mysql.connector.connect(
                host=host, user=user, password=password, database=database
            )
            cur = conn.cursor()
            cur.execute("SELECT COUNT(*) FROM Fact_Sales")
            total = cur.fetchone()[0]
            conn.close()
            st.session_state["db_ok"] = True
            st.session_state["db_cfg"] = dict(host=host, user=user, password=password, database=database)
            st.session_state["total_records"] = total
            st.success(f"Connected! {total:,} records in Fact_Sales")
        except Exception as e:
            st.session_state["db_ok"] = False
            st.error(f"Connection failed: {e}")

# Main content
if not st.session_state.get("db_ok"):
    st.markdown('<div class="status-box"><h3>Welcome</h3><p>Please enter your database credentials in the sidebar and click Connect to begin.</p></div>', unsafe_allow_html=True)
    st.stop()

total = st.session_state.get("total_records", "-")
st.markdown(f'<div class="status-box"><strong>Database Status:</strong> Connected to {st.session_state["db_cfg"]["database"]} ({total:,} records in Fact_Sales)</div>', unsafe_allow_html=True)

st.markdown("### Select Analysis Query")
query_name = st.selectbox("", list(QUERIES.keys()), label_visibility="collapsed")
run_btn = st.button("Run Query", type="primary", use_container_width=True)

st.divider()

if run_btn:
    cfg = st.session_state["db_cfg"]
    q = QUERIES[query_name]

    st.markdown("### Query Results")
    try:
        import mysql.connector
        conn = mysql.connector.connect(**cfg)
        t0 = time.time()
        df = pd.read_sql(q["sql"], conn)
        elapsed = time.time() - t0
        conn.close()

        if df.empty:
            st.warning("Query returned no results.")
        else:
            st.dataframe(df, use_container_width=True)
            st.caption(f"Rows returned: {len(df)} | Execution time: {elapsed:.2f} seconds")

    except Exception as e:
        st.error(f"Query error: {e}")
        st.code(q["sql"], language="sql")

    st.info(f"Description: {q['desc']}")

# Footer
st.markdown("""
<div class="footer">
  | Made by Eesha Emaan |
</div>
""", unsafe_allow_html=True)