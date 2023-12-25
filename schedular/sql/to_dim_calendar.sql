INSERT INTO gold.dim_calendar (ID, Day, Month, Month_Name, Year, Quarter, Weekday, Day_Of_Week_Name, Is_Weekend)
    SELECT     
        d::DATE AS ID,
        EXTRACT(DAY FROM d) AS Day,
        EXTRACT(MONTH FROM d) AS Month,
        TO_CHAR(d, 'Month') AS Month_Name,
        EXTRACT(YEAR FROM d) AS Year,
        EXTRACT(QUARTER FROM d) AS Quarter,
        EXTRACT(ISODOW FROM d) AS Weekday,
        TO_CHAR(d, 'Day') AS Day_Of_Week_Name,
        CASE WHEN EXTRACT(ISODOW FROM d) IN (6, 7) THEN TRUE ELSE FALSE END AS Is_Weekend
    FROM generate_series(
        '2021-01-01'::DATE,
        '2024-12-31'::DATE,
        interval '1 day') AS d;