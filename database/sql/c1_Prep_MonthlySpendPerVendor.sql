CREATE VIEW IF NOT EXISTS c1_Prep_MonthlySpendPerVendor AS
SELECT
    VendorName,
    strftime('%Y-%m', InvoiceDate) AS year_month,
    SUM(Dollars) AS total_spent
FROM VendorInvoicesDec
GROUP BY VendorName, year_month
ORDER BY VendorName, year_month;
