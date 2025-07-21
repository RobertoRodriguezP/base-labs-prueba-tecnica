CREATE VIEW IF NOT EXISTS c1_Prep_VendorPurchaseDiversity AS
SELECT
    v.VendorName,
    COUNT(DISTINCT p.Description) AS unique_products_purchased
FROM VendorInvoicesDec v
JOIN PricingPurchasesDec p ON v.VendorNumber = p.VendorNumber
GROUP BY v.VendorName
ORDER BY unique_products_purchased DESC;
