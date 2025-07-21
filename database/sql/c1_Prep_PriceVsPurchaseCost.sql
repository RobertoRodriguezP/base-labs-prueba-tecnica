CREATE VIEW IF NOT EXISTS c1_Prep_PriceVsPurchaseCost AS
SELECT
    Brand,
    Description,
    ROUND(AVG(Price), 2) AS avg_price,
    ROUND(AVG(PurchasePrice), 2) AS avg_purchase_cost,
    ROUND(AVG(Price - PurchasePrice), 2) AS avg_margin_dollars
FROM PricingPurchasesDec
GROUP BY Brand, Description;
