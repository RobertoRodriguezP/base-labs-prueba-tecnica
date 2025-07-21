CREATE VIEW IF NOT EXISTS c1_Prep_EndInventoryByBrand AS
SELECT
    Brand,
    Description,
    SUM(onHand) AS total_units_on_hand,
    ROUND(SUM(onHand * Price), 2) AS total_inventory_value
FROM EndInvDec
GROUP BY Brand,Description
ORDER BY total_inventory_value DESC;
