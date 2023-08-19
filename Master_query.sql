SELECT [Transaction Date]
	  ,CONCAT_WS('.',[Year], [Month], [Day]) as 'Nepali Date' 
	  ,sysTran.[Transaction ID]
      ,[Bill Receiveable Person]
	  ,accProfInfo.[Vat Pan No]
	  ,STRING_AGG([Inventory Name], '/') as 'Item'
      ,SUM([Item In]) as 'In'
      ,SUM([Item Out]) as 'Out'
      ,amtTran.[Grand Total]
      ,amtTran.[Taxable Amount]
      ,amtTran.[Tax Amount]
      -- ,[Transaction Type]
  FROM [VatBillingSoftware].[dbo].[SystemTransaction] sysTran
  ,[VatBillingSoftware].[dbo].[SystemTransactionPurchaseSalesAmount] amtTran
  ,[VatBillingSoftware].[dbo].[AccountProfileProduct] accProfInfo
  ,[VatBillingSoftware].[dbo].[SystemCalenderDate]
  ,[VatBillingSoftware].[dbo].[SystemTransactionPurchaseSalesItem] psiTran
  ,[VatBillingSoftware].[dbo].[InventoryItem]
  WHERE sysTran.[Transaction Type] = 2 -- 1: Purchase 2: Sales
  AND [Transaction Date] BETWEEN '2023-05-15' AND '2023-06-15'
  AND sysTran.[Transaction ID] = amtTran.[Transaction ID]
  AND amtTran.[Account ID] = accProfInfo.[ACCOUNT ID]
  AND [Transaction Date] = [English Date]
  AND [Inventory Item Code] = [Inventory ID]
  AND psiTran.[Transaction ID] = sysTran.[Transaction ID]
  GROUP BY [Transaction Date], [Year], [Month], [Day], sysTran.[Transaction ID]
	  ,psiTran.[Transaction ID]
	  ,[Bill Receiveable Person]
	  ,accProfInfo.[Vat Pan No]
      ,amtTran.[Grand Total]
      ,amtTran.[Taxable Amount]
      ,amtTran.[Tax Amount]
  ORDER BY sysTran.[Transaction ID]
