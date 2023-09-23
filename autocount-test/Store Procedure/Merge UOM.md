CREATE PROCEDURE [dbo].[bsp_ChangeItemUOMMerge] (@ItemCode d_ItemCode, @OldItemUOM d_UOM, @NewItemUOM d_UOM) AS

IF @OldItemUOM = @NewItemUOM

RETURN

BEGIN TRANSACTION

UPDATE ItemUOM

SET BalQty= COALESCE(BalQty , 0) + COALESCE((SELECT B.BalQty FROM ItemUOM B WHERE [B.ItemCode=@ItemCode](mailto:B.ItemCode=@ItemCode) AND [B.UOM=@OldItemUOM](mailto:B.UOM=@OldItemUOM) ), 0)

WHERE ItemCode= @ItemCode AND UOM =@NewItemUOM

IF @@ERROR <> 0

BEGIN

ROLLBACK TRANSACTION

RETURN

END

UPDATE ItemBalQty

SET BalQty= COALESCE(BalQty , 0) + COALESCE((SELECT B.BalQty FROM ItemBalQty B WHERE [B.ItemCode=@ItemCode](mailto:B.ItemCode=@ItemCode) AND [B.UOM=@OldItemUOM](mailto:B.UOM=@OldItemUOM)), 0)

WHERE ItemCode= @ItemCode AND UOM =@NewItemUOM

IF @@ERROR <> 0

BEGIN

ROLLBACK TRANSACTION

RETURN

END

UPDATE ItemBatchBalQty

SET BalQty= COALESCE(BalQty , 0) + COALESCE((SELECT B.BalQty FROM ItemBatchBalQty B WHERE [B.ItemCode=@ItemCode](mailto:B.ItemCode=@ItemCode) AND [B.UOM=@OldItemUOM](mailto:B.UOM=@OldItemUOM)), 0)

WHERE ItemCode= @ItemCode AND UOM =@NewItemUOM

IF @@ERROR <> 0

BEGIN

ROLLBACK TRANSACTION

RETURN

END

declare @sql nvarchar (200)

declare @dec nvarchar (100)

DECLARE @colName  NVARCHAR (50)

declare @cur_item cursor

--Update related tables

DECLARE @ListTable TABLE (TableName nvarchar (50), ColumnName nvarchar( 50), ItemCodeName nvarchar(50 ))

INSERT @ListTable VALUES ('Item', 'SalesUOM', 'ItemCode')

INSERT @ListTable VALUES ('Item', 'PurchaseUOM', 'ItemCode')

INSERT @ListTable VALUES ('Item', 'ReportUOM', 'ItemCode')

INSERT @ListTable VALUES ('Item', 'BaseUOM', 'ItemCode')

INSERT @ListTable VALUES('ItemBalQty', 'UOM', 'ItemCode')

INSERT @ListTable VALUES('ItemBatchBalQty', 'UOM', 'ItemCode')

INSERT @ListTable VALUES ('ItemPrice', 'UOM', 'ItemCode')

INSERT @ListTable VALUES ('ItemOpening', 'UOM', 'ItemCode')

INSERT @ListTable VALUES ('IPHIST', 'UOM', 'ItemCode')

INSERT @ListTable VALUES ('PackageDTL', 'UOM', 'ItemCode')

INSERT @ListTable VALUES ('QTDTL', 'UOM', 'ItemCode')

INSERT @ListTable VALUES ('SODTL', 'UOM', 'ItemCode')

INSERT @ListTable VALUES ('DODTL', 'UOM', 'ItemCode')

INSERT @ListTable VALUES ('IVDTL', 'UOM', 'ItemCode')

INSERT @ListTable VALUES ('CSDTL', 'UOM', 'ItemCode')

INSERT @ListTable VALUES ('CNDTL', 'UOM', 'ItemCode')

INSERT @ListTable VALUES ('DNDTL', 'UOM', 'ItemCode')

INSERT @ListTable VALUES ('XSDTL', 'UOM', 'ItemCode')

INSERT @ListTable VALUES ('DRDTL', 'UOM', 'ItemCode')

INSERT @ListTable VALUES ('RQDTL', 'UOM', 'ItemCode')

INSERT @ListTable VALUES ('PODTL', 'UOM', 'ItemCode')

INSERT @ListTable VALUES ('GRDTL', 'UOM', 'ItemCode')

INSERT @ListTable VALUES ('PIDTL', 'UOM', 'ItemCode')

INSERT @ListTable VALUES ('CPDTL', 'UOM', 'ItemCode')

INSERT @ListTable VALUES ('PRDTL', 'UOM', 'ItemCode')

INSERT @ListTable VALUES ('XPDTL', 'UOM', 'ItemCode')

INSERT @ListTable VALUES ('GTDTL', 'UOM', 'ItemCode')

INSERT @ListTable VALUES ('CSGNItemBalQty', 'UOM', 'ItemCode')

INSERT @ListTable VALUES ('CSGNDTL', 'UOM', 'ItemCode')

INSERT @ListTable VALUES ('CSGNXFERDTL', 'UOM', 'ItemCode')

INSERT @ListTable VALUES ('SupplierCSGNDTL', 'UOM', 'ItemCode')

INSERT @ListTable VALUES ('XFERDTL', 'UOM', 'ItemCode')

INSERT @ListTable VALUES ('ADJDTL', 'UOM', 'ItemCode')

INSERT @ListTable VALUES ('ISSDTL', 'UOM', 'ItemCode')

INSERT @ListTable VALUES ('RCVDTL', 'UOM', 'ItemCode')

INSERT @ListTable VALUES ('WOFFDTL', 'UOM', 'ItemCode')

INSERT @ListTable VALUES ('UpdateCostDTL', 'UOM', 'ItemCode')

INSERT @ListTable VALUES ('ItemCostHistory', 'UOM', 'ItemCode')

INSERT @ListTable VALUES ('UTDStockCost', 'UOM', 'ItemCode')

INSERT @ListTable VALUES ('StockDTL', 'UOM', 'ItemCode')

INSERT @ListTable VALUES ('UOMConvDTL', 'FromUOM', 'ItemCode')

INSERT @ListTable VALUES ('UOMConvDTL', 'ToUOM', 'ItemCode')

INSERT @ListTable VALUES ('PRProcessing', 'RequestUOM', 'ItemCode')

INSERT @ListTable VALUES ('PRProcessingPO', 'OrderUOM', 'ItemCode')

INSERT @ListTable VALUES ('DRProcessing', 'DeliveryUOM', 'ItemCode')

INSERT @ListTable VALUES ('DRProcessingDO', 'DeliveryUOM', 'ItemCode')

INSERT @ListTable VALUES ('ItemLocationPrice', 'UOM', 'ItemCode')

INSERT @ListTable VALUES ('ItemCurrencyPrice', 'UOM', 'ItemCode')

DECLARE @tableName  NVARCHAR (50)

DECLARE @itemCodeName  NVARCHAR (50)

set @dec = '@NewItemUOM d_UOM, @OldItemUOM d_UOM, @ItemCodeName d_ItemCode'

SET @cur_Item = CURSOR FAST_FORWARD FOR SELECT TableName , ColumnName, ItemCodeName From @ListTable

OPEN @cur_Item

FETCH NEXT FROM @cur_Item INTO @tableName, @colName, @itemCodeName

WHILE @@FETCH_STATUS = 0

BEGIN

set @sql = 'update [' + @tableName + '] SET [' + @colName + '] = @NewItemUOM WHERE [' + @colName + '] = @OldItemUOM AND [' + @itemCodeName + '] = @ItemCodeName'

Exec sp_executesql @sql, @dec, @NewItemUOM , @OldItemUOM, @ItemCode

IF @@ERROR <> 0

BEGIN

ROLLBACK TRANSACTION

CLOSE @cur_Item

DEALLOCATE @cur_Item

RETURN

END

FETCH NEXT FROM @cur_Item INTO @tableName, @colName, @itemCodeName

END

CLOSE @cur_Item

DEALLOCATE @cur_Item

-- Finally, delete @OldItemUOM

DELETE FROM ItemBalQty WHERE ItemCode = @ItemCode AND UOM = @OldItemUOM

DELETE FROM ItemBatchBalQty WHERE ItemCode = @ItemCode AND UOM = @OldItemUOM

DELETE FROM ItemUOM WHERE ItemCode = @ItemCode AND UOM = @OldItemUOM

IF @@ERROR <> 0

BEGIN

ROLLBACK TRANSACTION

RETURN

END

COMMIT TRANSACTION

GO

Execute bsp_ChangeItemUOMMerge @ItemCode='TI 923-447' , @OldItemUOM= 'SET', @NewItemUOM='1L'

DROP PROCEDURE bsp_ChangeItemUOMMerge
