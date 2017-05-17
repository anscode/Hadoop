SELECT        CustomerKey, FirstName, MiddleName, LastName, Gender, EmailAddress
FROM            DimCustomer

SELECT        CustomerKey, ProductKey, TotalProductCost
FROM            FactInternetSales


SELECT        p.ProductKey, c.EnglishProductCategoryName, p.EnglishProductName
FROM            DimProduct AS p INNER JOIN
                         DimProductCategory AS c ON p.ProductSubcategoryKey = c.ProductCategoryKey



select * from DimCustomer c inner join FactInternetSales s on
c.CustomerKey=s.CustomerKey
where C.CustomerKey<11004



select  SalesOrderNumber, OrderDate, CustomerKey from AdventureWorksDW2012.dbo.FactInternetSales group by SalesOrderNumber, OrderDate, CustomerKey

select SalesOrderNumber,SalesAmount,ProductKey,SalesAmount from AdventureWorksDW2012.dbo.FactInternetSales 

insert into orders
select  CONVERT(int,replace(SalesOrderNumber,'SO','')), OrderDate, CustomerKey,'Closed' from AdventureWorksDW2012.dbo.FactInternetSales group by SalesOrderNumber, OrderDate, CustomerKey
go
insert into order_items
select ROW_NUMBER() OVER (order by (select 1)), CONVERT(int,replace(SalesOrderNumber,'SO','')),ProductKey,1,SalesAmount,SalesAmount from AdventureWorksDW2012.dbo.FactInternetSales 

truncate table order_items


SELECT       * 
FROM            orders INNER JOIN
                         order_items ON orders.order_id = order_items.order_item_order_id