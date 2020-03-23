library(tidyr)
library(lubridate)
library(forcats)
library(dplyr)
library(tidyverse)
library(DBI)
library(RPostgreSQL)
library(plyr)
library(conflicted)
library(forecast)
library(data.table)
library(tidyverse)
library(tidyquant)
library(timetk)
library(sweep)
library(forecast)
library(modelr)
library(fpp)

#customers
#renamingCustomers
Customers <- v1(customers)[1] <- "Id"
Customers(v1) <- c("Id")
customers$v1 <- rename()
Customers <- Customers %>%
  rename(customerNumber = CustomerNumber)
Customers <- Customers %>%
  rename(Customer_Name = V2)
Customers <- Customers %>%
  rename(contactLastName = V3)
Customers <- Customers %>%
  rename(contactFirstName = V4)
Customers <- Customers %>%
  rename(phone = V5)
Customers <- Customers %>%
  rename(addressLine1 = V6)
Customers <- Customers %>%
  rename(addressLine2 = V7)
Customers <- Customers %>%
  rename(city = V8)
Customers <- Customers %>%
  rename(state = V9)
Customers <- Customers %>%
  rename(postalCode = V10)
Customers <- Customers %>%
  rename(country = V11)
Customers <- Customers %>%
  rename(salesRepEmployeeNumber = V12)
Customers <- Customers %>%
  rename(creditLimit = V13)
Customers <- Customers %>%
  rename(employeeNumber = salesRepEmployeeNumber)
#employees
Employees <- Employees %>%
  dplyr::rename(employeeNumber = V1)
Employees <- Employees %>%
  dplyr:: rename(lastName = V2)
Employees <- Employees %>%
  dplyr::  rename(firstName = V3)
Employees <- Employees %>%
  dplyr::rename(extension = V4)
Employees <- Employees %>%
  dplyr::rename(email = V5)
Employees <- Employees %>%
  dplyr:: rename(officeCode = V6)
Employees <- Employees %>%
  dplyr:: rename(reportTo = V7)
Employees <- Employees %>%
  dplyr::rename(jobTitle = V8)

#orderDetails
OrderDetails <- OrderDetails %>%
  rename(orderNumber = V1)
OrderDetails <- OrderDetails %>%
  rename(productCode = V2)
OrderDetails <- OrderDetails %>%
  rename(quantityOrdered = V3)
OrderDetails <- OrderDetails %>%
  rename(priceEach = V4)
OrderDetails <- OrderDetails %>%
  rename(orderLineNumber = V5)

#orders
Orders <- Orders %>%
  dplyr::rename(orderNumber = X1)
Orders <- Orders %>%
  dplyr:: rename(orderDate = X2)
Orders <- Orders %>%
  dplyr::rename(requiredDate = X3)
Orders <- Orders %>%
  dplyr::rename(shippedDate = X4)
Orders <- Orders %>%
  dplyr::rename(status = X5)
Orders <- Orders %>%
  dplyr::rename(comments = X6)
Orders <- Orders %>%
  dplyr::rename(customerNumber = X7)

#Payments
Payments <- Payments %>%
  dplyr::rename(customerNumber = X1)
Payments <- Payments %>%
  dplyr::rename(checkNumber = X2)
Payments <- Payments %>%
  dplyr:: rename(paymentDate = X3)
Payments <- Payments %>%
  dplyr::rename(amount = X4)

#Products
Products <- Products %>%
  rename(productCode = V1)
Products <- Products %>%
  rename(productName = V2)
Products <- Products %>%
  rename(productLine = V3)
Products <- Products %>%
  rename(productScale = V4)
Products <- Products %>%
  rename(productVendor = V5)
Products <- Products %>%
  rename(productDescription = V6)
Products <- Products %>%
  rename(quantityInStock = V7)
Products <- Products %>%
  rename(buyPrice = V8)
Products <- Products %>%
  rename(MSRP = V9)

#ProductLines
ProductLines <- ProductLines %>%
  rename(productLine = V1)
ProductLines <- ProductLines %>%
  rename(textDescription = V2)

#offices
Offices <- Offices %>%
  rename( officeCode= V1)
Offices <- Offices %>%
  rename( city= V2)
Offices <- Offices %>%
  rename( phone= V3)
Offices <- Offices %>%
  rename( addressLine1= V4)
Offices <- Offices %>%
  rename( addressLine2= V5)
Offices <- Offices %>%
  rename( state= V6)
Offices <- Offices %>%
  rename( country= V7)
Offices <- Offices %>%
  rename( postalCode= V8)
Offices <- Offices %>%
  rename( territory= V9)


#create new table customers and sales
Orders <- dplyr::full_join(Customers, Orders)
Orders <- Orders %>%  select(-contactFirstName, -contactLastName, -phone, -addressLine1, -addressLine2, -city, -state, -postalCode)
Orders <- Orders %>%
  dplyr::rename( employeeNumber= salesRepEmployeeNumber)

#create new table employees and sales
Employees_customers <- dplyr::full_join(Customers, Employees)
Employees_customers <- Employees_customers %>%  select(-country, -reportTo,-Customer_Name, -lastName, -firstName, -jobTitle,-contactFirstName, -contactLastName, -phone, -addressLine1, -addressLine2, -city, -state, -postalCode, -extension, -email, -officeCode)
Employees <- Employees %>%  select(-jobTitle)
#create new table payments and products
Customerpay <- dplyr::full_join(PaymentsSum, Customers)

Orders <- Orders %>%
  select(-checkNumber, -paymentDate)
PaymentsSum <- Payments %>%
  select(customerNumber, amount)%>%
  dplyr::group_by(customerNumber)%>%
  dplyr::summarise(amount= sum(amount, na.rm = TRUE))%>%
  dplyr::distinct() %>%
  dplyr::ungroup() %>%
  dplyr::mutate(idpay = row_number())
 dplyr:: mutate(idLateStatus=row_number())
Payments <- Payments %>%  select(-idLateStatus, -Customer_Name, -country, -creditLimit, -orderDate, -requiredDate, -shippedDate, -status, -comments, -number_days, -LateValue, -idLateStatus)%>%
  Payments <- Payments %>%  dplyr::mutate(idpay = row_number())

Orders <- Orders %>% 
  select(-checkNumber, -paymentDate)


#create a table for obtaining amount of payments over the years. 
Sales.Years <- Payments %>%
  dplyr:: select(amount, paymentDate, customerNumber) %>%
  dplyr::rename(amount = amount, years = paymentDate, customerNumber=customerNumber) %>%
  dplyr::arrange(customerNumber,amount, years) %>%
  dplyr::group_by(customerNumber,amount,years) %>%
  dplyr::distinct() %>%
  dplyr::ungroup() %>%
  dplyr::mutate(Salesyears = years)

Sales.Years <- Sales.Years %>%
  dplyr::mutate(idLateStatus = row_number())

#chaging date to just years
Sales.Years$Salesyears <-  format(as.Date(Sales.Years$Salesyears, format="%Y-%m-%d","%Y"))
Sales.Years$months <-  format(as.Date(Sales.Years$years, format="%Y-%m-%d","%m"))
Sales.Years$Salesyears <-strftime(Sales.Years$Salesyears,format="%Y")
Sales.Years$months <-strftime(Sales.Years$months,format="%m")
#create column
SalesYears$years <- aggregate(SalesYears$years, by=list(SalesYears$years), sum)
 
#creating table grouped by years
SalesYears <- SalesYears %>%
  select(amount, years) %>%
  rename(amount = amount, years = years) %>%
  arrange(amount, years) %>%
  group_by(amount,years) %>%
  summarise(years)%>%
  distinct() %>%
  ungroup() %>%
  mutate(SalesYears = row_number())
class(SalesYears$years)

#grouping by years
SalesYears<-SalesYears %>%
  group_by(years) %>%
  summarize(amount = sum(amount))


#calculating revenue and compare to amount
#Products
OrderDetails <- OrderDetails %>%
  mutate(Revenue= OrderDetails$quantityOrdered * OrderDetails$priceEach)
OrderDetails <- dplyr::full_join(Orders, OrderDetails)
OrderDetails <- OrderDetails %>%
  select(-customerNumber, -Customer_Name, -country, -employeeNumber, -creditLimit, -number_days,-LateValue, -idLateStatus, -requiredDate, -shippedDate, -status, -comments)
OrderDetails$orderDate <-  format(as.Date(OrderDetails$orderDate, format="%Y-%m-%d"))
class(OrderDetails$orderDate)
# counting rows - customers per employee
Employees.2 <- nrow(employees$employeeNumber %.% group_by(employees$customerNumber))
mutate(clients.employee = count(Employees, Employees$employeeNumber))

#new column for clientes per employee
Employees_freq <- aggregate(cbind(clients.per.employee = Employees_customers$employeeNumber) ~ Employees_customers$employeeNumber, 
          data = Employees_customers, 
          FUN = function(x){NROW(x)})
#rename column
Employees_freq <- Employees_freq %>%
  dplyr::rename(employeeNumber = "Employees_customers$employeeNumber")

#creating new table
Employees_freq <- dplyr::full_join(Employees, Employees_freq)
Employees_freq <- Employees_freq %>%
  select(-customerNumber, -Customer_Name, -country, -creditLimit,) 
Employees_freq <- Employees_freq[!is.na(Employees_freq$employeeNumber),]

#calculating frequency of customers per employees
Employees_freq<-Employees_freq %>% group_by(employeeNumber,firstName,lastName, reportTo, jobTitle, clients.per.employee) %>%
Employees_freq<-subset(Employees_freq, jobTitle=="President" && jobTitle=="VP Marketing")
Employees_freq<- Employees_freq[!( Employees_freq$jobTitle=="President" &  Employees_freq$clients.per.employee==1),]
Employees_freq<- Employees_freq[!( Employees_freq$jobTitle=="VP Sales" &  Employees_freq$clients.per.employee==1),]
Employees_freq<- Employees_freq[!( Employees_freq$jobTitle=="VP Marketing" &  Employees_freq$clients.per.employee==1),]
Employees_freq<- Employees_freq[!( Employees_freq$jobTitle=="Sales Manager (APAC)" &  Employees_freq$clients.per.employee==1),]
Employees_freq<- Employees_freq[!( Employees_freq$jobTitle=="Sale Manager (EMEA)" &  Employees_freq$clients.per.employee==1),]
Employees_freq<- Employees_freq[!( Employees_freq$jobTitle=="Sales Manager (NA)" &  Employees_freq$clients.per.employee==1),]

Employees_customers<- Employees_customers %>%
  select(-Customer_Name, -country, -lastName, -firstName, -reportTo, -jobTitle) %>%

#create table payments per employee
Employees_payments<- dplyr::full_join(Payments, Employees_customers)
select(-checkNumber, -paymentDate) %>%
Employees_payments<-  Employees_payments %>%  select(-checkNumber, -paymentDate, -id, -creditLimit) 
Employees_payments<-  Employees_payments %>% select(-id)
Employees_payments<-  Employees_payments %>% select (-creditLimit)
Employees_payments <- Employees_payments[!is.na(Employees_payments$customerNumber),]

Employees_payments <- Employees_payments %>% 
  select(customerNumber, amount, employeeNumber) %>%
  group_by(employeeNumber) %>%
  dplyr::summarise(amount= sum(amount, na.rm = TRUE))%>%
  distinct() %>%
  ungroup() %>%
  dplyr::mutate(id = row_number())

#take data away
Employees_payments<-  Employees_payments %>%   
  select(-customerNumber)

#modify table
Employees_payments<- Employees_payments %>% 
  select(amount, employeeNumber) %>%
  group_by(employeeNumber) %>%
  dplyr::summarise(amount= sum(amount))%>%
  distinct() %>%
  ungroup() %>%
  dplyr::mutate(id = row_number())

#join Productivity with freq
Productivity <- dplyr::full_join(Employees_payments, Employees_freq)
Productivity <- Productivity %>% select(-lastName, -firstName, -extension, -email, -officeCode, -reportTo, -jobTitle)
Productivity <- Productivity [!is.na(Productivity$employeeNumber),]
Productivity <- Productivity %>%
  dplyr::mutate( amount.clients = amount/clients.per.employee)


#calculating market share 
MarketShare <- Sales.Years %>%
  select(amount, years, customerNumber, Salesyears) %>%
  group_by(Salesyears) %>%
  dplyr::summarise(amount= sum(amount))%>%
  distinct() %>%
  ungroup() %>%
  dplyr::mutate(id = row_number())

#making date format
as.Date(Orders$orderDate, format = "%Y/%m/%d")
as.Date(Orders$shippedDate, format = "%Y/%m/%d")

#making date format
class(Orders$orderDate)

#getting status of late or not
#calculate dates for shipping
Orders <- Orders %>%
  dplyr::mutate(orderDate = ymd(`orderDate`), shippedDate = ymd(`shippedDate`),
         number_days = interval(orderDate,shippedDate)/ddays(),
         LateValue = if_else(number_days>3,"Late","NotLate"),
         idLateStatus = row_number())  
Orders <- Orders %>%
select(-idLateStatus)

#calculate dates for shipping
Orders <- Orders %>%
  dplyr::mutate(orderDate = ymd(`orderDate`), shippedDate = ymd(`shippedDate`),
                number_days = interval(orderDate,shippedDate)/ddays(),
                LateValue = if_else(number_days>3,"Late","NotLate"),
                idLateStatus = row_number())  
Orders <- Orders %>%
  select(-idLateStatus)

Orders_freq <- Orders %>%
  frequency(Shipped)
  select(orderNumber, status)
  group_by(orderNumber,status)
  dplyr::summarise(frequency= count(status))%>%
  distinct() %>%
  ungroup() %>%
  dplyr::mutate(id = row_number())
  as.data.frame(with(Orders(status, freq)))
  frequency(status)
   
Orders_freq <- aggregate(Orders, by=list(Orders$status), sum)
Orders_freq <-   Orders %>%
  select(orderNumber, status, customerNumber)
    
Orders_freq <-   
    select(orderNumber, status)
    group_by(orderNumber,status)

Orders_freq <-  
    Orders %>%
    select(orderNumber, status, customerNumber) %>%
    table(Orders_freq$status) %>%
    dplyr::mutate(orderNumber = (orderNumber))
      
Orders_freq <-  Orders_freq %>%
dplyr::rename(status = Var1)
dplyr::mutate(orderNumber = (orderNumber))
tsclean(Sales.Years)
Sales.Years$years = as.Date(Sales.Years$years)
      
Sales.Years$Salesyears <-  format(as.Date(Sales.Years$Salesyears, format="%Y-%m-%d","%Y"))
Sales.Years$months <-  format(as.Date(Sales.Years$years, format="%Y-%m-%d","%m"))
Sales.Years$Salesyears <-strftime(Sales.Years$Salesyears,format="%Y")
Sales.Years$months <-strftime(Sales.Years$months,format="%m")
      
#creating forecast
Forecast <- Payments
Forecast$ym <-  format(as.Date(Forecast$years, format="%Y-%m-%d","%Y-%m"))
Forecast$ym <- strftime(Forecast$years,format="%Y-%m")
Forecast$months <-  format(as.Date(Forecast$ym, format="%Y-%m","%m"))
Forecast$months <- strftime(Forecast$m,format="%m")
Forecast <- Forecast %>%
        select (amount,ym, Salesyears, months) %>%
        dplyr::group_by(ym, months, Salesyears) %>%
        dplyr::summarise(aym= sum(amount))%>%
        dplyr::distinct() %>%
        dplyr:: ungroup() %>%
        dplyr::mutate(id = row_number())
      
#checking status of colum home
class(Forecast$amount)
head(Forecast$amount)
forecast(Forecast)
plot(Forecast$amount, main = 'Sales per month')
ts <- ts(Forecast$aym, start=c(2003, 1), end=c(2005, 12), frequency=12)
     
##ggplot for sales month
Sales.Years %>%
ggplot(aes(x = months, y = amount, group = Salesyears)) +
geom_area(aes(fill = Salesyears), position = "fit") +
labs(title = "Sales per month", x = "", y = "Sales",
subtitle = "2003-2005") +
scale_y_continuous() +
theme_tq()
lines(Forecast, col="red")
     
#creating a model 
  model <- glm(formula = aym ~ id, data=Forecast)
  summary(model)
  ggplot(data = Forecast, aes(x=id, y=aym)) +
  geom_point() +
#add line
geom_smooth(method="lm", se=TRUE)
      
#predict next 6 months
Forecast <- Forecast %>%
m1 <- lm(aym ~ id, data = Forecast)
grid <- data.frame(id = seq(31, 36, length = 6))
grid <- grid %>% add_predictions(m1)
grid <- grid %>% dplyr::select(-predictions.id)
full_join(Forecast,grid)
dplyr::rename(amount = pred)
predict(m1, newdata = grid, interval = 'prediction', level=0.90)
confint(m1)
accurancy(grid$pred, payments$amount)
mean.pred.intervals(grid$id, grid$pred, grid)


ggplot(data = Forecast2, aes(x=id, y=aym))
geom_point() +
#add line
geom_smooth(method="lm", se=FALSE)

ggplot(data = Forecast2, aes(x=id, y=aym)) +
  geom_point() +
  #add line
  geom_smooth(method="lm", se=TRUE)


fit1 <-lm(formula = amount~Salesyears, data=Sales.Years)
#see graph
summary(fit1)

#end of the project

#connecting to Postgre
drv <- dbDriver("PostgreSQL")
con <- dbConnect(drv, port = 5432, host = "bronto.ewi.utwente.nl",
                 dbname = "dab_ds19201b_32", user = "dab_ds19201b_32", password = "R9qHVF9juAqgJJl7",
                 options="-c search_path=project")
dbWriteTable(con, "Employees", value = Employees, overwrite = T, row.names = F)
dbWriteTable(con, "Products", value = Products, overwrite = T, row.names = F)
dbWriteTable(con, "Orders", value = Orders, overwrite = T, row.names = F)
dbWriteTable(con, "Payments", value = Payments, overwrite = T, row.names = F)
dbWriteTable(con, "PaymentsSum", value = PaymentsSum, overwrite = T, row.names = F)
dbWriteTable(con, "Customers", value = Customers, overwrite = T, row.names = F)
dbWriteTable(con, "Customerpay", value = Customerpay, overwrite = T, row.names = F)

dbWriteTable(con, "OrderDetails", value = OrderDetails, overwrite = T, row.names = F)
dbWriteTable(con, "Sales.Years", value = Sales.Years, overwrite = T, row.names = F)
dbWriteTable(con, "Employees_freq", value = Employees_freq, overwrite = T, row.names = F)
dbWriteTable(con, "Productivity", value = Productivity, overwrite = T, row.names = F)
dbWriteTable(con, "Offices", value=Offices, overwrite = T, row.names = F)
dbWriteTable(con, "Forecast", value=Forecast, overwrite = T, row.names = F)
dbWriteTable(con, "grid", value=grid, overwrite = T, row.names = F)
dbWriteTable(con, "Forecast2", value=Forecast2, overwrite = T, row.names = F)


#checking the tables

dbListTables(con)
str(dbReadTable(con,"Employees"))
str(dbReadTable(con,"Employees_freq"))
str(dbReadTable(con,"Products"))
str(dbReadTable(con,"Orders"))
str(dbReadTable(con,"Payments"))
str(dbReadTable(con,"PaymentsSum"))
str(dbReadTable(con,"Customers"))
str(dbReadTable(con,"OrderDetails"))
str(dbReadTable(con,"Sales.Years"))
str(dbReadTable(con,"Productivity"))
str(dbReadTable(con,"Offices"))
str(dbReadTable(con,"Forecast2"))
str(dbReadTable(con,"grid"))