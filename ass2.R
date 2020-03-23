
data0 <- read_delim(file = "/Users/mac/Desktop/data/BI_Raw_data.csv",
                               delim = ";", col_names = TRUE, col_types = NULL)
head(data0)
  

product1 <- BI_Raw_Data %>%
  select(Product_Name, Product_Category) %>%
  rename(pname = Product_Name, category = Product_Category) %>%
  arrange(pname, category) %>%
  group_by(pname, category) %>%
  distinct() %>%
  ungroup() %>%
  mutate(productid = row_number())


customer1 <- BI_Raw_Data %>%  select(Customer_Name, Customer_Country) %>%
  rename(cname = Customer_Name, country = Customer_Country) %>%                      
  arrange(cname, country) %>%
  group_by(cname, country) %>%
  distinct() %>%
  ungroup() %>%
  mutate(customerid = row_number())

sales1 <- BI_Raw_Data %>% 
select(Order_Date_Day, Product_Order_Price_Total, Product_Name, Product_Category, Customer_Country, Customer_Name) %>%
rename(date = Order_Date_Day, sales=Product_Order_Price_Total, pname=Product_Name, category=Product_Category, cname=Customer_Name, country=Customer_Country) %>%                         
arrange(sales, date, pname, cname, category, country) %>%
group_by(sales, date, pname, cname, category, country) %>%
distinct() %>%
ungroup() %>%
mutate(orderid = row_number())


#joining table
sales1 <- dplyr::full_join(product1, sales1)
sales1 <- dplyr::full_join(customer1, sales1)
#taking away rows
sales1 <- sales1 %>%  select(-pname, -cname, -category, -country)



#connecting into database
drv <- dbDriver("PostgreSQL")
con <- dbConnect(drv, port = 5432, host = "bronto.ewi.utwente.nl",
                 dbname = "dab_ds19201b_32", user = "dab_ds19201b_32", password = "R9qHVF9juAqgJJl7",
                 options="-c search_path=ass2")
dbWriteTable(con, "product", value = product1, overwrite = T, row.names = F)
dbWriteTable(con, "customer", value = customer1, overwrite = T, row.names = F)
dbWriteTable(con, "sales", value = sales1, overwrite = T, row.names = F)

dbListTables(con)
str(dbReadTable(con,"customer"))
str(dbReadTable(con,"sales"))
str(dbReadTable(con,"product"))

