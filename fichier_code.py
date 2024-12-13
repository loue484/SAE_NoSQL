# Importation des modules utilisés
import sqlite3
import pandas

# Création de la connexion
conn = sqlite3.connect("ClassicModel.sqlite")

# Récupération du contenu de Customers avec une requête SQL
customers = pandas.read_sql_query("SELECT * FROM Customers;", conn)
# print(customers)

# 1. Lister les clients n’ayant jamais effecuté une commande

client_sans_commande = pandas.read_sql_query(
    """
   SELECT Customers.customerNumber, Customers.customerName
   FROM Customers 
   LEFT JOIN Orders ON Customers.customerNumber = Orders.customerNumber
   WHERE Orders.customerNumber IS NULL;
   """,
    conn
)
# print(client_sans_commande)

# 2. Pour chaque employé, le nombre de clients, le nombre de commandes et le montant total de celles-ci

infos_employe = pandas.read_sql_query(
    """
   SELECT Employees.employeeNumber, Employees.firstName, Employees.lastName, 
       COUNT (DISTINCT Customers.customerNumber),
       COUNT (DISTINCT Orders.orderNumber),
       SUM (Payments.amount)
   FROM Employees 
   LEFT JOIN Customers ON Employees.employeeNumber = Customers.salesRepEmployeeNumber
   LEFT JOIN Orders ON Customers.customerNumber = Orders.customerNumber
   LEFT JOIN Payments ON Customers.customerNumber = Payments.customerNumber
   GROUP BY Employees.employeeNumber;
   """,
    conn
)
# print(infos_employe)

# 3. Idem pour chaque bureau (nombre de clients, nombre de commandes et montant total),
# avec en plus le nombre de clients d’un pays différent, s’il y en a

infos_bureau = pandas.read_sql_query(
    """
   SELECT Offices.officeCode, Offices.country, Offices.city,
       COUNT (DISTINCT Customers.customerNumber),
       COUNT (DISTINCT Orders.orderNumber),
       SUM (Payments.amount),
       COUNT (DISTINCT CASE 
                  WHEN Customers.country != Offices.country THEN Customers.customerNumber
                  ELSE NULL
            END) AS client_autre_pays
   FROM Offices
   LEFT JOIN Employees ON Offices.officeCode = Employees.officeCode
   LEFT JOIN Customers ON Employees.employeeNumber = Customers.salesRepEmployeeNumber
   LEFT JOIN Orders ON Customers.customerNumber = Orders.customerNumber
   LEFT JOIN Payments ON Customers.customerNumber = Payments.customerNumber
   GROUP BY Offices.officeCode;
   """,
    conn
)
#print(infos_bureau)

# 4. Pour chaque produit, donner le nombre de commandes, la quantité totale commandée, et le nombre de clients différents

infos_produit = pandas.read_sql_query(
    """
   SELECT Products.productCode, Products.productName,
       COUNT (DISTINCT Orders.orderNumber),
       SUM (OrderDetails.quantityOrdered),
       COUNT (DISTINCT Customers.customerNumber)
   FROM Products
   LEFT JOIN OrderDetails ON Products.productCode = OrderDetails.productCode
   LEFT JOIN Orders ON OrderDetails.orderNumber = Orders.orderNumber
   LEFT JOIN Customers ON Orders.customerNumber = Customers.customerNumber
   GROUP BY Products.productCode;
   """,
    conn
)
#print(infos_produit)

# 5. Donner le nombre de commande pour chaque pays du client, ainsi que le montant total des commandes 
# et le montant total payé : on veut conserver les clients n’ayant jamais commandé dans le résultat final

infos_pays_client = pandas.read_sql_query(
    """
   SELECT Customers.country,
       COUNT (DISTINCT Orders.orderNumber),
       SUM (OrderDetails.quantityOrdered*OrderDetails.priceEach),
       SUM (Payments.amount)
   FROM Customers
   LEFT JOIN Payments ON Customers.customerNumber = Payments.customerNumber
   LEFT JOIN Orders ON Customers.customerNumber = Orders.customerNumber
   LEFT JOIN OrderDetails ON Orders.orderNumber = OrderDetails.orderNumber
   GROUP BY Customers.country;
   """,
    conn
)
#print(infos_pays_client)

# 6. On veut la table de contigence du nombre de commande entre la ligne de produits et le pays du client

table_contingence_nb_commande = pandas.read_sql_query(
    """
   SELECT Customers.country, Products.productLine,
       COUNT (DISTINCT Orders.orderNumber)
   FROM Customers
   LEFT JOIN Orders ON Customers.customerNumber = Orders.customerNumber
   LEFT JOIN OrderDetails ON Orders.orderNumber = OrderDetails.orderNumber
   LEFT JOIN Products ON OrderDetails.productCode = Products.productCode
   GROUP BY Customers.country, Products.productLine;
   """,
    conn
)
#print(table_contingence_nb_commande)

# 7. On veut la même table croisant la ligne de produits et le pays du client, mais avec le montant 
# total payé dans chaque cellule

table_contingence_montant = pandas.read_sql_query(
    """
   SELECT  Products.productLine,Customers.country,
       SUM (Payments.amount)
   FROM Customers
   LEFT JOIN Orders ON Customers.customerNumber = Orders.customerNumber
   LEFT JOIN OrderDetails ON Orders.orderNumber = OrderDetails.orderNumber
   LEFT JOIN Products ON OrderDetails.productCode = Products.productCode
   LEFT JOIN Payments ON Customers.customerNumber = Payments.customerNumber
   GROUP BY  Products.productLine, Customers.country;
   """,
    conn
)
#print(table_contingence_montant)

# 8. Donner les 10 produits pour lesquels la marge moyenne est la plus importante (cf buyPrice et priceEach)

top10_marge_produit = pandas.read_sql_query(
    """
   SELECT Products.productCode, Products.productName,
       AVG (OrderDetails.priceEach - Products.buyPrice) AS marge_moyenne
   FROM Products
   LEFT JOIN OrderDetails ON Products.productCode = OrderDetails.productCode
   GROUP BY Products.productCode
   ORDER BY marge_moyenne DESC
   LIMIT 10;
   """,
    conn
)
#print(top10_marge_produit)

# 9. Lister les produits (avec le nom et le code du client) qui ont été vendus à perte :
#Si un produit a été dans cette situation plusieurs fois, il doit apparaître plusieurs fois,
#Une vente à perte arrive quand le prix de vente est inférieur au prix d’achat ;

produits_vente_a_perte = pandas.read_sql_query(
    """
   SELECT Products.productCode, Products.productName, Customers.customerNumber, Customers.customerName, OrderDetails.priceEach, Products.buyPrice
   FROM Products
   LEFT JOIN OrderDetails ON Products.productCode = OrderDetails.productCode
   LEFT JOIN Orders ON OrderDetails.orderNumber = Orders.orderNumber
   LEFT JOIN Customers ON Orders.customerNumber = Customers.customerNumber
   WHERE OrderDetails.priceEach < Products.buyPrice;
   """,
    conn
)
#print(produits_vente_a_perte)

#10. (bonus) Lister les clients pour lesquels le montant total payé est supérieur aux montants totals des achats

infos_montant_client = pandas.read_sql_query(
    """
   SELECT Customers.customerNumber, Customers.customerName, 
       COUNT (DISTINCT Orders.orderNumber),
       SUM (OrderDetails.quantityOrdered*OrderDetails.priceEach) AS montant_achat,
       SUM (Payments.amount) AS total_payé
   FROM Customers
   LEFT JOIN Payments ON Customers.customerNumber = Payments.customerNumber
   LEFT JOIN Orders ON Customers.customerNumber = Orders.customerNumber
   LEFT JOIN OrderDetails ON Orders.orderNumber = OrderDetails.orderNumber
   GROUP BY Customers.customerNumber
   HAVING total_payé > montant_achat;
   """,
    conn
)
print(infos_montant_client)

# Fermeture de la connexion : IMPORTANT à faire dans un cadre professionnel
conn.close()

