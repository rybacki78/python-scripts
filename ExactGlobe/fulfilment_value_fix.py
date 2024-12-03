import pyodbc
from secrets.credentials import *

choose = False

while choose == False:
    environment = input("Want to run against production or test DB (p/t)? ")
    if environment == "p":
        connectionString = f"DRIVER={{ODBC Driver 18 for SQL Server}};SERVER={
            SERVER};DATABASE={DATABASE};UID={USERNAME};PWD={PASSWORD};TrustServerCertificate=yes;"
        print("*********** WARNING RUNNING AGAINST PRODUCTION DATABASE ***********")
        choose = True
    elif environment == "t":
        connectionString = f"DRIVER={{ODBC Driver 18 for SQL Server}};SERVER={SERVER_TEST};DATABASE={
            DATABASE_TEST};UID={USERNAME};PWD={PASSWORD};TrustServerCertificate=yes;"
        choose = True

conn = pyodbc.connect(connectionString)
cursor = conn.cursor()

print("Enter date.")
year = int(input("Year: "))
month = int(input("Month: "))
print("-------------")

SELECT_QUERY = """SELECT g.artcode, g.bkstnr, g.bdr_hfl, i.CostPriceStandard, g.project, g.aantal from gbkmut g WITH (NOLOCK)
left join items i WITH (NOLOCK) on g.artcode = i.ItemCode
where g.reknr = 810000 and g.bkjrcode = ? and g.periode = ? and g.project is not NULL and g.bkstnr is not NULL and g.project != 'OPAKOWANIA' order by g.project"""

cursor.execute(SELECT_QUERY, year, month)

records = cursor.fetchall()
print(
    "project\tartcode\tdoc_number\tfulfill_value\tcostprice\tpieces\tdiff [%]".expandtabs(
        30
    )
)

docnumber_list = []
artcode_list = []
costprice_list = []

for r in records:
    if r.aantal != 0 and r.CostPriceStandard != 0:
        diff = round(r.bdr_hfl / r.aantal / r.CostPriceStandard * 100)
    else:
        diff = round(r.bdr_hfl * 100)
    if (abs(diff) < 90 or abs(diff) > 110) and r.aantal != 0:
        print(
            f"{r.project}\t{r.artcode}\t{r.bkstnr}\t{r.bdr_hfl}\t{
              r.CostPriceStandard}\t{round(r.aantal)}\t{diff}%".expandtabs(
                30
            )
        )
        docnumber_list.append(r.bkstnr)
        artcode_list.append(r.artcode)
        costprice_list.append(r.CostPriceStandard)

lists_len = len(docnumber_list)

print("-------------")
update = ""
if docnumber_list:
    update = input("Do you want to correct these fulfillments (y/n)? ")

if update == "y":
    UPDATE_QUERY_GBKMUT = """UPDATE gbkmut SET bdr_hfl = ?, bdr_val = ? WHERE reknr = ? and bkstnr = ? AND artcode = ?"""
    UPDATE_QUERY_AMUTAS = """UPDATE amutas SET bedrag = ?, val_bdr = ? WHERE reknr = ? and bkstnr = ? AND artcode = ?"""
    for i in range(lists_len):
        cursor.execute(
            UPDATE_QUERY_GBKMUT,
            -abs(costprice_list[i]),
            -abs(costprice_list[i]),
            "   302000",
            int(docnumber_list[i]),
            artcode_list[i],
        )
        cursor.execute(
            UPDATE_QUERY_GBKMUT,
            costprice_list[i],
            costprice_list[i],
            "   810000",
            int(docnumber_list[i]),
            artcode_list[i],
        )

        cursor.execute(
            UPDATE_QUERY_AMUTAS,
            -abs(costprice_list[i]),
            -abs(costprice_list[i]),
            "   302000",
            int(docnumber_list[i]),
            artcode_list[i],
        )
        cursor.execute(
            UPDATE_QUERY_AMUTAS,
            costprice_list[i],
            costprice_list[i],
            "   810000",
            int(docnumber_list[i]),
            artcode_list[i],
        )
    conn.commit()
    print("done.")

print("bye.")

cursor.close()
conn.close()
