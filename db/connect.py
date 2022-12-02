import pymysql as mysql

conn = mysql.connect(
    host="localhost",
    port=3306,
    user="sanhak",
    password="sanhak",
    database="hyundaitransys",
    charset="utf8"
)
