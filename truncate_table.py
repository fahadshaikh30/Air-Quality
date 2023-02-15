import pyodbc

conn = pyodbc.connect('Driver={SQL Server};'
                      'Server=sjc04ihsprddb.teslamotors.com;'
                      'Database=dAQI;'
                      'Trusted_Connection=yes;')
cursor = conn.cursor()

cursor.execute('''TRUNCATE TABLE t_pr_ds10333_AQIHourly''')

conn.commit()

conn.close()