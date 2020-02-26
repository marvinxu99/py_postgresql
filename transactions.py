# https://www.postgresqltutorial.com/postgresql-python/transaction/

'''
In psycopg, the connection class is responsible for handling transactions. When you issue the first SQL 
statement to the PostgreSQL database using a cursor object, psycopg creates a new transaction. 
From that moment, psycopg executes all the subsequent statements in the same transaction. If any statement 
fails, psycopg will abort the transaction.

The connection class has two methods for terminating a transaction: commit() and rollback(). 
If you want to commit all changes to the PostgreSQL database permanently, you call the commit() method. 
And in case you want to cancel the changes, you call the rollback() method. Closing the connection object 
or destroying it using the  del will also result in an implicit rollback.
'''

'''
It is important to notice that a simple SELECT statement will start a transaction that may result in 
undesirable effects such as table bloat and locks. Therefore, if you are developing a long-living 
application, you should call the commit() or rollback() method before leaving the connection unused for a long time

Alternatively, you can set the  autocommit attribute of the connection object to True. This ensures that 
psycopg will execute every statement and commit it immediately.
'''
import psycopg2
from config import config
 
 
def add_part(part_name, vendor_list):
    # statement for inserting a new row into the parts table
    insert_part = "INSERT INTO parts(part_name) VALUES(%s) RETURNING part_id;"
    # statement for inserting a new row into the vendor_parts table
    assign_vendor = "INSERT INTO vendor_parts(vendor_id,part_id) VALUES(%s,%s)"
 
    conn = None
    try:
        params = config()
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        # insert a new part
        cur.execute(insert_part, (part_name,))
        # get the part id
        part_id = cur.fetchone()[0]
        # assign parts provided by vendors
        for vendor_id in vendor_list:
            cur.execute(assign_vendor, (vendor_id, part_id))
 
        # commit changes
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()

if __name__ == '__main__':
    add_part('SIM Tray', (1, 2))
    add_part('Speaker', (3, 4))
    add_part('Vibrator', (5, 6))
    add_part('Antenna', (6, 7))
    add_part('Home Button', (1, 5))
    add_part('LTE Modem', (1, 5))


'''
Starting from psycopg 2.5, the connection and cursor are Context Managers and 
therefore you can use them with the with statement:

with psycopg2.connect(dsn) as conn:
    with conn.cursor() as cur:
        cur.execute(sql)

Psycopg commits the transaction if no exception occurs within the with block, and otherwise it rolls back the transaction.
'''

'''
Unlike other context manager objects, existing the with block does not close the connection but only 
terminates the transaction. As the result, you can use the same connection object in the subsequent 
with statement in another transaction as follows:

conn = psycopg2.connect(dsn)
 
# transaction 1
with conn:
    with conn.cursor() as cur:
&nbsp;&nbsp;&nbsp;&nbsp;    cur.execute(sql)
 
# transaction 2
with conn:
    with conn.cursor() as cur:
        cur.execute(sql)
 
conn.close()

'''