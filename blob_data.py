'''
Standard SQL defines BLOB as the binary large object for storing binary data in 
the database. With the BLOB data type, you can store the content of a picture, 
a document, etc. into the table.

PostgreSQL does not support BLOB but you can use the BYTEA data type for storing the binary data.
'''

'''
Insert BLOB into a table
To insert BLOB data into a table, you use the following steps:
First, read data from a file.
Next, connect to the PostgreSQL database by creating a new connection object from the connect() function.
Then, create a cursor object from the connection object.
After that, execute the INSERT statement with the input values. For BLOB data, you use the Binary object of the psycopg module
Finally, commit the changes permanently to the PostgreSQL database by calling the commit() method of the connection object.
'''

import psycopg2
from config import config
 
 
def write_blob(part_id, path_to_file, file_extension):
    """ insert a BLOB into a table """
    conn = None
    try:
        # read data from a picture
        drawing = open(path_to_file, 'rb').read()
        # read database configuration
        params = config()
        # connect to the PostgresQL database
        conn = psycopg2.connect(**params)
        # create a new cursor object
        cur = conn.cursor()
        # execute the INSERT statement
        cur.execute("INSERT INTO part_drawings(part_id,file_extension,drawing_data) " +
                    "VALUES(%s,%s,%s)",
                    (part_id, file_extension, psycopg2.Binary(drawing)))
        # commit the changes to the database
        conn.commit()
        # close the communication with the PostgresQL database
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


def read_blob(part_id, path_to_dir):
    """ read BLOB data from a table """
    conn = None
    try:
        # read database configuration
        params = config()
        # connect to the PostgresQL database
        conn = psycopg2.connect(**params)
        # create a new cursor object
        cur = conn.cursor()
        # execute the SELECT statement
        cur.execute(""" SELECT part_name, file_extension, drawing_data
                        FROM part_drawings
                        INNER JOIN parts on parts.part_id = part_drawings.part_id
                        WHERE parts.part_id = %s """,
                    (part_id,))
 
        blob = cur.fetchone()
        open(path_to_dir + blob[0] + '.' + blob[1], 'wb').write(blob[2])
        # close the communication with the PostgresQL database
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()

if __name__ == '__main__':
    # write_blob(1, 'images/simtray.jpg', 'jpg')
    # write_blob(2, 'images/speaker.jpg', 'jpg')
    # write_blob(3, 'images/winter.jpg', 'jpg')
    # write_blob(4, 'images/winter2.jpg', 'jpg')

    read_blob(1, '.\\images\\blob\\')