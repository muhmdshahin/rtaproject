from confluent_kafka import Consumer
import mysql.connector
import pyodbc
import json
################

c=Consumer({'bootstrap.servers':'localhost:9092','group.id':'python-consumer','auto.offset.reset':'earliest'})

print('Available topics to consume: ', c.list_topics().topics)

c.subscribe(['user-tracker'])

################

def main():

    cnxn = pyodbc.connect(
        'DRIVER={SQL Server};SERVER=localhost;PORT=1433;DATABASE=Sales;UID=sa;PWD=yourStrong(!)Password;'
    )

    cursor = cnxn.cursor()
    # cursor.execute("SELECT * FROM SalesRecord")
    # rows = cursor.fetchall()
    # for row in rows:
    #     print(row)
    # cursor.close()

    while True:
        msg=c.poll(1.0) #timeout
        if msg is None:
            continue
        if msg.error():
            print('Error: {}'.format(msg.error()))
            continue
        data=msg.value().decode('utf-8')
        json_object = json.loads(data)
        print(json_object['item_id'])
        item_id = int(json_object['item_id'])
        status = json_object['status']
        created_at = json_object['created_at']
        qty_ordered = int(json_object['qty_ordered'])
        grand_total = int(float(json_object['grand_total']))
        category_name_1 = json_object['category_name_1']
        payment_method = json_object['payment_method']
        year = int(json_object['Year'])
        month = int(json_object['Month'])

        cursor.execute("INSERT INTO SalesRecord(item_id,status,created_at,qty_ordered,grant_total,category_name,payment_method,year,month)"
                        " VALUES(?,?,?,?,?,?,?,?,?)",
                        (item_id, status, created_at, qty_ordered, grand_total,
                        category_name_1, payment_method, year, month)
                       )
        cnxn.commit()

        # creating a single-element container.
        # placeholder = st.empty()
    cursor.close()

if __name__ == '__main__':
    main()