import requests
import pymysql
import json
from flask import Flask, jsonify
from time import sleep
from threading import Thread

app = Flask(__name__)

response = list()


def fetchData():
    for page_num in range(1,10):
        r = requests.get('https://api.openbrewerydb.org/breweries?by_state=texas&per_page=50&page={}'.format(page_num))
        data_json = json.loads(r.text)
        if r is not None:
            response.append(data_json)

def onceADay():
    while True:
        fetchData()
        sleep(86400)


Thread(target=onceADay).start()

con = pymysql.connect(
    host='localhost',
    user='root',
    password='Poochie@123',
    db='jsonparsong',
    connect_timeout=2000
)
cursor = con.cursor(pymysql.cursors.DictCursor)


def validate_string(val):
    if val is not None:
        if type(val) is int:
            return str(val).encode('utf-8')
        else:
            return val


for data in response:
    for i in data:
        query = 'Insert ignore into new_table ('
        t1 = []
        first_item = True
        for x in i:
            xx = validate_string(i.get(x, None))
            t1.append(xx)
            if not first_item:
                # add a comma and space to the query if it's not the first item
                query += ', '
            # add the field name to the query
            query += x
            # mark that it's no longer the first item
            first_item = False

            # finish off the query string
        query += ') VALUES {}'
        # and send the query
        cursor.execute(query.format(tuple(t1)))
con.commit()
con.close()


@app.route('/', methods=['GET'])
def get_data():  # put application's code here
    con.ping(reconnect=True)
    cursor.execute('SELECT * FROM new_table')
    brewery_data = jsonify(cursor.fetchall())
    brewery_data.headers.add('Access-Control-Allow-Origin', 'http://localhost:3000')
    return brewery_data


if __name__ == '__main__':
    app.run()
