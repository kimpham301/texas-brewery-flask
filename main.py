import requests
import pymysql
import json
from flask import Flask, jsonify
from threading import Timer

app = Flask(__name__)

response = list()


def fetchData():
    for page_num in range(1,10):
        r = requests.get('https://api.openbrewerydb.org/breweries?by_state=texas&per_page=50&page={}'.format(page_num))
        data_json = json.loads(r.text)
        response.append(data_json)
    t = Timer(60*60*24, fetchData)
    t.start()

fetchData()


con = pymysql.connect(
    host='localhost',
    user='root',
    password='password',
    db='jsonparsing',
    connect_timeout=1000
)
cursor = con.cursor(pymysql.cursors.DictCursor)
create_table = '''CREATE TABLE IF NOT EXISTS brewery_data (
       id VARCHAR(50) NOT NULL PRIMARY KEY, name VARCHAR(50) NOT NULL, brewery_type VARCHAR(20), street VARCHAR(20), address_2 VARCHAR(20), address_3 VARCHAR(20),
       city VARCHAR(20), state VARCHAR(20), county_province VARCHAR(20), postal_code VARCHAR(30),
       country VARCHAR(20), longitude VARCHAR(20), latitude VARCHAR(20), phone VARCHAR(20), website_url VARCHAR(100),
       updated_at VARCHAR(50), created_at VARCHAR(50), None VARCHAR(20) )'''
cursor.execute(create_table)

def validate_string(val):
    if val is not None:
        if type(val) is int:
            return str(val).encode('utf-8')
        else:
            return val


for data in response:
    for i in data:
        query = 'INSERT IGNORE INTO brewery_data ('
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
