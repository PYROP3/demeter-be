from datetime import datetime, timedelta
from flask_socketio import SocketIO, emit
from flask import Flask, render_template, request, redirect, url_for
from tinydb import TinyDB, where
from tinydb.storages import JSONStorage
from tinydb.middlewares import CachingMiddleware
from tinydb_serialization import SerializationMiddleware
from tinydb_serialization.serializers import DateTimeSerializer

import pandas as pd

import plotly
import plotly.express as px
import json
import logging
import os

app = Flask(__name__)
app.logger.root.setLevel(logging.getLevelName(os.getenv('LOG_LEVEL') or 'DEBUG'))
socketio = SocketIO(app)

serialization = SerializationMiddleware(JSONStorage)#(CachingMiddleware(JSONStorage))
serialization.register_serializer(DateTimeSerializer(), 'TinyDate')

# con = sqlite3.connect('demeter_old.db', detect_types=sqlite3.PARSE_DECLTYPES)
# cur = con.cursor()
# cur.execute('CREATE TABLE hygro (timestamp timestamp, value integer, buoy integer)')

def _or(dictionary, key, default):
    return default if key not in dictionary else dictionary[key]

def tag_time(msg):
    return "[{}] {}".format(datetime.now().strftime("%d/%b/%Y %H:%M:%S"), msg)

@app.route("/")
def _index():
    return redirect(url_for('_history', querydate=datetime.today().date().strftime("%Y/%m/%d")))

@app.route("/history/<path:querydate>")
def _history(querydate=datetime.today().date().strftime("%Y/%m/%d")):
    app.logger.debug(f'querydate={querydate}')
    
    _date = datetime.strptime(querydate, "%Y/%m/%d").date()
    table = TinyDB('db/demeter.json', sort_keys=True, storage=serialization).table('hygro')#, cache_size=24*60/10)
    # app.logger.debug(f'table.all={table.all()}')
    
    tabledata = [row for row in table.all() if row['timestamp'].date() == _date]
    # TODO tabledata = table.search(where('timestamp').date == today)
    # app.logger.debug(f'tabledata={tabledata}')

    df = pd.DataFrame({
        "Time": [data['timestamp'] for data in tabledata],
        "Moisture": [data['hygro'] for data in tabledata]
    })

    fig = px.line(df.sort_values(by="Time"), x="Time", y="Moisture", markers=True)
    fig.update_layout(yaxis_range=[0,100])

    graphJSON = json.dumps(fig, cls=plotly.utils.PlotlyJSONEncoder)
    header=f"Detected moisture on {str(_date)}"
    description = f"""
    Measured moisture levels on {str(_date)}
    """
    return render_template('index.html', graphJSON=graphJSON, header=header,description=description,
                           prevDay=(_date - timedelta(days=1)).strftime("%Y/%m/%d"),
                           nextDay=(_date + timedelta(days=1)).strftime("%Y/%m/%d"))

@app.route("/log", methods=["POST"])
def _log(*args, **kwargs):
    data = request.get_json(force=True)
    app.logger.debug(f"/log -> {str(data)}")

    hygro = int(_or(data, 'hygro', 0))
    buoy = int(_or(data, 'buoy', 0))

    try:
        db = TinyDB('db/demeter.json', sort_keys=True, storage=serialization)
        table = db.table('hygro')#, cache_size=24*60/10)
        table.insert({'timestamp': datetime.now(), 'hygro': hygro, 'buoy': buoy})
        db.close()
        # cur.execute("INSERT INTO hygro VALUES (?, ?, ?)", (datetime.now(), hygro, buoy))
        # con.commit()
    except Exception as e:
        app.logger.warning(f"Failed to save entry ('{datetime.now().strftime('%d/%b/%Y %H:%M:%S')}',{hygro},{buoy}) : " + str(e))
    return "Success: {}".format(200)

@app.route("/getConfig/<device_id>")
def _get_config(device_id):
    db = TinyDB('db/demeter.json', sort_keys=True, storage=serialization)
    table = db.table('config')#, cache_size=24*60/10)
    tabledata = table.search(where('device_id') == device_id)

    app.logger.debug(f'tabledata={tabledata}')

    if tabledata is not None and len(tabledata) > 0:
        db.close()
        del(tabledata[0]['device_id'])
        return str(tabledata[0])
    
    app.logger.warning(f'Fetching default config ({device_id} not found)')

    tabledata = table.search(where('device_id') == 'default')
    db.close()
    del(tabledata[0]['device_id'])
    return str(tabledata[0])

@app.route("/setConfig/<device_id>", methods=["POST"])
def _set_config(device_id):
    data = request.get_json(force=True)
    app.logger.debug(f"/setConfig -> {str(data)}")

    data['device_id'] = device_id

    db = TinyDB('db/demeter.json', sort_keys=True, storage=serialization)
    table = db.table('config')#, cache_size=24*60/10)
    table.upsert(data, where('device_id') == device_id)
    db.close()

    return "Success: {}".format(200)

if __name__ == '__main__':
    try:
        app.logger.info("Starting flask app")
        socketio.run(app, host='0.0.0.0', port=5001)
    except KeyboardInterrupt:
        app.logger.info("Captured KeyboardInterrupt, exiting cleanly")
        exit(0)
