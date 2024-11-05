from flask import Flask, request, jsonify
import logging
import pyodbc

app = Flask(__name__)

# Configure logging
logging.basicConfig(level=logging.INFO)

@app.route('/execute', methods=['POST'])
def execute():
    logging.info("Start Execute")

    payload = request.get_json()

    # Initialize the result dictionary
    result = {"status": "complete"}

    # Extract the sink table name
    datasets = payload.get("datasets", [])
    if datasets:
        result["sinktable"] = datasets[0]["properties"]["typeProperties"]["tableName"]

    # Extract connection string from linked services and create SQL connection
    linked_services = payload.get("linkedServices", [])
    if linked_services:
        conn_string = linked_services[0]["properties"]["typeProperties"]["connectionString"]
        sql_conn = pyodbc.connect(conn_string)
        result["sinkServer"] = sql_conn.getinfo(pyodbc.SQL_SERVER_NAME)
        sql_conn.close()

    logging.info("Stop Execute")

    return jsonify(result), 200

if __name__ == "__main__":
    app.run(debug=True)
