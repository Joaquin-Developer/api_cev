"""API rest para obtener datos de CovidEnVivo"""

from flask import Flask, request
from database.database import Database

app = Flask(__name__)


@app.get("/total_info")
def get_all_info():
    """get dafault all info"""
    return Database.get_all_info()


@app.post("/info_in_range")
def get_info_by_date_range():
    """get info in range date"""
    json_req = request.get_json(force=True)
    return Database.get_info_by_date_range(json_req.get("date_from"), json_req.get("date_to"))


if __name__ == "__main__":
    app.run(debug=True)
