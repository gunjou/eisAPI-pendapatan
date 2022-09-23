from flask import Flask
from flask_cors import CORS

from api.endpoints import pendapatan_bp


api = Flask(__name__)
CORS(api)

api.register_blueprint(pendapatan_bp)