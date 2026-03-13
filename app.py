from flask import Flask
from models import db
from routes import dropsonde_bp

app = Flask(__name__, static_folder='static')
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///dropsonde.db' # SQLite
#app.config['SQLALCHEMY_DATABASE_URI'] = 'postgresql://postgres:ncar@localhost:5432/dropsonde_db' # dropsonde_db for operational... dropsonde_test for 100 test drops
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

db.init_app(app)
app.register_blueprint(dropsonde_bp)

@app.route('/')
def index():
    return app.send_static_file('index.html')

if __name__ == '__main__':
    app.run(debug=True)