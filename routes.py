from flask import Blueprint, jsonify, request
from models import Dropsonde, db
from datetime import datetime, timedelta

dropsonde_bp = Blueprint('dropsonde', __name__)

@dropsonde_bp.route('/api/drops', methods=['GET'])
def get_drops():
    query = Dropsonde.query

    # Optional filters
    if (tail := request.args.get('tail_number')):
        query = query.filter(Dropsonde.tail_number == tail)
    if (op := request.args.get('operator')):
        query = query.filter(Dropsonde.operator == op)

    # Optional date filters
    start = request.args.get('start')
    end = request.args.get('end')

    if start:
        query = query.filter(Dropsonde.droptime >= datetime.strptime(start, "%Y-%m-%d"))
    if end:
        query = query.filter(Dropsonde.droptime <= datetime.strptime(end, "%Y-%m-%d"))


    drops = query.all()

    return jsonify([{
        'id': d.id,
        'tail': d.tail,
        'operator': d.operator,
        'droptime': d.droptime.isoformat() if d.droptime else None,
        'lat': d.lat,
        'lon': d.lon,
        'serial': d.serial
    } for d in drops])

@dropsonde_bp.route("/api/operators")
def get_operators():
    # Use SQLAlchemy to get distinct operators
    operators = (
        db.session.query(Dropsonde.operator)
        .filter(Dropsonde.operator.isnot(None), Dropsonde.operator != "")
        .distinct()
        .order_by(Dropsonde.operator)
        .all()
    )
    # Flatten the list of tuples
    operator_list = [op[0] for op in operators]
    return jsonify(operator_list)