import os
from dotenv import load_dotenv
load_dotenv()

from fastapi import FastAPI, Query, HTTPException, Depends
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, StreamingResponse
from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime, text, func, extract
from sqlalchemy.orm import declarative_base, Session
from datetime import datetime, date
from typing import Optional
from pydantic import BaseModel
import csv
import io

# ── Database setup ────────────────────────────────────────────────────────────
DATABASE_URL = os.getenv('DATABASE_URL', 'sqlite:///instance/dropsonde.db')

engine = create_engine(
    DATABASE_URL,
    connect_args={"check_same_thread": False} if DATABASE_URL.startswith("sqlite") else {}
)
Base = declarative_base()

class Dropsonde(Base):
    __tablename__ = "dropsonde_data"
    id       = Column(Integer, primary_key=True)
    uid      = Column(String(50), unique=True, nullable=True)
    tail     = Column(String(10), nullable=False)
    operator = Column(String(50), nullable=False)
    droptime = Column(DateTime, nullable=False)
    lat      = Column(Float, nullable=False)
    lon      = Column(Float, nullable=False)
    serial   = Column(String(10), nullable=True)

Base.metadata.create_all(bind=engine)

# ── Pydantic schemas ──────────────────────────────────────────────────────────
class DropMap(BaseModel):
    uid:      Optional[str]
    lat:      float
    lon:      float
    tail:     str

    class Config:
        from_attributes = True

class DropDetail(BaseModel):
    uid:      Optional[str]
    tail:     str
    operator: str
    droptime: Optional[datetime]
    lat:      float
    lon:      float
    serial:   Optional[str]

    class Config:
        from_attributes = True

# ── App ───────────────────────────────────────────────────────────────────────
app = FastAPI(title="AOC Dropsonde API")

def get_db():
    db = Session(engine)
    try:
        yield db
    finally:
        db.close()

def apply_filters(query, db, start, end, operator, tail):
    if start:
        query = query.filter(Dropsonde.droptime >= datetime.combine(start, datetime.min.time()))
    if end:
        query = query.filter(Dropsonde.droptime <= datetime.combine(end, datetime.max.time()))
    if operator:
        query = query.filter(Dropsonde.operator == operator)
    if tail:
        query = query.filter(Dropsonde.tail == tail)
    return query

# ── Config endpoint (serves Mapbox token to frontend) ─────────────────────────
@app.get("/api/config")
def config():
    return {"mapbox_token": os.getenv("MAPBOX_TOKEN", "")}

# ── Routes ────────────────────────────────────────────────────────────────────

@app.get("/api/drops/map", response_model=list[DropMap])
def get_drops_map(
    start:    Optional[date] = None,
    end:      Optional[date] = None,
    operator: Optional[str]  = None,
    tail:     Optional[str]  = None,
    db:       Session        = Depends(get_db)
):
    """Lightweight endpoint for map rendering - returns only lat, lon, tail, uid."""
    query = db.query(Dropsonde.uid, Dropsonde.lat, Dropsonde.lon, Dropsonde.tail)
    query = apply_filters(query, db, start, end, operator, tail)
    rows = query.all()
    return [DropMap(uid=r.uid, lat=r.lat, lon=r.lon, tail=r.tail) for r in rows]


# NOTE: /api/drops/export must be defined BEFORE /api/drops/{uid}
@app.get("/api/drops/export")
def export_drops(
    start:    Optional[date] = None,
    end:      Optional[date] = None,
    operator: Optional[str]  = None,
    tail:     Optional[str]  = None,
    db:       Session        = Depends(get_db)
):
    """Export filtered drops as a CSV file download."""
    query = db.query(Dropsonde)
    query = apply_filters(query, db, start, end, operator, tail)
    drops = query.all()

    def generate():
        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow(["uid", "tail", "operator", "droptime", "lat", "lon", "serial"])
        for d in drops:
            writer.writerow([d.uid, d.tail, d.operator, d.droptime, d.lat, d.lon, d.serial])
            yield output.getvalue()
            output.seek(0)
            output.truncate(0)

    filename = f"dropsonde_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    return StreamingResponse(
        generate(),
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename={filename}"}
    )


@app.get("/api/drops/{uid}", response_model=DropDetail)
def get_drop_detail(uid: str, db: Session = Depends(get_db)):
    """Full details for a single drop, fetched on marker click."""
    drop = db.query(Dropsonde).filter(Dropsonde.uid == uid).first()
    if not drop:
        raise HTTPException(status_code=404, detail="Drop not found")
    return drop


@app.get("/api/drops", response_model=list[DropDetail])
def get_drops(
    start:    Optional[date] = None,
    end:      Optional[date] = None,
    operator: Optional[str]  = None,
    tail:     Optional[str]  = None,
    db:       Session        = Depends(get_db)
):
    query = db.query(Dropsonde)
    query = apply_filters(query, db, start, end, operator, tail)
    return query.all()


@app.get("/api/operators")
def get_operators(db: Session = Depends(get_db)):
    rows = (
        db.query(Dropsonde.operator)
        .filter(Dropsonde.operator.isnot(None), Dropsonde.operator != "")
        .distinct()
        .order_by(Dropsonde.operator)
        .all()
    )
    return [r[0] for r in rows]


@app.get("/api/stats")
def get_stats(db: Session = Depends(get_db)):
    rows = (
        db.query(
            extract("year", Dropsonde.droptime).label("year"),
            Dropsonde.tail,
            func.count(Dropsonde.id).label("count")
        )
        .group_by("year", Dropsonde.tail)
        .order_by("year")
        .all()
    )
    return [{"year": int(r.year), "tail": r.tail, "count": r.count} for r in rows]


@app.get("/api/missions")
def get_missions(
    start:    Optional[date] = None,
    end:      Optional[date] = None,
    tail:     Optional[str]  = None,
    db:       Session        = Depends(get_db)
):
    query = db.query(
        func.date(Dropsonde.droptime).label("mission_date"),
        Dropsonde.tail,
        func.count(Dropsonde.id).label("drop_count"),
        func.min(Dropsonde.droptime).label("first_drop"),
        func.max(Dropsonde.droptime).label("last_drop")
    )
    if start:
        query = query.filter(Dropsonde.droptime >= datetime.combine(start, datetime.min.time()))
    if end:
        query = query.filter(Dropsonde.droptime <= datetime.combine(end, datetime.max.time()))
    if tail:
        query = query.filter(Dropsonde.tail == tail)

    rows = (
        query
        .group_by("mission_date", Dropsonde.tail)
        .order_by("mission_date")
        .all()
    )
    return [
        {
            "mission_date": r.mission_date,
            "tail":         r.tail,
            "drop_count":   r.drop_count,
            "first_drop":   r.first_drop,
            "last_drop":    r.last_drop
        }
        for r in rows
    ]


# ── Static files / frontend ───────────────────────────────────────────────────
app.mount("/static", StaticFiles(directory="static"), name="static")

@app.get("/")
def index():
    return FileResponse("static/index.html")
