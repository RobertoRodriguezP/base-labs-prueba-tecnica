from flask import Blueprint, jsonify, render_template
import pandas as pd
from sqlalchemy import create_engine
import os


main = Blueprint('main', __name__)
DB_PATH = "database/annie.db"

@main.route("/")
def index():
    return render_template("dashboard.html")

@main.route("/dashboard/refresh")
def refresh_data():
    from .ingest import load_or_reuse
    load_or_reuse(force=True)
    return jsonify({"status": "updated"})

@main.route("/api/view/<name>")
def get_view(name):
    sql_dir = "database/sql"
    allowed = []

    if os.path.exists(sql_dir):
        for filename in os.listdir(sql_dir):
            if filename.endswith(".sql"):
                view_name = filename.replace(".sql", "")
                allowed.append(view_name)

    if name not in allowed:
        return jsonify({"error": "Invalid view"}), 400
    df = pd.read_sql(f"SELECT * FROM {name}", con=engine)
    return df.to_json(orient="records")

@main.route("/dashboard/last-update")
def get_last_update():
    engine = create_engine(f'sqlite:///{DB_PATH}')
    try:
        df = pd.read_sql("SELECT * FROM metadata", engine)
        timestamp = df.iloc[0]["updated_at"]
        return jsonify({"last_update": timestamp})
    except Exception as e:
        return jsonify({"last_update": "Not available"})


@main.route("/dashboard/data")
def get_dashboard_data():
    engine = create_engine(f'sqlite:///' + DB_PATH)
    df = pd.read_sql("SELECT * FROM sales_data", engine)

    # Limpiar datos
    df = df.dropna(subset=["description", "brand", "profit", "margin"])

    # Top 10 productos por ganancia
    top_products_profit = (
        df.groupby("description")["profit"]
        .sum()
        .sort_values(ascending=False)
        .head(10)
    )

    # Top 10 productos por margen
    top_products_margin = (
        df.groupby("description")["margin"]
        .mean()
        .sort_values(ascending=False)
        .head(10)
    )

    # Top 10 marcas por ganancia
    top_brands_profit = (
        df.groupby("brand")["profit"]
        .sum()
        .sort_values(ascending=False)
        .head(10)
    )

    # Top 10 marcas por margen
    top_brands_margin = (
        df.groupby("brand")["margin"]
        .mean()
        .sort_values(ascending=False)
        .head(10)
    )

    # Productos que pierden dinero
    losing_products = (
        df[df["profit"] < 0]
        .groupby("description")["profit"]
        .sum()
        .sort_values()
        .head(10)
    )

    return jsonify({
        "top_products_profit": top_products_profit.to_dict(),
        "top_products_margin": top_products_margin.to_dict(),
        "top_brands_profit": top_brands_profit.to_dict(),
        "top_brands_margin": top_brands_margin.to_dict(),
        "losing_products": losing_products.to_dict()
    })
