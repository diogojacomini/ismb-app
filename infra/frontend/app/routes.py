from flask import Blueprint, render_template, request, abort
import markdown
import os

main_bp = Blueprint("main", __name__)

@main_bp.route("/")
def index():
    return render_template("index.html")

@main_bp.route("/saibamais/<letra>")
def saiba_mais(letra):
    md_path = os.path.join("docs", f"indicador{letra}.md")
    if not os.path.exists(md_path):
        abort(404)
    with open(md_path, "r", encoding="utf-8") as f:
        lines = f.readlines()

    md_content = "".join(lines[1:])
    html_content = markdown.markdown(md_content)
    return html_content
