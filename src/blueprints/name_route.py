from flask import Blueprint, render_template, Response
import logging
from features.name_generator import get_unique_animal_names



log = logging.getLogger(__name__)

name_bp = Blueprint('name', __name__, url_prefix='/names')


@name_bp.route("/")
def check_names():
    unique_names = get_unique_animal_names()
    return Response(render_template("names.html", names=unique_names['name'].tolist()))
