from flask import Blueprint
from . import views


main = Blueprint('main', __name__)

main.add_url_rule('/','index', views.index)
main.add_url_rule('/upload_csv','upload_csv', views.upload_csv,methods=['POST','GET'])