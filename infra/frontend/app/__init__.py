from flask import Flask
from .routes import main_bp

def create_app():
    app = Flask(__name__)

    # Configurações (pega do config.py)
    # app.config.from_object("config.Config")

    # Registrar rotas
    app.register_blueprint(main_bp)

    return app
