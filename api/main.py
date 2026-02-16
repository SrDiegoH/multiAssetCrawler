from flask import Flask

from api.controllers.controller import controller_blue_print
from log.log_manager import LOG_LEVEL, log_debug

app = Flask(__name__)
app.json.sort_keys = False

app.register_blueprint(controller_blue_print)

if __name__ == '__main__':
    log_debug('Starting API')
    app.run(debug=LOG_LEVEL == 'DEBUG')