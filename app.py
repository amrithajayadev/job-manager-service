import logging
import os

from flask import Flask, request, jsonify
from flask_swagger_ui import get_swaggerui_blueprint

from flask_cors import CORS

from service import job_handler
_logger = logging.getLogger(__name__)

app = Flask(__name__)
CORS(app)
SWAGGER_URL = '/apidocs'
API_URL = '/static/openapi.json'
SWAGGERUI_BLUEPRINT = get_swaggerui_blueprint(
    SWAGGER_URL,
    API_URL,
    config={
        'app_name': "Job manager service"
    }
)
app.register_blueprint(SWAGGERUI_BLUEPRINT, url_prefix=SWAGGER_URL)

job_handler_service = job_handler.JobHandler()


@app.route("/")
def main():
    return "Job manager service"


@app.route("/submit_job", methods=["POST"])
def submit_job():
    """
    This API allows clients to enqueue jobs with a payload and a unique identifier will be created.
    Sample request body: { “content”: {}, priority: 2, “timeout”:3000, "task_category":"read_file"}

    :return: JSON response
    """
    data = request.get_json()
    response = job_handler_service.submit_job(data)
    return jsonify(response), 200


@app.route("/job_status", methods=["GET"])
def get_job_status():
    """
    Get status of a job that is submitted
    :return: JSON response
    """
    message_id = request.args.get("message_id")
    if not message_id:
        return "message_id is required", 500
    try:
        response = job_handler_service.get_job_status(message_id)
        if response:
            return jsonify(response), 200
        else:
            return jsonify("Requested job not found"), 404
    except Exception as e:
        _logger.error(f"Could not fetch the data:{e}")


@app.route("/queue_status", methods=["GET"])
def get_queue_status():
    queue_id = request.args.get("queue_id")
    try:
        response = job_handler_service.get_queue_status(queue_id)
        return jsonify(response), 200
    except Exception as e:
        _logger.error(f"Could not fetch the data:{e}")


@app.route("/health")
def health():
    return "OK"


if __name__ == "__main__":
    port = os.environ.get("PORT", "4000")
    app.run(host="0.0.0.0", port=port)
