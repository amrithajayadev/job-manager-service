import datetime
import logging
import uuid
from collections import defaultdict

from utils.constants import JobStatus

_logger = logging.getLogger(__name__)


def _add_default_fields_message(data, unique_id):
    data["message_id"] = unique_id
    data["create_time"] = datetime.datetime.now()
    data["modified_time"] = datetime.datetime.now()
    data["completed_time"] = None
    data["status"] = JobStatus.READY.value
    return data


class JobHandler:
    """
    Class to submit jobs and get the results of the jobs
    """
    instance = None

    def __new__(cls, *args, **kwargs):
        if not isinstance(cls.instance, cls):
            cls.instance = object.__new__(cls)
        return cls.instance

    def __init__(self, db_session=None):
        if db_session:
            self.db_session = db_session
        self.queues_map = defaultdict(list)
        self.message_map = defaultdict(dict)

    def submit_job(self, data):
        """
        Submit job with response body containing task type, priority and job's description n content.
        data (json): { “content”: {}, priority: 2, “timeout”:3000, "task_category":"q1"}
        :returns message_id
        """
        unique_id = str(uuid.uuid1())
        priority = data.get("priority")
        data = _add_default_fields_message(data, unique_id)
        task_category = data.get("task_category")
        # save the task to DB
        self.queues_map[task_category].append((unique_id, priority))
        self.message_map[unique_id] = data
        # TODO: sort the queues map based on the priority and timestamp
        return {"message_id": unique_id}

    def get_job_status(self, message_id):
        """
        Get job status with message uuid
        :param message_id: int
        :return: (dict) Status of the message
        """
        resp = self.message_map.get(message_id)
        if resp:
            # mock running
            resp["status"] = JobStatus.PROGRESS.value
        return resp

    def get_queue_status(self, queue_id):
        """
        Get the status of a queue if queue_id is specified, else show all queues
        :param queue_id: int
        :return: dict
        """
        if not queue_id:
            return self.queues_map
        else:
            return self.queues_map.get(queue_id)
