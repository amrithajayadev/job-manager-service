from enum import Enum


class JobStatus(Enum):
    READY = "READY"
    FAILED = "FAILED"
    COMPLETED = "COMPLETED"
    PROGRESS = "RUNNING"
