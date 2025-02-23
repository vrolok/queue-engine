# src/api/exceptions.py
from fastapi import HTTPException
from starlette.status import (
    HTTP_400_BAD_REQUEST,
    HTTP_422_UNPROCESSABLE_ENTITY,
    HTTP_500_INTERNAL_SERVER_ERROR,
)


class TaskValidationError(HTTPException):
    def __init__(self, detail: str):
        super().__init__(status_code=HTTP_422_UNPROCESSABLE_ENTITY, detail=detail)


class TaskQueueError(HTTPException):
    def __init__(self, detail: str):
        super().__init__(status_code=HTTP_500_INTERNAL_SERVER_ERROR, detail=detail)
