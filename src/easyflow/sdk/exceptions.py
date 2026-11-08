class EasyflowError(Exception):
    """Base class for public easyflow exceptions."""

    error_code = "EASYFLOW_ERROR"


class AirflowSkipException(EasyflowError):
    """任务主动跳过，不消耗重试次数。"""

    error_code = "TASK_SKIPPED"


class DagDefinitionError(EasyflowError):
    error_code = "DAG_DEFINITION_ERROR"


class DuplicateDagIdError(DagDefinitionError):
    error_code = "DUPLICATE_DAG_ID"


class DuplicateTaskIdError(DagDefinitionError):
    error_code = "DUPLICATE_TASK_ID"


class DagCycleError(DagDefinitionError):
    error_code = "DAG_CYCLE"


class TaskBindingError(DagDefinitionError):
    error_code = "TASK_BINDING_ERROR"


class UnsupportedFeatureError(EasyflowError):
    error_code = "UNSUPPORTED_FEATURE"


class ContextNotAvailableError(EasyflowError):
    error_code = "CONTEXT_NOT_AVAILABLE"


class TemplateRenderError(EasyflowError):
    error_code = "TEMPLATE_RENDER_ERROR"


class XComError(EasyflowError):
    error_code = "XCOM_ERROR"


class XComSerializationError(XComError):
    error_code = "XCOM_SERIALIZATION_ERROR"


class XComValueTooLargeError(XComError):
    error_code = "XCOM_VALUE_TOO_LARGE"


class StateConflictError(EasyflowError):
    error_code = "STATE_CONFLICT"
