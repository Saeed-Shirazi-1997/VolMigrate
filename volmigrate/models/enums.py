"""Domain enumerations for connections, migration state machines, and events."""

from enum import Enum


class ConnectionType(str, Enum):
    DOCKER = "docker"
    KUBERNETES = "kubernetes"


class ConnectionStatus(str, Enum):
    ACTIVE = "active"
    ERROR = "error"
    UNTESTED = "untested"


class MigrationStatus(str, Enum):
    DRAFT = "draft"
    PREFLIGHT = "preflight"
    QUEUED = "queued"
    PREPARING = "preparing"
    TRANSFERRING = "transferring"
    VALIDATING = "validating"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    NEEDS_ATTENTION = "needs_attention"
    CLEANUP_PENDING = "cleanup_pending"


class EventLevel(str, Enum):
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"


class ValidationPolicy(str, Enum):
    FAST_MANIFEST = "fast_manifest"
    FULL_SHA256 = "full_sha256"
    BASIC = "basic"


class OverwritePolicy(str, Enum):
    FAIL_IF_EXISTS = "fail_if_exists"
    OVERWRITE = "overwrite"
