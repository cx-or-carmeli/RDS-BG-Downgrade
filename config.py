"""Configuration for RDS Blue/Green Instance Resize."""

# Thresholds
GIB = 1024 ** 3
CPU_WARNING_THRESHOLD = 40.0
CPU_CRITICAL_THRESHOLD = 80.0
MEMORY_WARNING_GIB = 1.0
MEMORY_CRITICAL_GIB = 0.5
COMBINED_CPU_THRESHOLD = 30.0
COMBINED_MEMORY_THRESHOLD = 2.0 * GIB
OLD_RESOURCE_SUFFIXES = ("-old1", "-old2", "-old", "-blue", "-previous")
DEFAULT_POLL_INTERVAL = 10
BG_SWITCH_POLL_INTERVAL = 30
BG_TIMEOUT_MINUTES = 90

# Instance specs: {instance_class: (vCPUs, memory_gib)}
INSTANCE_SPECS = {
    # T3/T4g burstable
    "db.t3.micro": (2, 1), "db.t3.small": (2, 2), "db.t3.medium": (2, 4), "db.t3.large": (2, 8),
    "db.t3.xlarge": (4, 16), "db.t3.2xlarge": (8, 32), "db.t4g.micro": (2, 1), "db.t4g.small": (2, 2),
    "db.t4g.medium": (2, 4), "db.t4g.large": (2, 8), "db.t4g.xlarge": (4, 16), "db.t4g.2xlarge": (8, 32),
    # M5/M6g/M6i general purpose
    "db.m5.large": (2, 8), "db.m5.xlarge": (4, 16), "db.m5.2xlarge": (8, 32), "db.m5.4xlarge": (16, 64),
    "db.m5.8xlarge": (32, 128), "db.m5.12xlarge": (48, 192), "db.m5.16xlarge": (64, 256), "db.m5.24xlarge": (96, 384),
    "db.m6g.large": (2, 8), "db.m6g.xlarge": (4, 16), "db.m6g.2xlarge": (8, 32), "db.m6g.4xlarge": (16, 64),
    "db.m6g.8xlarge": (32, 128), "db.m6g.12xlarge": (48, 192), "db.m6g.16xlarge": (64, 256),
    "db.m6i.large": (2, 8), "db.m6i.xlarge": (4, 16), "db.m6i.2xlarge": (8, 32), "db.m6i.4xlarge": (16, 64),
    "db.m6i.8xlarge": (32, 128), "db.m6i.12xlarge": (48, 192), "db.m6i.16xlarge": (64, 256),
    "db.m6i.24xlarge": (96, 384), "db.m6i.32xlarge": (128, 512),
    # R5/R6g memory optimized
    "db.r5.large": (2, 16), "db.r5.xlarge": (4, 32), "db.r5.2xlarge": (8, 64), "db.r5.4xlarge": (16, 128),
    "db.r5.8xlarge": (32, 256), "db.r5.12xlarge": (48, 384), "db.r5.16xlarge": (64, 512), "db.r5.24xlarge": (96, 768),
    "db.r6g.large": (2, 16), "db.r6g.xlarge": (4, 32), "db.r6g.2xlarge": (8, 64), "db.r6g.4xlarge": (16, 128),
    "db.r6g.8xlarge": (32, 256), "db.r6g.12xlarge": (48, 384), "db.r6g.16xlarge": (64, 512),
}
