#!/usr/bin/env python3
"""RDS Blue/Green Instance Resize Helper.

Automates RDS/Aurora instance class changes (upgrades/downgrades) using AWS Blue/Green deployments.
Includes pre-checks, workload suitability analysis, snapshots, and rollback support.

Usage:
    export AWS_PROFILE=prod AWS_REGION=eu-west-1
    python rds_bg_resize.py

Requirements:
    pip install boto3
"""

# Self-bootstrapping: ensure boto3 is available
import os
import pathlib
import subprocess
import sys
import venv


def _bootstrap_boto3():
    """Check if boto3 is available, install in local .venv if not."""
    try:
        import boto3  # noqa: F401
        return
    except ImportError:
        root = pathlib.Path(__file__).resolve().parent
        venv_dir = root / ".venv"
        
        if not venv_dir.exists():
            print("Creating virtualenv at .venv")
            venv.EnvBuilder(with_pip=True).create(str(venv_dir))
        
        py_dir = "Scripts" if os.name == "nt" else "bin"
        py_exe = venv_dir / py_dir / "python"
        
        print("Installing boto3...")
        subprocess.check_call([str(py_exe), "-m", "pip", "install", "-q", "-U", "pip"])
        subprocess.check_call([str(py_exe), "-m", "pip", "install", "-q", "boto3>=1.34"])
        
        # Re-exec under venv interpreter
        os.execv(str(py_exe), [str(py_exe), __file__] + sys.argv[1:])


_bootstrap_boto3()

# Standard library imports
import datetime as dt
import json
import time
from typing import Dict, List, Optional, Tuple

# Third-party imports
import boto3
from botocore.exceptions import ClientError, NoRegionError, ParamValidationError, ProfileNotFound

# Constants
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
    # T3 (burstable)
    "db.t3.micro": (2, 1), "db.t3.small": (2, 2), "db.t3.medium": (2, 4),
    "db.t3.large": (2, 8), "db.t3.xlarge": (4, 16), "db.t3.2xlarge": (8, 32),
    # T4g (ARM burstable)
    "db.t4g.micro": (2, 1), "db.t4g.small": (2, 2), "db.t4g.medium": (2, 4),
    "db.t4g.large": (2, 8), "db.t4g.xlarge": (4, 16), "db.t4g.2xlarge": (8, 32),
    # M5 (general purpose)
    "db.m5.large": (2, 8), "db.m5.xlarge": (4, 16), "db.m5.2xlarge": (8, 32),
    "db.m5.4xlarge": (16, 64), "db.m5.8xlarge": (32, 128), "db.m5.12xlarge": (48, 192),
    "db.m5.16xlarge": (64, 256), "db.m5.24xlarge": (96, 384),
    # M6g (ARM general purpose)
    "db.m6g.large": (2, 8), "db.m6g.xlarge": (4, 16), "db.m6g.2xlarge": (8, 32),
    "db.m6g.4xlarge": (16, 64), "db.m6g.8xlarge": (32, 128), "db.m6g.12xlarge": (48, 192),
    "db.m6g.16xlarge": (64, 256),
    # M6i
    "db.m6i.large": (2, 8), "db.m6i.xlarge": (4, 16), "db.m6i.2xlarge": (8, 32),
    "db.m6i.4xlarge": (16, 64), "db.m6i.8xlarge": (32, 128), "db.m6i.12xlarge": (48, 192),
    "db.m6i.16xlarge": (64, 256), "db.m6i.24xlarge": (96, 384), "db.m6i.32xlarge": (128, 512),
    # R5 (memory optimized)
    "db.r5.large": (2, 16), "db.r5.xlarge": (4, 32), "db.r5.2xlarge": (8, 64),
    "db.r5.4xlarge": (16, 128), "db.r5.8xlarge": (32, 256), "db.r5.12xlarge": (48, 384),
    "db.r5.16xlarge": (64, 512), "db.r5.24xlarge": (96, 768),
    # R6g (ARM memory optimized)
    "db.r6g.large": (2, 16), "db.r6g.xlarge": (4, 32), "db.r6g.2xlarge": (8, 64),
    "db.r6g.4xlarge": (16, 128), "db.r6g.8xlarge": (32, 256), "db.r6g.12xlarge": (48, 384),
    "db.r6g.16xlarge": (64, 512),
}

# -------------- helpers: time --------------

def now_utc():
    UTC = getattr(dt, "UTC", dt.timezone.utc)
    return dt.datetime.now(UTC)

def time_range(minutes: int = 15) -> Tuple[dt.datetime, dt.datetime]:
    end = now_utc()
    start = end - dt.timedelta(minutes=minutes)
    return start, end

# -------------- session and banner --------------

def get_session_from_env() -> boto3.session.Session:
    try:
        return boto3.Session()  # uses AWS_PROFILE, AWS_REGION, config files
    except ProfileNotFound as e:
        print(f"Profile not found: {e}")
        sys.exit(2)
    except NoRegionError:
        print("Region not specified")
        sys.exit(2)

def print_environment_banner(sess: boto3.session.Session):
    profile = os.environ.get("AWS_PROFILE") or "default"
    region = sess.region_name
    print("=== RDS Blue/Green Instance Resize Helper ===")
    print(f"Using AWS profile: {profile}")
    if not region:
        print("No region resolved. Set AWS_REGION or configure a default region, example:")
        print("  export AWS_REGION=eu-west-1")
        sys.exit(2)
    print(f"Using AWS region: {region}")
    print("Make sure the profile and region above are correct.\n")

# -------------- describe helpers --------------

def _select_from_list(items: List, prompt: str = "Choose a number") -> any:
    """Generic selection from numbered list. Returns selected item or exits."""
    while True:
        choice = input(f"{prompt}: ").strip()
        if choice.isdigit() and 1 <= int(choice) <= len(items):
            return items[int(choice) - 1]
        print("Invalid choice")

def list_db_targets(rds) -> List[Dict]:
    items: List[Dict] = []
    # clusters
    try:
        paginator = rds.get_paginator("describe_db_clusters")
        for page in paginator.paginate():
            for clu in page.get("DBClusters", []):
                writer_id = None
                for m in clu.get("DBClusterMembers", []):
                    if m.get("IsClusterWriter"):
                        writer_id = m.get("DBInstanceIdentifier")
                        break
                writer_class = None
                if writer_id:
                    try:
                        i = rds.describe_db_instances(DBInstanceIdentifier=writer_id)["DBInstances"][0]
                        writer_class = i.get("DBInstanceClass")
                    except ClientError:
                        pass
                items.append({
                    "type": "cluster",
                    "id": clu["DBClusterIdentifier"],
                    "engine": clu.get("Engine"),
                    "version": clu.get("EngineVersion"),
                    "class": writer_class,
                    "storage_gb": None,
                })
    except ClientError:
        pass
    # instances
    try:
        paginator = rds.get_paginator("describe_db_instances")
        for page in paginator.paginate():
            for inst in page.get("DBInstances", []):
                items.append({
                    "type": "instance",
                    "id": inst["DBInstanceIdentifier"],
                    "engine": inst.get("Engine"),
                    "version": inst.get("EngineVersion"),
                    "class": inst.get("DBInstanceClass"),
                    "storage_gb": inst.get("AllocatedStorage"),
                    "storage_type": inst.get("StorageType"),
                })
    except ClientError:
        pass
    return items

def choose_target(rds) -> Dict:
    """Interactive selection of RDS target database."""
    items = list_db_targets(rds)
    if not items:
        print("No RDS instances or Aurora clusters found")
        sys.exit(2)
    
    print("\nAvailable databases:")
    for idx, item in enumerate(items, 1):
        storage = f", {item.get('storage_gb')}GB" if item.get('storage_gb') else ""
        print(f"  {idx}) [{item['type']}] {item['id']} | {item.get('engine')} {item.get('version')} | {item.get('class')}{storage}")
    
    return _select_from_list(items, "Choose a number")

def is_cluster(rds, identifier: str) -> Tuple[bool, Dict]:
    """Check if identifier is cluster or instance. Returns (is_cluster, description)."""
    try:
        resp = rds.describe_db_clusters(DBClusterIdentifier=identifier)
        return True, resp["DBClusters"][0]
    except ClientError as e:
        if e.response["Error"]["Code"] not in ("DBClusterNotFoundFault", "InvalidDBClusterStateFault"):
            raise
    
    try:
        resp = rds.describe_db_instances(DBInstanceIdentifier=identifier)
        return False, resp["DBInstances"][0]
    except ClientError as e:
        if e.response["Error"]["Code"] != "DBInstanceNotFound":
            raise
    
    print(f"Error: Identifier '{identifier}' not found")
    sys.exit(2)

# -------------- CloudWatch metrics --------------

def get_metric_avg(cw, metric: str, dim_name: str, dim_value: str, period: int = 300, minutes: int = 15, stat: str = "Average") -> Optional[float]:
    start, end = time_range(minutes)
    resp = cw.get_metric_statistics(
        Namespace="AWS/RDS",
        MetricName=metric,
        Dimensions=[{"Name": dim_name, "Value": dim_value}],
        StartTime=start,
        EndTime=end,
        Period=period,
        Statistics=[stat],
    )
    dps = sorted(resp.get("Datapoints", []), key=lambda x: x["Timestamp"])
    if not dps:
        return None
    return float(dps[-1][stat])

def prechecks(rds, cw, identifier: str) -> Dict[str, Optional[float]]:
    """Get CloudWatch metrics for pre/post checks."""
    is_clu, desc = is_cluster(rds, identifier)
    
    # Get writer instance for clusters
    if is_clu:
        writer = next((m for m in desc.get("DBClusterMembers", []) if m.get("IsClusterWriter")), None)
        inst_id = writer["DBInstanceIdentifier"] if writer else None
        if not inst_id:
            raise RuntimeError("Writer instance not found")
    else:
        inst_id = desc["DBInstanceIdentifier"]
    
    # Fetch all metrics
    return {
        "CPUUtilization_Average_percent": get_metric_avg(cw, "CPUUtilization", "DBInstanceIdentifier", inst_id),
        "FreeableMemory_Average_bytes": get_metric_avg(cw, "FreeableMemory", "DBInstanceIdentifier", inst_id),
        "ReadIOPS_Average": get_metric_avg(cw, "ReadIOPS", "DBInstanceIdentifier", inst_id),
        "WriteIOPS_Average": get_metric_avg(cw, "WriteIOPS", "DBInstanceIdentifier", inst_id),
        "DatabaseConnections_Average": get_metric_avg(cw, "DatabaseConnections", "DBInstanceIdentifier", inst_id),
    }

def print_precheck_summary(metrics: Dict[str, Optional[float]], title: str = "Pre checks") -> bool:
    """Display metrics and check thresholds."""
    print(f"\n{title}, last 15 minutes average:")
    for k, v in metrics.items():
        print(f"  {k}: {v}")
    ok = True
    cpu = metrics.get("CPUUtilization_Average_percent") or 0.0
    free_mem = metrics.get("FreeableMemory_Average_bytes") or 0.0
    conns = metrics.get("DatabaseConnections_Average") or 0.0
    
    if cpu > CPU_WARNING_THRESHOLD:
        ok = False
        print(f"  Warning: CPU average is above {CPU_WARNING_THRESHOLD}%")
    if free_mem < MEMORY_WARNING_GIB * GIB:
        ok = False
        print(f"  Warning: FreeableMemory average is below {MEMORY_WARNING_GIB} GiB")
    if conns > 0 and cpu > COMBINED_CPU_THRESHOLD and free_mem < COMBINED_MEMORY_THRESHOLD:
        ok = False
        print("  Warning: combined pressure suggests workload risk")
    
    print(f"Checks pass: {ok}")
    return ok

# -------------- snapshot --------------

def wait_snapshot_progress(rds, is_clu: bool, snap_id: str, poll: int = DEFAULT_POLL_INTERVAL):
    """Wait for snapshot to complete."""
    print("\nWaiting for snapshot to complete...")
    last_pct = -1
    
    describe_func = rds.describe_db_cluster_snapshots if is_clu else rds.describe_db_snapshots
    id_param = "DBClusterSnapshotIdentifier" if is_clu else "DBSnapshotIdentifier"
    result_key = "DBClusterSnapshots" if is_clu else "DBSnapshots"
    
    while True:
        try:
            snap = describe_func(**{id_param: snap_id})[result_key][0]
            status = snap.get("Status", "")
            pct = snap.get("PercentProgress", 0)
            
            if pct != last_pct:
                print(f"  {pct}% - {status}")
                last_pct = pct
            
            if status == "available":
                print("Snapshot ready ✅")
                return
        except ClientError as e:
            if e.response["Error"]["Code"] not in ("DBSnapshotNotFound", "DBClusterSnapshotNotFoundFault"):
                raise
        
        time.sleep(poll)

def create_snapshot(rds, identifier: str) -> str:
    """Create pre-resize snapshot and wait for completion."""
    timestamp = now_utc().strftime("%Y%m%d-%H%M%S")
    is_clu, _ = is_cluster(rds, identifier)
    snap_id = f"{identifier}-pre-resize-{timestamp}"
    
    print(f"\nCreating snapshot: {snap_id}")
    
    if is_clu:
        rds.create_db_cluster_snapshot(
            DBClusterSnapshotIdentifier=snap_id,
            DBClusterIdentifier=identifier,
            Tags=[{"Key": "purpose", "Value": "pre-resize"}]
        )
    else:
        rds.create_db_snapshot(
            DBSnapshotIdentifier=snap_id,
            DBInstanceIdentifier=identifier,
            Tags=[{"Key": "purpose", "Value": "pre-resize"}]
        )
    
    wait_snapshot_progress(rds, is_clu, snap_id)
    return snap_id

# -------------- workload suitability --------------

def check_target_class_suitability(rds, cw, identifier: str, current_class: str, target_class: str, metrics: Dict) -> bool:
    """Check if target instance class is suitable for current workload."""
    print(f"\n=== Workload Suitability Check ===")
    print(f"Current: {current_class} → Target: {target_class}")
    
    # Instance specs (vCPU, memory_gib)
    specs = INSTANCE_SPECS.get(current_class), INSTANCE_SPECS.get(target_class)
    if not all(specs):
        print("  ⚠️  Instance specs not in database, skipping detailed check")
        return True
    
    curr_vcpu, curr_mem = specs[0]
    tgt_vcpu, tgt_mem = specs[1]
    
    print(f"  Current: {curr_vcpu} vCPUs, {curr_mem} GiB RAM")
    print(f"  Target:  {tgt_vcpu} vCPUs, {tgt_mem} GiB RAM")
    
    # Determine if this is an upgrade or downgrade
    is_downgrade = tgt_vcpu < curr_vcpu or tgt_mem < curr_mem
    change_type = "Downgrade" if is_downgrade else "Upgrade"
    print(f"  Change type: {change_type}")
    
    issues = []
    warnings = []
    
    # CPU projection
    cpu = metrics.get("CPUUtilization_Average_percent") or 0.0
    projected_cpu = cpu * (curr_vcpu / tgt_vcpu) if tgt_vcpu > 0 else cpu
    print(f"\nCPU: {cpu:.1f}% → projected {projected_cpu:.1f}%")
    
    if projected_cpu > CPU_CRITICAL_THRESHOLD:
        issues.append(f"Projected CPU ({projected_cpu:.0f}%) would be critically high")
    elif projected_cpu > CPU_WARNING_THRESHOLD and is_downgrade:
        warnings.append(f"Projected CPU ({projected_cpu:.0f}%) above warning threshold")
    
    # Memory projection
    free_mem_gib = (metrics.get("FreeableMemory_Average_bytes") or 0.0) / GIB
    mem_change = tgt_mem - curr_mem
    projected_mem = free_mem_gib + mem_change
    print(f"Memory: {free_mem_gib:.1f} GiB free → projected {projected_mem:.1f} GiB free")
    
    if projected_mem < MEMORY_CRITICAL_GIB:
        issues.append(f"Projected free memory ({projected_mem:.1f} GiB) critically low")
    elif projected_mem < MEMORY_WARNING_GIB and is_downgrade:
        warnings.append(f"Projected free memory ({projected_mem:.1f} GiB) below warning threshold")
    
    # IOPS (informational)
    read_iops = metrics.get("ReadIOPS_Average") or 0.0
    write_iops = metrics.get("WriteIOPS_Average") or 0.0
    print(f"IOPS: {read_iops + write_iops:.0f} total")
    
    if issues:
        print("\n❌ CRITICAL ISSUES:")
        for issue in issues:
            print(f"  - {issue}")
        return False
    
    if warnings:
        print("\n⚠️  WARNINGS:")
        for warning in warnings:
            print(f"  - {warning}")
    
    print("\n✅ Target appears suitable")
    return True

# -------------- orderable classes --------------

def list_orderable_classes(rds, engine: str, engine_version: str, storage_type: Optional[str]) -> List[str]:
    classes = set()
    def _call(**kwargs):
        marker = None
        while True:
            if marker:
                kwargs["Marker"] = marker
            resp = rds.describe_orderable_db_instance_options(**kwargs)
            for opt in resp.get("OrderableDBInstanceOptions", []):
                c = opt.get("DBInstanceClass")
                if c:
                    classes.add(c)
            marker = resp.get("Marker")
            if not marker:
                break
    tries = []
    if storage_type:
        tries.append({"Engine": engine, "EngineVersion": engine_version, "StorageType": storage_type})
    tries.append({"Engine": engine, "EngineVersion": engine_version})
    tries.append({"Engine": engine})
    for params in tries:
        try:
            _call(**params)
            if classes:
                break
        except (ClientError, ParamValidationError):
            continue
    return sorted(classes)

def show_and_pick_target_class(rds, identifier: str) -> str:
    """Interactive selection of target instance class."""
    is_clu, desc = is_cluster(rds, identifier)
    engine = (desc.get("Engine") or "").lower()
    version = desc.get("EngineVersion") or ""
    storage = None if is_clu else desc.get("StorageType")
    
    allowed = list_orderable_classes(rds, engine, version, storage)
    if not allowed:
        print("Could not retrieve orderable classes. Check permissions or try again")
        sys.exit(2)
    preferred = [c for c in allowed if any(k in c for k in ("db.t4g.", "db.t3.", "db.m6g.", "db.m5."))]
    ordered = preferred + [c for c in allowed if c not in preferred]
    print("\nOrderable target classes for this engine and version:")
    for idx, cls in enumerate(ordered[:50], 1):
        print(f"  {idx}) {cls}")
    print("  0) Type a class manually")
    while True:
        sel = input("Pick a number, or 0 to type: ").strip()
        if sel.isdigit():
            i = int(sel)
            if i == 0:
                manual = input("Enter class, example: db.t3.medium: ").strip()
                if manual in allowed:
                    return manual
                print("Not in allowed list, try again")
                continue
            if 1 <= i <= len(ordered[:50]):
                return ordered[i - 1]
        print("Invalid choice, try again")

# -------------- ETA --------------

def estimate_bg_eta_minutes(engine: str, storage_gb: Optional[int]) -> Tuple[int, int]:
    eng = (engine or "").lower()
    if "aurora" in eng:
        return (5, 25)
    if storage_gb is None:
        return (10, 45)
    if storage_gb <= 100:
        return (10, 25)
    if storage_gb <= 500:
        return (20, 60)
    return (30, 120)

def print_eta_note(engine: str, storage_gb: Optional[int]):
    lo, hi = estimate_bg_eta_minutes(engine, storage_gb)
    size_note = f"{storage_gb} GB" if storage_gb is not None else "unknown size"
    print(f"\nETA note: Blue/Green provisioning for {engine}, storage {size_note}, often takes about {lo} to {hi} minutes.")
    print("You can press Ctrl+C to stop watching at any time, the deployment continues in AWS.")
    print("You can resume later from Resume menu, or switch with action 4 using the list of existing deployments.")

# -------------- Blue Green core --------------

def create_bg(rds, identifier: str, target_class: str) -> str:
    """Create Blue/Green deployment."""
    is_clu, desc = is_cluster(rds, identifier)
    name = f"bg-{identifier}-{now_utc().strftime('%Y%m%d-%H%M%S')}"
    arn = desc["DBClusterArn"] if is_clu else desc["DBInstanceArn"]
    
    print(f"\nCreating Blue/Green deployment: {name}")
    resp = rds.create_blue_green_deployment(
        BlueGreenDeploymentName=name,
        Source=arn,
        TargetDBInstanceClass=target_class,
        Tags=[{"Key": "purpose", "Value": "instance-resize"}]
    )
    bg_id = resp["BlueGreenDeployment"]["BlueGreenDeploymentIdentifier"]
    print(f"Created: {bg_id}")
    return bg_id

def bg_status(rds, bg_id: str) -> Optional[Dict]:
    try:
        resp = rds.describe_blue_green_deployments(BlueGreenDeploymentIdentifier=bg_id)
        return resp["BlueGreenDeployments"][0]
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code", "")
        if code == "BlueGreenDeploymentNotFoundFault":
            return None
        raise

def wait_switch_ready(rds, bg_id: str, poll: int = BG_SWITCH_POLL_INTERVAL, timeout_min: int = BG_TIMEOUT_MINUTES) -> bool:
    print("\nWaiting for SWITCH_READY")
    start = time.time()
    last = ""
    deadline = start + timeout_min * 60
    while time.time() < deadline:
        s = bg_status(rds, bg_id)
        if s is None:
            print("  Blue/Green deployment not found")
            return False
        status = s.get("Status")
        if status != last:
            elapsed = int((time.time() - start) // 60)
            print(f"  status: {status} (elapsed {elapsed} min)")
            last = status
        if status in ("SWITCH_READY", "AVAILABLE"):
            return True
        if status in ("SWITCHOVER_COMPLETED", "SWITCHOVER_FAILED", "DELETED"):
            print(f"  terminal status: {status}")
            return status == "SWITCHOVER_COMPLETED"
        time.sleep(poll)
    print("Timed out while waiting")
    return False

def switch_over(rds, bg_id: str):
    print(f"\nSwitching Blue/Green: {bg_id}")
    rds.switchover_blue_green_deployment(BlueGreenDeploymentIdentifier=bg_id)
    while True:
        s = bg_status(rds, bg_id)
        if s is None:
            print("  Blue/Green deployment not found")
            return
        st = s.get("Status")
        print(f"  status: {st}")
        if st == "SWITCHOVER_COMPLETED":
            print("Switchover completed")
            return
        if st in ("SWITCHOVER_FAILED", "DELETED"):
            raise RuntimeError(f"Switchover failed, final status: {st}")
        time.sleep(10)

# -------------- verify and post checks --------------

def verify_same_identifier_and_endpoint(rds, original_identifier: str):
    """
    Confirm that after switchover, the writer is still reachable via the same identifier and endpoint.
    """
    is_clu, desc = is_cluster(rds, original_identifier)
    if is_clu:
        ep = desc.get("Endpoint") or {}
        print(f"\nVerification:")
        print(f"  Cluster identifier still present: {original_identifier}")
        print(f"  Writer endpoint: {ep.get('Address')}:{ep.get('Port')}")
        print("  This is the same endpoint name your apps used before switchover.")
    else:
        ep = desc.get("Endpoint") or {}
        icls = desc.get("DBInstanceClass")
        print(f"\nVerification:")
        print(f"  Instance identifier still present: {original_identifier}")
        print(f"  Instance class now: {icls}")
        print(f"  Endpoint: {ep.get('Address')}:{ep.get('Port')}")
        print("  This is the same endpoint name your apps used before switchover.")

def post_checks(rds, cw, identifier: str):
    """Run post-switchover health checks."""
    metrics = prechecks(rds, cw, identifier)
    print_precheck_summary(metrics, title="Post checks")

# -------------- resume list --------------

def parse_identifier_from_arn(arn: str) -> Optional[str]:
    try:
        resource = arn.split(":", 5)[5]
        if ":" in resource:
            return resource.split("=", 1)[-1] if "=" in resource else resource.split(":", 1)[1]
        if "/" in resource:
            return resource.split("/", 1)[1]
        return resource
    except Exception:
        return None

def extract_source_id(src) -> Optional[str]:
    """Extract identifier from Source field (ARN string)."""
    if isinstance(src, str):
        return parse_identifier_from_arn(src)
    if isinstance(src, dict):
        # Fallback for dict format (shouldn't normally happen)
        return src.get("DBClusterIdentifier") or src.get("DBInstanceIdentifier")
    return None

def list_bg_deployments(rds) -> List[Dict]:
    items: List[Dict] = []
    paginator = rds.get_paginator("describe_blue_green_deployments")
    for page in paginator.paginate():
        for d in page.get("BlueGreenDeployments", []):
            st = d.get("Status")
            if st in ("DELETED",):
                continue
            bgid = d.get("BlueGreenDeploymentIdentifier", "")
            short = bgid if len(bgid) <= 20 else f"{bgid[:8]}…{bgid[-6:]}"
            src_id = extract_source_id(d.get("Source"))
            eng = d.get("Engine") or d.get("TargetEngine") or ""
            items.append({
                "id": bgid,
                "short": short,
                "status": st,
                "engine": eng,
                "source_id": src_id or "unknown",
                "created": d.get("CreateTime"),
            })
    return items

def choose_existing_bg(rds) -> Optional[str]:
    """Interactive selection of existing Blue/Green deployment. Returns ID or None."""
    items = list_bg_deployments(rds)
    if not items:
        print("No existing Blue/Green deployments found")
        return None
    
    print("\nExisting Blue/Green deployments:")
    for idx, item in enumerate(items, 1):
        print(f"  {idx}) {item['short']} | {item['status']} | source: {item['source_id']} | {item.get('created', '')}")
    
    while True:
        choice = input("Pick a number or 0 to cancel: ").strip()
        if choice == "0":
            return None
        if choice.isdigit() and 1 <= int(choice) <= len(items):
            return items[int(choice) - 1]["id"]
        print("Invalid choice")

# -------------- cleanup with waits --------------

def wait_deleted(rds, resource_type: str, resource_id: str, poll: int = 15):
    """Wait for resource deletion to complete."""
    print(f"  Waiting for {resource_type} {resource_id} to delete...")
    
    describe_func = rds.describe_db_clusters if resource_type == "cluster" else rds.describe_db_instances
    id_param = "DBClusterIdentifier" if resource_type == "cluster" else "DBInstanceIdentifier"
    not_found_code = "DBClusterNotFoundFault" if resource_type == "cluster" else "DBInstanceNotFound"
    
    while True:
        try:
            describe_func(**{id_param: resource_id})
            time.sleep(poll)
        except ClientError as e:
            if e.response["Error"]["Code"] == not_found_code:
                print("  Deleted ✅")
                return
            raise

def delete_bg_deployment_record(rds, bg_id: str):
    print(f"Deleting Blue/Green deployment record: {bg_id}")
    rds.delete_blue_green_deployment(BlueGreenDeploymentIdentifier=bg_id)
    while True:
        s = bg_status(rds, bg_id)
        if s is None:
            print("  Deleted")
            return
        print("  still present")
        time.sleep(5)

def delete_old_resource(rds):
    """List old resources (-old suffix), allow user to select and delete."""
    insts = rds.describe_db_instances().get("DBInstances", [])
    clus = rds.describe_db_clusters().get("DBClusters", [])
    suffixes = OLD_RESOURCE_SUFFIXES
    candidates: List[Tuple[str, str]] = []
    for i in insts:
        iid = i["DBInstanceIdentifier"]
        if any(iid.endswith(s) for s in suffixes):
            candidates.append(("instance", iid))
    for c in clus:
        cid = c["DBClusterIdentifier"]
        if any(cid.endswith(s) for s in suffixes):
            candidates.append(("cluster", cid))
    if not candidates:
        print("Did not find obvious old resources with -old suffix")
        return
    print("\nOld resources candidates:")
    for idx, (typ, rid) in enumerate(candidates, 1):
        print(f"  {idx}) [{typ}] {rid}")
    sel = input("Pick a number to delete, or 0 to cancel: ").strip()
    if not sel.isdigit() or int(sel) < 1 or int(sel) > len(candidates):
        print("Cancelled")
        return
    typ, rid = candidates[int(sel) - 1]
    if typ == "instance":
        print(f"Deleting instance: {rid}")
        rds.delete_db_instance(DBInstanceIdentifier=rid, SkipFinalSnapshot=True, DeleteAutomatedBackups=True)
        wait_deleted(rds, "instance", rid)
    else:
        print(f"Deleting cluster: {rid}")
        try:
            cluster = rds.describe_db_clusters(DBClusterIdentifier=rid)["DBClusters"][0]
            member_ids = [m["DBInstanceIdentifier"] for m in cluster.get("DBClusterMembers", [])]
            
            for inst_id in member_ids:
                try:
                    print(f"  Deleting cluster instance: {inst_id}")
                    rds.delete_db_instance(DBInstanceIdentifier=inst_id, SkipFinalSnapshot=True, DeleteAutomatedBackups=True)
                    wait_deleted(rds, "instance", inst_id)
                except ClientError:
                    pass
            
            print("  Deleting cluster itself")
            rds.delete_db_cluster(DBClusterIdentifier=rid, SkipFinalSnapshot=True)
            wait_deleted(rds, "cluster", rid)
        except ClientError as e:
            print(f"Error deleting cluster: {e}")

# -------------- rollback helpers --------------

def find_old_resource_like(rds, base_identifier: str) -> Optional[Dict]:
    """Find old blue copy (e.g., base-old1). Returns dict with keys: type, id, class."""
    suffixes = OLD_RESOURCE_SUFFIXES
    try:
        insts = rds.describe_db_instances().get("DBInstances", [])
        for i in insts:
            iid = i["DBInstanceIdentifier"]
            if any(iid == base_identifier + suf for suf in suffixes):
                return {"type": "instance", "id": iid, "class": i.get("DBInstanceClass")}
    except ClientError:
        pass
    try:
        clus = rds.describe_db_clusters().get("DBClusters", [])
        for c in clus:
            cid = c["DBClusterIdentifier"]
            if any(cid == base_identifier + suf for suf in suffixes):
                writer = None
                for m in c.get("DBClusterMembers", []):
                    if m.get("IsClusterWriter"):
                        writer = m.get("DBInstanceIdentifier")
                        break
                klass = None
                if writer:
                    try:
                        inst = rds.describe_db_instances(DBInstanceIdentifier=writer)["DBInstances"][0]
                        klass = inst.get("DBInstanceClass")
                    except ClientError:
                        pass
                return {"type": "cluster", "id": cid, "class": klass}
    except ClientError:
        pass
    return None

def latest_pre_resize_snapshot_id(rds, identifier: str) -> Optional[str]:
    """
    Look for snapshots created by this tool: <identifier>-pre-resize-YYYYmmdd-HHMMSS
    Return the latest id if found.
    """
    ts = None
    latest = None
    try:
        resp = rds.describe_db_snapshots(DBInstanceIdentifier=identifier)
        for s in resp.get("DBSnapshots", []):
            sid = s.get("DBSnapshotIdentifier", "")
            if sid.startswith(f"{identifier}-pre-resize-"):
                t = s.get("SnapshotCreateTime")
                if t and (ts is None or t > ts):
                    ts = t
                    latest = sid
    except ClientError:
        pass
    try:
        resp = rds.describe_db_cluster_snapshots(DBClusterIdentifier=identifier)
        for s in resp.get("DBClusterSnapshots", []):
            sid = s.get("DBClusterSnapshotIdentifier", "")
            if sid.startswith(f"{identifier}-pre-resize-"):
                t = s.get("SnapshotCreateTime")
                if t and (ts is None or t > ts):
                    ts = t
                    latest = sid
    except ClientError:
        pass
    return latest

def rollback_with_reverse_bg(rds, cw, identifier: str):
    """Create reverse Blue/Green to rollback to previous instance class after switchover."""
    print("\n=== Rollback Process ===")
    print("Searching for old instance/cluster...")
    
    # Strip any -old suffix from identifier to get base name
    base_identifier = identifier
    for suffix in OLD_RESOURCE_SUFFIXES:
        if identifier.endswith(suffix):
            base_identifier = identifier[:-len(suffix)]
            print(f"Detected identifier has suffix, using base: {base_identifier}")
            break
    
    old = find_old_resource_like(rds, base_identifier)
    if not old or not old.get("class"):
        print("\n❌ Rollback not possible:")
        print(f"  - No old copy found for base identifier: {base_identifier}")
        print("  - Looking for: {}-old1, {}-old2, etc.".format(base_identifier, base_identifier))
        print("  - This means either:")
        print("    1. You haven't switched yet (nothing to rollback)")
        print("    2. The old resource was already deleted")
        
        snap = latest_pre_resize_snapshot_id(rds, base_identifier)
        if snap:
            print(f"\n💡 Alternative: Restore from snapshot '{snap}' manually in AWS Console")
        else:
            print("\n💡 No pre-resize snapshot found by this tool")
        return

    target_class = old["class"]
    print(f"\n✅ Found old copy: {old['id']} (class: {target_class})")
    print(f"   Current: {base_identifier} → Will rollback to: {target_class}")

    # Pre-checks (use base identifier which is the current primary)
    print("\nRunning pre-rollback checks...")
    metrics = prechecks(rds, cw, base_identifier)
    if not print_precheck_summary(metrics):
        print("\n⚠️  Pre-checks failed")
        confirm = input("Continue with rollback anyway? Type 'yes': ").strip().lower()
        if confirm != "yes":
            print("Rollback cancelled")
            return

    # Create reverse Blue/Green (target the base identifier which is current primary)
    print("\n🔄 Creating reverse Blue/Green deployment...")
    bg_id = create_bg(rds, base_identifier, target_class)
    
    is_clu, desc = is_cluster(rds, base_identifier)
    storage_gb = None if is_clu else desc.get("AllocatedStorage")
    print_eta_note(desc.get("Engine"), storage_gb)
    
    print(f"\n⏳ Waiting for rollback Blue/Green to be ready: {bg_id}")
    if not wait_switch_ready(rds, bg_id):
        print("\n⚠️  Reverse Blue/Green not ready. Check AWS Console.")
        print(f"   Blue/Green ID: {bg_id}")
        return

    print("\n🔄 Switching back to previous instance class...")
    switch_over(rds, bg_id)
    verify_same_identifier_and_endpoint(rds, base_identifier)
    
    print("\nRunning post-rollback checks...")
    post_checks(rds, cw, base_identifier)
    
    print("\n✅ Rollback completed successfully!")
    print(f"   {base_identifier} is now back on instance class: {target_class}")

# -------------- small navigation loop helpers --------------

def pause():
    input("\nPress Enter to continue...")

def mode_menu() -> str:
    print("\n=== Mode Selection ===")
    print("  1) Start a new Blue/Green instance resize process")
    print("  2) Resume from an existing Blue/Green deployment in this region")
    print("  0) Quit")
    return input("Enter 1, 2 or 0: ").strip()

def resume_action_menu() -> str:
    print("\nChoose action:")
    print("  4) switch")
    print("  5) rollback")
    print("  7) show status")
    print("  8) delete Blue/Green deployment record")
    print("  9) delete an old blue instance or cluster")
    print("  0) Back")
    return input("Enter 4, 5, 7, 8, 9 or 0: ").strip()

def new_flow_action_menu() -> str:
    print("\nChoose action:")
    print("  1) Create Blue/Green deployment (precheck, snapshot, create, wait)")
    print("  2) Switch to new instance class")
    print("  3) Rollback to previous instance class")
    print("  4) Cleanup (delete BG record or old resources)")
    print("  5) Advanced options")
    print("  0) Back")
    return input("Enter choice: ").strip()

def advanced_menu() -> str:
    print("\nAdvanced options:")
    print("  1) Precheck only")
    print("  2) Snapshot only")
    print("  3) Show Blue/Green status")
    print("  4) Postcheck only")
    print("  0) Back")
    return input("Enter choice: ").strip()

def cleanup_menu() -> str:
    print("\nCleanup options:")
    print("  1) Delete Blue/Green deployment record")
    print("  2) Delete old resource (instance or cluster)")
    print("  0) Back")
    return input("Enter choice: ").strip()

# -------------- main --------------

def main():
    sess = get_session_from_env()
    print_environment_banner(sess)
    rds = sess.client("rds")
    cw = sess.client("cloudwatch")

    while True:
        m = mode_menu()
        if m == "0":
            print("Goodbye")
            return

        if m == "2":
            while True:
                bg_id = choose_existing_bg(rds)
                if not bg_id:
                    break
                d = bg_status(rds, bg_id)
                if d is None:
                    print("That Blue/Green deployment no longer exists.")
                    continue
                src = d.get("Source", "")
                identifier = extract_source_id(src)
                if not identifier:
                    print("Could not determine source identifier from Blue/Green deployment.")
                    print("Please select the associated database:")
                    identifier = choose_target(rds)["id"]
                while True:
                    c = resume_action_menu()
                    if c == "0":
                        break

                    s = bg_status(rds, bg_id)
                    if s is None:
                        print("This Blue/Green deployment no longer exists. Returning to the list.")
                        break

                    if c == "4":
                        switch_over(rds, bg_id)
                        verify_same_identifier_and_endpoint(rds, identifier)
                        post_checks(rds, cw, identifier)
                        pause()

                    elif c == "5":
                        if s.get("Status") in ("SWITCH_READY", "AVAILABLE"):
                            print("This Blue/Green has not switched yet. Rollback means do not proceed. Delete it with option 8 if you want.")
                            pause()
                        else:
                            rollback_with_reverse_bg(rds, cw, identifier)
                            pause()

                    elif c == "7":
                        s = bg_status(rds, bg_id)
                        if s is None:
                            print("This Blue/Green deployment was deleted. Returning to the list.")
                            break
                        print(json.dumps(s, indent=2, default=str))
                        pause()

                    elif c == "8":
                        delete_bg_deployment_record(rds, bg_id)
                        print("Blue/Green record deleted. Returning to the list.")
                        pause()
                        break

                    elif c == "9":
                        delete_old_resource(rds)
                        pause()

                    else:
                        print("Invalid choice")
                # back to mode menu
            continue

        # New flow - simplified workflow
        chosen = choose_target(rds)
        identifier = chosen["id"]
        
        while True:
            action = new_flow_action_menu()
            if action == "0":
                break
            
            # 1) Create Blue/Green deployment (full workflow)
            if action == "1":
                target_class = show_and_pick_target_class(rds, identifier)
                
                # Get current instance class for suitability check
                is_clu, desc = is_cluster(rds, identifier)
                if is_clu:
                    writer = next((m for m in desc.get("DBClusterMembers", []) if m.get("IsClusterWriter")), None)
                    current_class = None
                    if writer:
                        try:
                            writer_inst = rds.describe_db_instances(DBInstanceIdentifier=writer["DBInstanceIdentifier"])["DBInstances"][0]
                            current_class = writer_inst.get("DBInstanceClass")
                        except ClientError as e:
                            print(f"Warning: Could not get writer instance class: {e}")
                else:
                    current_class = desc.get("DBInstanceClass")
                
                # Pre-checks
                metrics = prechecks(rds, cw, identifier)
                if not print_precheck_summary(metrics):
                    print("Pre-checks failed. Aborting.")
                    pause()
                    continue
                
                # Suitability check
                if current_class:
                    suitable = check_target_class_suitability(rds, cw, identifier, current_class, target_class, metrics)
                    if not suitable:
                        print("\n⚠️  Target instance class appears unsuitable.")
                        confirm = input("Proceed anyway? Type 'yes': ").strip().lower()
                        if confirm != "yes":
                            print("Cancelled")
                            pause()
                            continue
                
                # Create snapshot
                create_snapshot(rds, identifier)
                
                # Create Blue/Green
                bg_id = create_bg(rds, identifier, target_class)
                storage_gb = None if is_clu else desc.get("AllocatedStorage")
                print_eta_note(desc.get("Engine"), storage_gb)
                
                print(f"\n✅ Blue/Green created: {bg_id}")
                print("Waiting for deployment to be ready...")
                
                if wait_switch_ready(rds, bg_id):
                    print("\n✅ Blue/Green is ready to switch!")
                else:
                    print("\n⚠️  Not ready yet. Check AWS console.")
                pause()
            
            # 2) Switch to new instance class
            elif action == "2":
                bg_id = choose_existing_bg(rds)
                if not bg_id:
                    continue
                
                bg = bg_status(rds, bg_id)
                if not bg:
                    print("Blue/Green deployment not found")
                    pause()
                    continue
                
                print("\nRunning pre-switch checks...")
                metrics = prechecks(rds, cw, identifier)
                if not print_precheck_summary(metrics):
                    confirm = input("Pre-checks failed. Switch anyway? Type 'yes': ").strip().lower()
                    if confirm != "yes":
                        print("Cancelled")
                        pause()
                        continue
                
                switch_over(rds, bg_id)
                verify_same_identifier_and_endpoint(rds, identifier)
                post_checks(rds, cw, identifier)
                print("\n✅ Switch completed successfully!")
                pause()
            
            # 3) Rollback to previous instance class
            elif action == "3":
                print("\n=== Rollback to Previous Instance Class ===")
                print("This will:")
                print("  1. Find your old instance (e.g., mydb-old1)")
                print("  2. Create a reverse Blue/Green deployment")
                print("  3. Switch back to the previous instance class")
                print("\nNote: This only works AFTER you've switched over.")
                
                confirm = input("\nProceed with rollback? Type 'yes': ").strip().lower()
                if confirm != "yes":
                    print("Cancelled")
                    pause()
                    continue
                
                rollback_with_reverse_bg(rds, cw, identifier)
                pause()
            
            # 4) Cleanup submenu
            elif action == "4":
                while True:
                    cleanup_action = cleanup_menu()
                    if cleanup_action == "0":
                        break
                    
                    if cleanup_action == "1":  # Delete BG record
                        bg_id = choose_existing_bg(rds)
                        if bg_id:
                            delete_bg_deployment_record(rds, bg_id)
                            pause()
                    
                    elif cleanup_action == "2":  # Delete old resource
                        delete_old_resource(rds)
                        pause()
            
            # 5) Advanced options submenu
            elif action == "5":
                while True:
                    adv_action = advanced_menu()
                    if adv_action == "0":
                        break
                    
                    if adv_action == "1":  # Precheck only
                        metrics = prechecks(rds, cw, identifier)
                        print_precheck_summary(metrics)
                        pause()
                    
                    elif adv_action == "2":  # Snapshot only
                        create_snapshot(rds, identifier)
                        pause()
                    
                    elif adv_action == "3":  # Show status
                        bg_id = choose_existing_bg(rds)
                        if bg_id:
                            bg = bg_status(rds, bg_id)
                            if bg:
                                print(json.dumps(bg, indent=2, default=str))
                            else:
                                print("Blue/Green deployment not found")
                            pause()
                    
                    elif adv_action == "4":  # Postcheck only
                        post_checks(rds, cw, identifier)
                        pause()
            
            else:
                print("Invalid choice")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nAborted by user")

