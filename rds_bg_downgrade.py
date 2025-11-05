#!/usr/bin/env python3
# --- self bootstrapping: ensure local .venv with boto3, then re exec ---
import os, sys, subprocess, venv, pathlib

def _ensure_local_venv_with_boto3():
    try:
        import boto3  # already installed
        return
    except Exception:
        root = pathlib.Path(__file__).resolve().parent
        venv_dir = root / ".venv"
        if not venv_dir.exists():
            print("Creating virtualenv at .venv")
            venv.EnvBuilder(with_pip=True).create(str(venv_dir))

        py_dir = "Scripts" if os.name == "nt" else "bin"
        py = venv_dir / py_dir / "python"

        print("Installing dependencies: pip, boto3")
        subprocess.checkcall = subprocess.check_call  # alias to avoid lint nags
        subprocess.checkcall([str(py), "-m", "pip", "install", "-U", "pip"])
        subprocess.checkcall([str(py), "-m", "pip", "install", "boto3>=1.34"])

        # re exec current script under the venv interpreter
        os.execv(str(py), [str(py), __file__] + sys.argv[1:])

_ensure_local_venv_with_boto3()
# --- end bootstrap ---

"""
rds_bg_downgrade.py

Blue Green downgrade helper for Amazon RDS and Aurora.

Before you run:
  Make sure you already assumed the correct AWS profile and region.
  Example:
    export AWS_PROFILE=prod
    export AWS_REGION=eu-west-1

What this script does:
  Start a new Blue Green downgrade, or resume an existing Blue Green in the region.
  Pre checks, snapshot with progress, orderable class picker.
  Create Blue Green, show ETA, wait until SWITCH_READY or AVAILABLE, switch.
  Verify that the original identifier and endpoint remain, with the new instance class.
  Post checks.
  Rollback:
    If you already switched and still have an old copy with a suffix like -old1,
    the script creates a reverse Blue Green targeting that old instance class, waits until ready, then switches back.
    If there is no old copy, it points you to the latest pre downgrade snapshot.
  Optional cleanup: delete the Blue Green deployment record, delete the old blue instance or cluster.
  All long running deletes are waited on, success is printed, and the menu does not exit abruptly.

Requirements:
  pip install boto3
"""

import datetime as dt
import json
import os
import re
import socket
import sys
import time
from typing import Dict, List, Optional, Tuple

import boto3
from botocore.exceptions import (
    ClientError,
    NoRegionError,
    ProfileNotFound,
    ParamValidationError,
)

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
    print("=== RDS Blue Green Downgrade Helper ===")
    print(f"Using AWS profile: {profile}")
    if not region:
        print("No region resolved. Set AWS_REGION or configure a default region, example:")
        print("  export AWS_REGION=eu-west-1")
        sys.exit(2)
    print(f"Using AWS region: {region}")
    print("Make sure the profile and region above are correct.\n")

# -------------- describe helpers --------------

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
    items = list_db_targets(rds)
    if not items:
        print("No RDS instances or Aurora clusters found in this region")
        sys.exit(2)
    print("\nAvailable databases in this region:")
    for idx, it in enumerate(items, 1):
        storage = f", storage: {it['storage_gb']} GB" if it.get("storage_gb") else ""
        st = f"[{it['type']}] {it['id']} | {it.get('engine')} {it.get('version')} | class: {it.get('class')}{storage}"
        print(f"  {idx}) {st}")
    while True:
        sel = input("Choose a number: ").strip()
        if sel.isdigit() and 1 <= int(sel) <= len(items):
            chosen = items[int(sel) - 1]
            print(f"Selected: {chosen['id']}")
            return chosen
        print("Invalid choice, try again")

def is_cluster(rds, identifier: str) -> Tuple[bool, Dict]:
    try:
        resp = rds.describe_db_clusters(DBClusterIdentifier=identifier)
        if resp.get("DBClusters"):
            return True, resp["DBClusters"][0]
    except ClientError as e:
        if e.response["Error"]["Code"] not in ("DBClusterNotFoundFault", "InvalidDBClusterStateFault"):
            raise
    try:
        resp = rds.describe_db_instances(DBInstanceIdentifier=identifier)
        if resp.get("DBInstances"):
            return False, resp["DBInstances"][0]
    except ClientError as e:
        if e.response["Error"]["Code"] != "DBInstanceNotFound":
            raise
    print(f"Identifier not found: {identifier}")
    sys.exit(2)

def writer_instance_id(cluster_desc: Dict) -> str:
    for m in cluster_desc.get("DBClusterMembers", []):
        if m.get("IsClusterWriter"):
            return m["DBInstanceIdentifier"]
    raise RuntimeError("Writer instance not found in cluster")

def source_engine_info(is_clu: bool, desc: Dict) -> Tuple[str, str, Optional[str]]:
    if is_clu:
        return (desc.get("Engine") or "").lower(), desc.get("EngineVersion") or "", None
    return (desc.get("Engine") or "").lower(), desc.get("EngineVersion") or "", desc.get("StorageType")

def source_arn(desc: Dict, is_clu: bool) -> str:
    return desc["DBClusterArn"] if is_clu else desc["DBInstanceArn"]

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
    is_clu, desc = is_cluster(rds, identifier)
    dim_name = "DBInstanceIdentifier"
    inst_id = writer_instance_id(desc) if is_clu else desc["DBInstanceIdentifier"]
    return {
        "CPUUtilization_Average_percent": get_metric_avg(cw, "CPUUtilization", dim_name, inst_id),
        "FreeableMemory_Average_bytes": get_metric_avg(cw, "FreeableMemory", dim_name, inst_id),
        "ReadIOPS_Average": get_metric_avg(cw, "ReadIOPS", dim_name, inst_id),
        "WriteIOPS_Average": get_metric_avg(cw, "WriteIOPS", dim_name, inst_id),
        "DatabaseConnections_Average": get_metric_avg(cw, "DatabaseConnections", dim_name, inst_id),
    }

def print_precheck_summary(metrics: Dict[str, Optional[float]]) -> bool:
    print("\nPre checks, last 15 minutes average:")
    for k, v in metrics.items():
        print(f"  {k}: {v}")
    ok = True
    cpu = metrics.get("CPUUtilization_Average_percent") or 0.0
    free_mem = metrics.get("FreeableMemory_Average_bytes") or 0.0
    conns = metrics.get("DatabaseConnections_Average") or 0.0
    if cpu > 40.0:
        ok = False
        print("  Warning: CPU average is above 40 percent")
    if free_mem < 1.5 * 1024**3:
        ok = False
        print("  Warning: FreeableMemory average is below 1.5 GiB")
    if conns > 0 and cpu > 30.0 and free_mem < 2 * 1024**3:
        ok = False
        print("  Warning: combined pressure suggests risk for downgrade")
    print(f"Pre checks pass: {ok}")
    return ok

# -------------- snapshot --------------

def wait_snapshot_progress(rds, is_clu: bool, snap_id: str, poll: int = 10):
    print("\nWaiting for snapshot to be Available:")
    last = -1
    while True:
        try:
            if is_clu:
                resp = rds.describe_db_cluster_snapshots(DBClusterSnapshotIdentifier=snap_id)
                s = resp["DBClusterSnapshots"][0]
            else:
                resp = rds.describe_db_snapshots(DBSnapshotIdentifier=snap_id)
                s = resp["DBSnapshots"][0]
            status = s.get("Status") or s.get("DBSnapshotAttributesResult", {}).get("Status")
            pct = s.get("PercentProgress", 0)
            if pct != last:
                print(f"  {pct} percent, status: {status}")
                last = pct
            if status == "available":
                print("Snapshot ready")
                return
        except ClientError as e:
            if e.response["Error"]["Code"] in ("DBSnapshotNotFound", "DBClusterSnapshotNotFoundFault"):
                pass
            else:
                raise
        time.sleep(poll)

def create_snapshot(rds, identifier: str) -> str:
    ts = now_utc().strftime("%Y%m%d-%H%M%S")
    is_clu, _ = is_cluster(rds, identifier)
    snap_id = f"{identifier}-pre-downgrade-{ts}"
    if is_clu:
        print(f"\nCreating cluster snapshot: {snap_id}")
        rds.create_db_cluster_snapshot(
            DBClusterSnapshotIdentifier=snap_id,
            DBClusterIdentifier=identifier,
            Tags=[{"Key": "purpose", "Value": "pre-downgrade"}],
        )
    else:
        print(f"\nCreating instance snapshot: {snap_id}")
        rds.create_db_snapshot(
            DBSnapshotIdentifier=snap_id,
            DBInstanceIdentifier=identifier,
            Tags=[{"Key": "purpose", "Value": "pre-downgrade"}],
        )
    wait_snapshot_progress(rds, is_clu, snap_id)
    return snap_id

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
    is_clu, desc = is_cluster(rds, identifier)
    eng, ver, storage = source_engine_info(is_clu, desc)
    allowed = list_orderable_classes(rds, eng, ver, storage)
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
    print(f"\nETA note: Blue Green provisioning for {engine}, storage {size_note}, often takes about {lo} to {hi} minutes.")
    print("You can press Ctrl+C to stop watching at any time, the deployment continues in AWS.")
    print("You can resume later from Resume menu, or switch with action 4 using the list of existing deployments.")

# -------------- Blue Green core --------------

def create_bg(rds, identifier: str, target_class: str) -> str:
    is_clu, desc = is_cluster(rds, identifier)
    name = f"bg-{identifier}-{now_utc().strftime('%Y%m%d-%H%M%S')}"
    src = source_arn(desc, is_clu)
    print(f"\nCreating Blue Green deployment: {name}")
    resp = rds.create_blue_green_deployment(
        BlueGreenDeploymentName=name,
        Source=src,
        TargetDBInstanceClass=target_class,
        Tags=[{"Key": "purpose", "Value": "downgrade"}],
    )
    bg_id = resp["BlueGreenDeployment"]["BlueGreenDeploymentIdentifier"]
    print(f"Blue Green created: {bg_id}")
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

def wait_switch_ready(rds, bg_id: str, poll: int = 30, timeout_min: int = 90) -> bool:
    print("\nWaiting for SWITCH_READY")
    start = time.time()
    last = ""
    deadline = start + timeout_min * 60
    while time.time() < deadline:
        s = bg_status(rds, bg_id)
        if s is None:
            print("  Blue Green deployment not found")
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
    print(f"\nSwitching Blue Green: {bg_id}")
    rds.switchover_blue_green_deployment(BlueGreenDeploymentIdentifier=bg_id)
    while True:
        s = bg_status(rds, bg_id)
        if s is None:
            print("  Blue Green deployment not found")
            return
        st = s.get("Status")
        print(f"  status: {st}")
        if st == "SWITCHOVER_COMPLETED":
            print("Cut over completed")
            return
        if st in ("SWITCHOVER_FAILED", "DELETED"):
            raise RuntimeError(f"Cut over failed, final status: {st}")
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
    print("\nPost cut over health snapshot")
    metrics = prechecks(rds, cw, identifier)
    print_precheck_summary(metrics)

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
    if isinstance(src, dict):
        return src.get("DBClusterIdentifier") or src.get("DBInstanceIdentifier")
    if isinstance(src, str):
        return parse_identifier_from_arn(src)
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
    items = list_bg_deployments(rds)
    if not items:
        print("No existing Blue Green deployments found in this region")
        return None
    print("\nExisting Blue Green deployments:")
    for idx, it in enumerate(items, 1):
        when = str(it.get("created") or "")
        print(f"  {idx}) {it['short']} | status: {it['status']} | engine: {it.get('engine')} | source: {it['source_id']} | created: {when}")
    while True:
        sel = input("Pick a number, or 0 to cancel: ").strip()
        if sel.isdigit():
            i = int(sel)
            if i == 0:
                return None
            if 1 <= i <= len(items):
                return items[i - 1]["id"]
        print("Invalid choice, try again")

# -------------- cleanup with waits --------------

def wait_instance_deleted(rds, instance_id: str, poll: int = 15):
    print(f"  waiting for instance {instance_id} to delete")
    while True:
        try:
            rds.describe_db_instances(DBInstanceIdentifier=instance_id)
            print("   still deleting")
            time.sleep(poll)
        except ClientError as e:
            if e.response["Error"]["Code"] == "DBInstanceNotFound":
                print("  Deleted")
                return
            raise

def wait_cluster_deleted(rds, cluster_id: str, poll: int = 20):
    print(f"  waiting for cluster {cluster_id} to delete")
    while True:
        try:
            rds.describe_db_clusters(DBClusterIdentifier=cluster_id)
            print("   still deleting")
            time.sleep(poll)
        except ClientError as e:
            if e.response["Error"]["Code"] in ("DBClusterNotFoundFault",):
                print("  Deleted")
                return
            raise

def delete_bg_deployment_record(rds, bg_id: str):
    print(f"Deleting Blue Green deployment record: {bg_id}")
    rds.delete_blue_green_deployment(BlueGreenDeploymentIdentifier=bg_id)
    while True:
        s = bg_status(rds, bg_id)
        if s is None:
            print("  Deleted")
            return
        print("  still present")
        time.sleep(5)

def delete_old_resource(rds):
    """
    List likely old resources with -old suffix, user selects one to delete.
    Wait until deletion completes, then confirm.
    """
    insts = rds.describe_db_instances().get("DBInstances", [])
    clus = rds.describe_db_clusters().get("DBClusters", [])
    suffixes = ("-old", "-old1", "-old2", "-blue", "-previous")
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
        wait_instance_deleted(rds, rid)
    else:
        print(f"Deleting cluster: {rid}")
        try:
            m = [m["DBInstanceIdentifier"] for m in rds.describe_db_clusters(DBClusterIdentifier=rid)["DBClusters"][0]["DBClusterMembers"]]
            for iid in m:
                try:
                    print(f"  deleting cluster instance: {iid}")
                    rds.delete_db_instance(DBInstanceIdentifier=iid, SkipFinalSnapshot=True, DeleteAutomatedBackups=True)
                    wait_instance_deleted(rds, iid)
                except ClientError:
                    pass
            print("  deleting cluster itself")
            rds.delete_db_cluster(DBClusterIdentifier=rid, SkipFinalSnapshot=True)
            wait_cluster_deleted(rds, rid)
        except ClientError as e:
            print(f"Could not delete cluster: {e}")

# -------------- rollback helpers --------------

def find_old_resource_like(rds, base_identifier: str) -> Optional[Dict]:
    """
    Find an old blue copy for this DB, for example base-old1, base-old, base-previous.
    Returns dict with keys: type, id, class.
    """
    suffixes = ("-old1", "-old2", "-old", "-blue", "-previous")
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

def latest_pre_downgrade_snapshot_id(rds, identifier: str) -> Optional[str]:
    """
    Look for snapshots created by this tool: <identifier>-pre-downgrade-YYYYmmdd-HHMMSS
    Return the latest id if found.
    """
    ts = None
    latest = None
    try:
        resp = rds.describe_db_snapshots(DBInstanceIdentifier=identifier)
        for s in resp.get("DBSnapshots", []):
            sid = s.get("DBSnapshotIdentifier", "")
            if sid.startswith(f"{identifier}-pre-downgrade-"):
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
            if sid.startswith(f"{identifier}-pre-downgrade-"):
                t = s.get("SnapshotCreateTime")
                if t and (ts is None or t > ts):
                    ts = t
                    latest = sid
    except ClientError:
        pass
    return latest

def rollback_with_reverse_bg(rds, cw, identifier: str):
    """
    Create a new Blue Green from the current primary to the old instance class,
    wait until ready, then switch back.
    """
    print("\nRollback: reverse Blue Green to previous instance class")

    old = find_old_resource_like(rds, identifier)
    if not old or not old.get("class"):
        print("Could not find an old copy with a known instance class.")
        snap = latest_pre_downgrade_snapshot_id(rds, identifier)
        if snap:
            print(f"Fallback: you can restore from snapshot {snap} manually in the console.")
        else:
            print("No pre downgrade snapshot found by the tool.")
        return

    target_class = old["class"]
    print(f"Found old copy: [{old['type']}] {old['id']} with class {target_class}")
    print("Creating reverse Blue Green from the current primary to that class...")

    if not print_precheck_summary(prechecks(rds, cw, identifier)):
        print("Pre checks did not pass, aborting rollback")
        return

    bg_id = create_bg(rds, identifier, target_class)
    is_clu, desc = is_cluster(rds, identifier)
    storage_gb = None if is_clu else desc.get("AllocatedStorage")
    engine = desc.get("Engine")
    print_eta_note(engine, storage_gb)
    print(f"\nWaiting until SWITCH_READY for rollback Blue Green: {bg_id}")
    if not wait_switch_ready(rds, bg_id):
        print("Reverse Blue Green not ready for switch, inspect in console")
        return

    print("Switching back...")
    switch_over(rds, bg_id)
    verify_same_identifier_and_endpoint(rds, identifier)
    post_checks(rds, cw, identifier)
    print("\nRollback completed. You are back on the previous instance class.")

# -------------- small navigation loop helpers --------------

def pause():
    input("\nPress Enter to continue...")

def mode_menu() -> str:
    print("\n=== Mode Selection ===")
    print("  1) Start a new Blue Green downgrade process")
    print("  2) Resume from an existing Blue Green deployment in this region")
    print("  0) Quit")
    return input("Enter 1, 2 or 0: ").strip()

def resume_action_menu() -> str:
    print("\nChoose action:")
    print("  4) switch")
    print("  5) rollback")
    print("  7) show status")
    print("  8) delete Blue Green deployment record")
    print("  9) delete an old blue instance or cluster")
    print("  0) Back")
    return input("Enter 4, 5, 7, 8, 9 or 0: ").strip()

def new_flow_action_menu() -> str:
    print("\nChoose action:")
    print("  1) precheck")
    print("  2) snapshot")
    print("  3) create and wait")
    print("  4) switch using an existing Blue Green")
    print("  5) rollback using an existing Blue Green")
    print("  6) postcheck")
    print("  7) show status of a Blue Green from the region list")
    print("  8) delete Blue Green deployment record")
    print("  9) delete an old blue instance or cluster")
    print("  0) Back")
    return input("Enter 0..9: ").strip()

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
                    print("That Blue Green deployment no longer exists.")
                    continue
                src = d.get("Source", {})
                identifier = extract_source_id(src) or choose_target(rds)["id"]
                while True:
                    c = resume_action_menu()
                    if c == "0":
                        break

                    s = bg_status(rds, bg_id)
                    if s is None:
                        print("This Blue Green deployment no longer exists. Returning to the list.")
                        break

                    if c == "4":
                        switch_over(rds, bg_id)
                        verify_same_identifier_and_endpoint(rds, identifier)
                        post_checks(rds, cw, identifier)
                        pause()

                    elif c == "5":
                        if s.get("Status") in ("SWITCH_READY", "AVAILABLE"):
                            print("This Blue Green has not switched yet. Rollback means do not proceed. Delete it with option 8 if you want.")
                            pause()
                        else:
                            rollback_with_reverse_bg(rds, cw, identifier)
                            pause()

                    elif c == "7":
                        s = bg_status(rds, bg_id)
                        if s is None:
                            print("This Blue Green deployment was deleted. Returning to the list.")
                            break
                        print(json.dumps(s, indent=2, default=str))
                        pause()

                    elif c == "8":
                        delete_bg_deployment_record(rds, bg_id)
                        print("Blue Green record deleted. Returning to the list.")
                        pause()
                        break

                    elif c == "9":
                        delete_old_resource(rds)
                        pause()

                    else:
                        print("Invalid choice")
                # back to mode menu
            continue

        # New flow
        chosen = choose_target(rds)
        identifier = chosen["id"]
        while True:
            c = new_flow_action_menu()
            if c == "0":
                break
            if c == "1":
                ok = print_precheck_summary(prechecks(rds, cw, identifier))
                print("Pre checks pass" if ok else "Pre checks did not pass")
                pause()
            elif c == "2":
                create_snapshot(rds, identifier)
                pause()
            elif c == "3":
                target_class = show_and_pick_target_class(rds, identifier)
                if not print_precheck_summary(prechecks(rds, cw, identifier)):
                    print("Pre checks did not pass, aborting Blue Green creation")
                    pause()
                    continue
                create_snapshot(rds, identifier)
                bg_id = create_bg(rds, identifier, target_class)
                is_clu, desc = is_cluster(rds, identifier)
                storage_gb = None if is_clu else desc.get("AllocatedStorage")
                engine = desc.get("Engine")
                print_eta_note(engine, storage_gb)
                print(f"\nRemember this Blue Green id: {bg_id}")
                ready = wait_switch_ready(rds, bg_id)
                print("Green is ready for switch" if ready else "Not ready for switch, inspect in console")
                pause()
            elif c in ("4", "5", "7", "8", "9"):
                bg_id = choose_existing_bg(rds)
                if not bg_id:
                    continue
                s = bg_status(rds, bg_id)
                if s is None:
                    print("That Blue Green id no longer exists.")
                    pause()
                    continue
                if c == "4":
                    switch_over(rds, bg_id)
                    verify_same_identifier_and_endpoint(rds, identifier)
                    post_checks(rds, cw, identifier)
                    pause()
                elif c == "5":
                    if s.get("Status") in ("SWITCH_READY", "AVAILABLE"):
                        print("That Blue Green has not switched yet. Rollback means do not proceed. Delete it with option 8 if you want.")
                        pause()
                    else:
                        rollback_with_reverse_bg(rds, cw, identifier)
                        pause()
                elif c == "7":
                    print(json.dumps(s, indent=2, default=str))
                    pause()
                elif c == "8":
                    delete_bg_deployment_record(rds, bg_id)
                    pause()
                elif c == "9":
                    delete_old_resource(rds)
                    pause()
            elif c == "6":
                post_checks(rds, cw, identifier)
                pause()
            else:
                print("Invalid choice")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nAborted by user")