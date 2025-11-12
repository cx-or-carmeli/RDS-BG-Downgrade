#!/usr/bin/env python3
"""RDS Blue/Green Instance Resize - Automates RDS instance class changes via Blue/Green deployments."""
import os, pathlib, subprocess, sys, venv

# Auto-install boto3 if missing
def _bootstrap_boto3():
    try:
        import boto3; return  # noqa
    except ImportError:
        venv_dir = pathlib.Path(__file__).resolve().parent / ".venv"
        if not venv_dir.exists():
            print("Creating venv..."); venv.EnvBuilder(with_pip=True).create(str(venv_dir))
        py_exe = venv_dir / ("Scripts" if os.name == "nt" else "bin") / "python"
        print("Installing boto3...")
        subprocess.check_call([str(py_exe), "-m", "pip", "install", "-q", "-U", "pip", "boto3>=1.34"])
        os.execv(str(py_exe), [str(py_exe), __file__] + sys.argv[1:])

_bootstrap_boto3()

import datetime as dt, json, time
from typing import Dict, List, Optional, Tuple
import boto3
from botocore.exceptions import ClientError, NoRegionError, ParamValidationError, ProfileNotFound
from config import (GIB, CPU_WARNING_THRESHOLD as CPU_WARN, CPU_CRITICAL_THRESHOLD as CPU_CRIT,
                    MEMORY_WARNING_GIB as MEM_WARN, MEMORY_CRITICAL_GIB as MEM_CRIT,
                    OLD_RESOURCE_SUFFIXES as OLD_SUFFIXES, DEFAULT_POLL_INTERVAL as POLL_INTERVAL,
                    BG_SWITCH_POLL_INTERVAL as BG_POLL, BG_TIMEOUT_MINUTES as BG_TIMEOUT, INSTANCE_SPECS,
                    PREFERRED_INSTANCE_TYPES, MAX_INSTANCE_DISPLAY, DELETABLE_INSTANCE_STATES,
                    ETA_ESTIMATES, STORAGE_THRESHOLDS)

# Get current time in UTC
def now_utc():
    return dt.datetime.now(getattr(dt, "UTC", dt.timezone.utc))

# Calculate time window for metrics
def time_range(minutes=15):
    end = now_utc()
    return end - dt.timedelta(minutes=minutes), end

# Let user pick AWS region
def choose_region():
    common_regions = [
        "us-east-1", "us-east-2", "us-west-1", "us-west-2",
        "eu-west-1", "eu-west-2", "eu-west-3", "eu-central-1",
        "ap-southeast-1", "ap-southeast-2", "ap-northeast-1"
    ]
    print("\n=== Select Region ===")
    for i, region in enumerate(common_regions, 1):
        print(f"  {i}) {region}")
    print(f"  {len(common_regions) + 1}) Enter custom region")
    
    choice = input("Choose region: ").strip()
    if choice.isdigit():
        idx = int(choice)
        if 1 <= idx <= len(common_regions):
            return common_regions[idx - 1]
        elif idx == len(common_regions) + 1:
            return input("Enter region (e.g., eu-west-2): ").strip()
    return None

# Setup AWS session from env vars or prompt for region
def get_session():
    try:
        sess = boto3.Session()
        if not sess.region_name:
            region = choose_region()
            if not region:
                print("No region selected"); sys.exit(2)
            sess = boto3.Session(region_name=region)
        return sess
    except (ProfileNotFound, NoRegionError) as e:
        print(f"Error: {e}"); sys.exit(2)

# Print startup info with profile and region
def print_banner(sess):
    print(f"\n=== RDS Blue/Green Resize ===\nProfile: {os.environ.get('AWS_PROFILE', 'default')}")
    print(f"Region: {sess.region_name}\n")

# Let user pick from a numbered list
def select_from_list(items, prompt="Choose"):
    while True:
        choice = input(f"{prompt}: ").strip()
        if choice.isdigit() and 1 <= int(choice) <= len(items):
            return items[int(choice) - 1]
        print("Invalid")

# Get all RDS instances in the account
def list_dbs(rds):
    items = []
    try:
        for page in rds.get_paginator("describe_db_instances").paginate():
            for i in page.get("DBInstances", []):
                items.append({"id": i["DBInstanceIdentifier"], "engine": i.get("Engine"),
                             "version": i.get("EngineVersion"), "class": i.get("DBInstanceClass"),
                             "storage_gb": i.get("AllocatedStorage"), "storage_type": i.get("StorageType")})
    except: pass
    return items

# Show list and let user select a database
def choose_db(rds):
    items = list_dbs(rds)
    if not items:
        print("No databases found"); sys.exit(2)
    print("\nRDS Instances:")
    for i, item in enumerate(items, 1):
        storage = f", {item.get('storage_gb')}GB" if item.get('storage_gb') else ""
        print(f"  {i}) {item['id']} | {item.get('engine')} | {item.get('class')}{storage}")
    return select_from_list(items)

# Fetch instance details from AWS
def is_cluster(rds, identifier):
    try:
        return False, rds.describe_db_instances(DBInstanceIdentifier=identifier)["DBInstances"][0]
    except ClientError:
        print(f"Instance not found: {identifier}"); sys.exit(2)

# Pull a single metric from CloudWatch
def get_metric(cw, metric, inst_id, minutes=15):
    start, end = time_range(minutes)
    resp = cw.get_metric_statistics(Namespace="AWS/RDS", MetricName=metric,
                                    Dimensions=[{"Name": "DBInstanceIdentifier", "Value": inst_id}],
                                    StartTime=start, EndTime=end, Period=300, Statistics=["Average"])
    dps = sorted(resp.get("Datapoints", []), key=lambda x: x["Timestamp"])
    return float(dps[-1]["Average"]) if dps else None

# Grab recent metrics from CloudWatch
def prechecks(rds, cw, identifier):
    _, desc = is_cluster(rds, identifier)
    inst_id = desc["DBInstanceIdentifier"]
    return {
        "CPUUtilization": get_metric(cw, "CPUUtilization", inst_id),
        "FreeableMemory": get_metric(cw, "FreeableMemory", inst_id),
        "ReadIOPS": get_metric(cw, "ReadIOPS", inst_id),
        "WriteIOPS": get_metric(cw, "WriteIOPS", inst_id),
        "Connections": get_metric(cw, "DatabaseConnections", inst_id),
    }

# Show metrics and check against thresholds
def print_checks(metrics, title="Checks"):
    print(f"\n{title} (last 15 min):")
    for k, v in metrics.items():
        print(f"  {k}: {v}")
    cpu = metrics.get("CPUUtilization") or 0
    mem = (metrics.get("FreeableMemory") or 0) / GIB
    ok = cpu <= CPU_WARN and mem >= MEM_WARN
    if not ok:
        print(f"  WARNING: CPU {cpu:.1f}% or Memory {mem:.1f}GiB concerning")
    print(f"Status: {'OK' if ok else 'Warning'}")
    return ok

# Wait for snapshot to finish
def wait_snapshot(rds, snap_id):
    print("Waiting for snapshot...")
    last_pct = -1
    while True:
        try:
            snap = rds.describe_db_snapshots(DBSnapshotIdentifier=snap_id)["DBSnapshots"][0]
            status, pct = snap.get("Status"), snap.get("PercentProgress", 0)
            if pct != last_pct:
                print(f"  {pct}% - {status}"); last_pct = pct
            if status == "available":
                print("Snapshot ready"); return
        except: pass
        time.sleep(POLL_INTERVAL)

# Take a snapshot before making changes
def create_snapshot(rds, identifier):
    snap_id = f"{identifier}-pre-resize-{now_utc().strftime('%Y%m%d-%H%M%S')}"
    print(f"\nCreating snapshot: {snap_id}")
    rds.create_db_snapshot(DBSnapshotIdentifier=snap_id, DBInstanceIdentifier=identifier,
                          Tags=[{"Key": "purpose", "Value": "pre-resize"}])
    wait_snapshot(rds, snap_id)
    return snap_id

# Check if target instance will handle the workload
def check_suitability(identifier, current_class, target_class, metrics):
    print(f"\n=== Suitability: {current_class} -> {target_class} ===")
    if current_class not in INSTANCE_SPECS or target_class not in INSTANCE_SPECS:
        print("WARNING: Specs unknown, skipping"); return True
    curr_cpu, curr_mem = INSTANCE_SPECS[current_class]
    tgt_cpu, tgt_mem = INSTANCE_SPECS[target_class]
    print(f"Current: {curr_cpu} vCPUs, {curr_mem} GiB | Target: {tgt_cpu} vCPUs, {tgt_mem} GiB")
    is_down = tgt_cpu < curr_cpu or tgt_mem < curr_mem
    print(f"Type: {'Downgrade' if is_down else 'Upgrade'}")
    
    cpu = metrics.get("CPUUtilization") or 0
    proj_cpu = cpu * (curr_cpu / tgt_cpu) if tgt_cpu > 0 else cpu
    print(f"CPU: {cpu:.1f}% -> {proj_cpu:.1f}%")
    
    mem = (metrics.get("FreeableMemory") or 0) / GIB
    proj_mem = mem + (tgt_mem - curr_mem)
    print(f"Memory: {mem:.1f}GiB -> {proj_mem:.1f}GiB free")
    
    if proj_cpu > CPU_CRIT or proj_mem < MEM_CRIT:
        print(f"\nCRITICAL: Target instance insufficient!")
        if proj_cpu > CPU_CRIT:
            print(f"   CPU would be {proj_cpu:.0f}% (max safe: {CPU_CRIT:.0f}%)")
        if proj_mem < MEM_CRIT:
            print(f"   Free memory would be {proj_mem:.1f}GiB (minimum: {MEM_CRIT:.1f}GiB)")
        print(f"\nAWS Recommendation: Choose a larger instance class")
        print(f"   Minimum safe target: ~{curr_mem}+ GiB RAM")
        return False
    
    if proj_cpu > CPU_WARN or proj_mem < MEM_WARN:
        print(f"\nWARNING: Marginal capacity")
        if proj_cpu > CPU_WARN:
            print(f"   CPU would be {proj_cpu:.0f}% (warning: {CPU_WARN:.0f}%)")
        if proj_mem < MEM_WARN:
            print(f"   Free memory would be {proj_mem:.1f}GiB (warning: {MEM_WARN:.1f}GiB)")
        print(f"   Consider a larger instance for better performance")
    else:
        print("Suitable")
    
    return True

# Get valid instance classes for this database
def list_orderable(rds, engine, version, storage_type):
    classes = set()
    def call(**kw):
        marker = None
        while True:
            if marker: kw["Marker"] = marker
            resp = rds.describe_orderable_db_instance_options(**kw)
            for opt in resp.get("OrderableDBInstanceOptions", []):
                if c := opt.get("DBInstanceClass"): classes.add(c)
            if not (marker := resp.get("Marker")): break
    for params in ([{"Engine": engine, "EngineVersion": version, "StorageType": storage_type}] if storage_type else []) + [{"Engine": engine, "EngineVersion": version}, {"Engine": engine}]:
        try:
            call(**params)
            if classes: break
        except: continue
    return sorted(classes)

# Let user pick target instance class
def pick_target_class(rds, identifier):
    _, desc = is_cluster(rds, identifier)
    allowed = list_orderable(rds, desc.get("Engine", "").lower(), desc.get("EngineVersion", ""),
                             desc.get("StorageType"))
    if not allowed:
        print("Can't retrieve classes"); sys.exit(2)
    preferred = [c for c in allowed if any(k in c for k in PREFERRED_INSTANCE_TYPES)]
    ordered = preferred + [c for c in allowed if c not in preferred]
    print("\nAvailable classes:")
    for i, cls in enumerate(ordered[:MAX_INSTANCE_DISPLAY], 1):
        print(f"  {i}) {cls}")
    print("  0) Manual entry")
    while True:
        sel = input("Pick: ").strip()
        if sel.isdigit():
            idx = int(sel)
            if idx == 0:
                manual = input("Enter class (e.g. db.t3.medium): ").strip()
                return manual if manual in allowed else print("Not allowed") or None
            if 1 <= idx <= len(ordered[:MAX_INSTANCE_DISPLAY]):
                return ordered[idx - 1]

# Rough estimate for how long BG will take
def estimate_eta(engine, storage_gb):
    eng = (engine or "").lower()
    if "aurora" in eng: 
        return ETA_ESTIMATES["aurora"]
    if not storage_gb: 
        return ETA_ESTIMATES["default"]
    if storage_gb <= STORAGE_THRESHOLDS["small"]: 
        return ETA_ESTIMATES["small"]
    if storage_gb <= STORAGE_THRESHOLDS["medium"]: 
        return ETA_ESTIMATES["medium"]
    return ETA_ESTIMATES["large"]

# Look for existing BG deployment for this database
def find_existing_bg_for_source(rds, identifier):
    _, desc = is_cluster(rds, identifier)
    source_arn = desc["DBInstanceArn"].lower()
    
    for page in rds.get_paginator("describe_blue_green_deployments").paginate():
        for d in page.get("BlueGreenDeployments", []):
            if d.get("Status") != "DELETED":
                src = d.get("Source", "")
                if isinstance(src, str) and src.lower() == source_arn:
                    return d
    return None

# Create new BG deployment or handle existing one
def create_bg(rds, identifier, target_class):
    _, desc = is_cluster(rds, identifier)
    name = f"bg-{identifier}-{now_utc().strftime('%Y%m%d-%H%M%S')}"
    arn = desc["DBInstanceArn"]
    print(f"\nCreating Blue/Green: {name}")
    try:
        resp = rds.create_blue_green_deployment(BlueGreenDeploymentName=name, Source=arn,
                                                TargetDBInstanceClass=target_class,
                                                Tags=[{"Key": "purpose", "Value": "resize"}])
        bg_id = resp["BlueGreenDeployment"]["BlueGreenDeploymentIdentifier"]
        print(f"Created: {bg_id}")
        return bg_id
    except ClientError as e:
        if e.response.get("Error", {}).get("Code") == "BlueGreenDeploymentAlreadyExistsFault":
            print(f"\nWARNING: A Blue/Green deployment already exists for: {identifier}")
            
            # Try to find the existing deployment
            existing = find_existing_bg_for_source(rds, identifier)
            if existing:
                bg_id = existing.get("BlueGreenDeploymentIdentifier", "")
                status = existing.get("Status", "")
                created = existing.get("CreateTime", "")
                print(f"\nExisting deployment:")
                print(f"  ID: {bg_id[:30]}...")
                print(f"  Status: {status}")
                print(f"  Created: {created}")
                
                print("\nWhat would you like to do?")
                print("  1) Continue with existing deployment")
                print("  2) Delete existing and create new")
                print("  0) Cancel")
                
                choice = input("Choice: ").strip()
                
                if choice == "1":
                    print(f"Using existing deployment: {bg_id[:30]}...")
                    return bg_id
                elif choice == "2":
                    print(f"\nDeleting existing deployment...")
                    try:
                        rds.delete_blue_green_deployment(BlueGreenDeploymentIdentifier=bg_id)
                        print("Waiting for deletion...")
                        while True:
                            try:
                                rds.describe_blue_green_deployments(BlueGreenDeploymentIdentifier=bg_id)
                                time.sleep(3)
                            except ClientError:
                                break
                        print("Deleted. Creating new deployment...")
                        return create_bg(rds, identifier, target_class)
                    except ClientError as del_err:
                        print(f"Failed to delete: {del_err}")
                        return None
                else:
                    print("Cancelled")
                    return None
            else:
                print("Could not find the existing deployment. Try 'Resume existing' from main menu.")
                return None
        raise

# Check BG deployment status
def bg_status(rds, bg_id):
    try:
        return rds.describe_blue_green_deployments(BlueGreenDeploymentIdentifier=bg_id)["BlueGreenDeployments"][0]
    except ClientError as e:
        return None if e.response.get("Error", {}).get("Code") == "BlueGreenDeploymentNotFoundFault" else (_ for _ in ()).throw(e)

# Wait until BG is ready to switch
def wait_ready(rds, bg_id, timeout_min=BG_TIMEOUT):
    print("\nWaiting for SWITCH_READY...")
    start, last = time.time(), ""
    while time.time() < start + timeout_min * 60:
        if not (s := bg_status(rds, bg_id)):
            print("Not found"); return False
        if (status := s.get("Status")) != last:
            print(f"  {status} ({int((time.time()-start)//60)}min)"); last = status
        if status in ("SWITCH_READY", "AVAILABLE"): return True
        if status in ("SWITCHOVER_COMPLETED", "SWITCHOVER_FAILED", "DELETED"):
            return status == "SWITCHOVER_COMPLETED"
        time.sleep(BG_POLL)
    print("Timeout"); return False

# Execute the BG switchover
def switch_over(rds, bg_id):
    print(f"\nSwitching: {bg_id}")
    rds.switchover_blue_green_deployment(BlueGreenDeploymentIdentifier=bg_id)
    while True:
        if not (s := bg_status(rds, bg_id)):
            print("Not found"); return
        st = s.get('Status')
        print(f"  {st}")
        if st == "SWITCHOVER_COMPLETED":
            print("Complete"); return
        if st in ("SWITCHOVER_FAILED", "DELETED"):
            raise RuntimeError(f"Failed: {st}")
        time.sleep(10)

# Check endpoint after switch
def verify_endpoint(rds, identifier):
    _, desc = is_cluster(rds, identifier)
    ep = desc.get("Endpoint") or {}
    print(f"\nVerified: {identifier} | {ep.get('Address')}:{ep.get('Port')}")

# Get all BG deployments in the region
def list_bgs(rds):
    items = []
    for page in rds.get_paginator("describe_blue_green_deployments").paginate():
        for d in page.get("BlueGreenDeployments", []):
            if (st := d.get("Status")) != "DELETED":
                items.append({"id": d.get("BlueGreenDeploymentIdentifier", ""), "status": st,
                            "created": d.get("CreateTime")})
    return items

# Let user select a BG deployment
def choose_bg(rds):
    items = list_bgs(rds)
    if not items:
        print("No Blue/Green deployments"); return None
    print("\nBlue/Green Deployments:")
    for i, item in enumerate(items, 1):
        print(f"  {i}) {item['id'][:20]}... | {item['status']} | {item.get('created', '')}")
    choice = input("Pick (0=cancel): ").strip()
    return items[int(choice) - 1]["id"] if choice.isdigit() and 1 <= int(choice) <= len(items) else None

# Delete BG deployment from AWS
def delete_bg(rds, bg_id):
    print(f"Deleting BG: {bg_id}")
    rds.delete_blue_green_deployment(BlueGreenDeploymentIdentifier=bg_id)
    while bg_status(rds, bg_id):
        time.sleep(5)
    print("Deleted")

# Find old instance for rollback
def find_old_resource(rds, base_id):
    for i in rds.describe_db_instances().get("DBInstances", []):
        if any(i["DBInstanceIdentifier"] == base_id + s for s in OLD_SUFFIXES):
            return {"id": i["DBInstanceIdentifier"], "class": i.get("DBInstanceClass")}
    return None

# Revert to previous instance class
def rollback(rds, cw, identifier):
    print("\n=== Rollback ===")
    base_id = identifier
    for suffix in OLD_SUFFIXES:
        if identifier.endswith(suffix):
            base_id = identifier[:-len(suffix)]
            break
    
    old = find_old_resource(rds, base_id)
    if not old or not old.get("class"):
        print(f"No old resource found for: {base_id}")
        print("   Rollback only works after switchover")
        return
    
    target_class = old["class"]
    print(f"Found: {old['id']} ({target_class})")
    if input(f"Rollback to {target_class}? (yes): ").strip().lower() != "yes":
        return
    
    print("Running pre-checks...")
    metrics = prechecks(rds, cw, base_id)
    if not print_checks(metrics):
        print("\nBLOCKED: Pre-checks failed. Cannot proceed with rollback.")
        print("   Current metrics show concerning CPU or memory values.")
        return
    
    print(f"Creating reverse Blue/Green to {target_class}...")
    bg_id = create_bg(rds, base_id, target_class)
    if not bg_id:
        return
    
    _, desc = is_cluster(rds, base_id)
    lo, hi = estimate_eta(desc.get("Engine"), desc.get("AllocatedStorage"))
    print(f"ETA: {lo}-{hi} min")
    
    if wait_ready(rds, bg_id):
        print("Switching back...")
        switch_over(rds, bg_id)
        verify_endpoint(rds, base_id)
        print_checks(prechecks(rds, cw, base_id), "Post-rollback")
        print(f"Rolled back to {target_class}")
    else:
        print("Not ready. Check AWS Console")

# Delete old instances with -old suffix
def delete_old(rds):
    items = []
    for i in rds.describe_db_instances().get("DBInstances", []):
        if any(i["DBInstanceIdentifier"].endswith(s) for s in OLD_SUFFIXES):
            status = i.get("DBInstanceStatus", "").lower()
            items.append({"id": i["DBInstanceIdentifier"], "status": status})
    
    if not items:
        print("No old instances found"); return
    
    print("\nOld instances:")
    for i, item in enumerate(items, 1):
        status_icon = "[DELETING]" if "delet" in item["status"] else ""
        print(f"  {i}) {item['id']} {status_icon} ({item['status']})")
    
    sel = input("Pick to delete (0=cancel): ").strip()
    if not sel.isdigit() or not (1 <= int(sel) <= len(items)):
        return
    
    selected = items[int(sel) - 1]
    rid = selected["id"]
    status = selected["status"]
    
    # Check if already being deleted
    if "delet" in status:
        print(f"WARNING: Instance is already being deleted (status: {status})")
        print("   No action needed - AWS is processing the deletion.")
        return
    
    # Check if in a state where deletion is not possible
    if status not in DELETABLE_INSTANCE_STATES:
        print(f"WARNING: Cannot delete instance in '{status}' state")
        print("   Wait for the instance to reach 'available' state or check AWS Console")
        return
    
    print(f"Deleting instance: {rid}")
    try:
        rds.delete_db_instance(DBInstanceIdentifier=rid, SkipFinalSnapshot=True, DeleteAutomatedBackups=True)
        print("Deletion initiated successfully")
    except ClientError as e:
        error_code = e.response.get("Error", {}).get("Code", "")
        if error_code == "InvalidDBInstanceState":
            print(f"WARNING: Instance is already being deleted or in invalid state")
            print("   Check AWS Console for current status")
        else:
            print(f"ERROR: {e}")
            raise

# Main entry point
def main():
    sess = get_session()
    print_banner(sess)
    rds, cw = sess.client("rds"), sess.client("cloudwatch")
    
    while True:
        print("\n" + "="*60)
        print("=== Main Menu ===".center(60))
        print("="*60)
        print("1) New resize       - Change instance class (upgrade/downgrade)")
        print("2) Resume existing  - Continue with existing Blue/Green deployment")
        print("3) Rollback         - Revert to previous instance class")
        print("4) Delete old       - Clean up old resources after resize")
        print("0) Quit")
        print("="*60)
        
        m = input("Choice: ").strip()
        if m == "0":
            print("Goodbye"); return
        
        if m == "2":
            if not (bg_id := choose_bg(rds)): continue
            # Try to extract identifier from BG deployment, or ask user
            bg = bg_status(rds, bg_id)
            identifier = None
            if bg and (src := bg.get("Source")):
                # Extract from ARN (format: arn:aws:rds:region:account:db:identifier)
                try:
                    identifier = src.split(":")[-1] if isinstance(src, str) else None
                except: pass
            if not identifier:
                print("Select the database for this Blue/Green:")
                identifier = choose_db(rds)["id"]
            
            print("\n--- Resume Blue-Green Deployment ---")
            print("1) Switch         - Execute switchover")
            print("2) Delete         - Remove deployment")
            print("3) View status    - Show deployment details")
            print("0) Back")
            if (c := input("Action: ").strip()) == "1":
                metrics = prechecks(rds, cw, identifier)
                if not print_checks(metrics):
                    print("\nBLOCKED: Pre-checks failed. Cannot proceed with switchover.")
                    print("   Current metrics show concerning CPU or memory values.")
                else:
                    switch_over(rds, bg_id)
                    verify_endpoint(rds, identifier)
                    print_checks(prechecks(rds, cw, identifier), "Post-switch")
            elif c == "2":
                delete_bg(rds, bg_id)
            elif c == "3":
                if s := bg_status(rds, bg_id): print(json.dumps(s, indent=2, default=str))
            continue
        
        if m == "3":
            print("\nSelect the database to rollback:")
            identifier = choose_db(rds)["id"]
            rollback(rds, cw, identifier)
            continue
        
        if m == "4":
            delete_old(rds)
            continue
        
        db = choose_db(rds)
        identifier = db["id"]
        
        while True:
            print("\n" + "-"*60)
            print(f"Database: {identifier}")
            print("-"*60)
            print("1) Create Blue-Green  - Start resize with pre-checks & snapshot")
            print("2) Switch             - Execute switchover to new instance class")
            print("3) Rollback           - Revert to previous instance class")
            print("4) Cleanup            - Delete deployments or old resources")
            print("5) Advanced           - Manual operations (checks, snapshots, status)")
            print("0) Back")
            print("-"*60)
            action = input("Action: ").strip()
            if action == "0": break
            
            if action == "1":
                target = pick_target_class(rds, identifier)
                _, desc = is_cluster(rds, identifier)
                current = desc.get("DBInstanceClass")
                
                metrics = prechecks(rds, cw, identifier)
                if not print_checks(metrics):
                    print("\nBLOCKED: Pre-checks failed. Cannot proceed with Blue/Green deployment.")
                    print("   Current metrics show concerning CPU or memory values.")
                    print("   Wait for metrics to stabilize before attempting resize.")
                    input("Press Enter to return...")
                    continue
                
                if current:
                    if not check_suitability(identifier, current, target, metrics):
                        print("\nBLOCKED: Cannot proceed - target instance is insufficient for current workload.")
                        input("Press Enter to choose a different instance class...")
                        continue
                
                create_snapshot(rds, identifier)
                bg_id = create_bg(rds, identifier, target)
                if not bg_id:
                    input("\nPress Enter...")
                    continue
                lo, hi = estimate_eta(desc.get("Engine"), desc.get("AllocatedStorage"))
                print(f"ETA: {lo}-{hi} min. Press Ctrl+C to stop watching.")
                if wait_ready(rds, bg_id):
                    print("Ready to switch!")
                else:
                    print("Check AWS Console")
                input("\nPress Enter...")
            
            elif action == "2":
                if not (bg_id := choose_bg(rds)): continue
                if not bg_status(rds, bg_id):
                    print("Not found"); continue
                metrics = prechecks(rds, cw, identifier)
                if not print_checks(metrics):
                    print("\nBLOCKED: Pre-checks failed. Cannot proceed with switchover.")
                    print("   Current metrics show concerning CPU or memory values.")
                    input("\nPress Enter...")
                    continue
                if input("Switch? (yes): ").strip().lower() == "yes":
                    switch_over(rds, bg_id)
                    verify_endpoint(rds, identifier)
                    print_checks(prechecks(rds, cw, identifier), "Post-switch")
                input("\nPress Enter...")
            
            elif action == "3":
                rollback(rds, cw, identifier)
                input("\nPress Enter...")
            
            elif action == "4":
                print("\n--- Cleanup ---")
                print("1) Delete Blue-Green deployment")
                print("2) Delete old instance (with -old suffix)")
                print("0) Cancel")
                if (c := input("Action: ").strip()) == "1":
                    if bg_id := choose_bg(rds): delete_bg(rds, bg_id)
                elif c == "2":
                    delete_old(rds)
                input("\nPress Enter...")
            
            elif action == "5":
                print("\n--- Advanced Operations ---")
                print("1) Run pre-checks         - CloudWatch metrics analysis")
                print("2) Check feasibility      - Test if resize would be allowed (no deployment)")
                print("3) Create snapshot        - Manual backup")
                print("4) View deployment status - Check Blue-Green details")
                print("0) Cancel")
                if (c := input("Action: ").strip()) == "1":
                    print_checks(prechecks(rds, cw, identifier))
                elif c == "2":
                    # Feasibility check without creating deployment
                    _, desc = is_cluster(rds, identifier)
                    current = desc.get("DBInstanceClass")
                    print(f"\nCurrent instance: {identifier} ({current})")
                    
                    target = pick_target_class(rds, identifier)
                    if not target:
                        continue
                    
                    print("\n" + "="*60)
                    print(f"Feasibility Check: {current} -> {target}")
                    print("="*60)
                    
                    metrics = prechecks(rds, cw, identifier)
                    precheck_pass = print_checks(metrics)
                    
                    if not precheck_pass:
                        print("\n❌ RESULT: Would be BLOCKED")
                        print("   Reason: Current metrics show concerning CPU or memory values")
                        print("   Action: Wait for metrics to stabilize before attempting resize")
                    elif current:
                        suitability_pass = check_suitability(identifier, current, target, metrics)
                        if not suitability_pass:
                            print("\n❌ RESULT: Would be BLOCKED")
                            print("   Reason: Target instance insufficient for current workload")
                            print("   Action: Choose a larger target instance class")
                        else:
                            print("\n✅ RESULT: Resize would be ALLOWED")
                            print("   Both pre-checks and suitability analysis passed")
                            print("   You can proceed with creating a Blue/Green deployment")
                    else:
                        print("\n✅ RESULT: Pre-checks passed")
                        print("   Note: Could not verify suitability (instance class specs unknown)")
                elif c == "3":
                    create_snapshot(rds, identifier)
                elif c == "4":
                    if bg_id := choose_bg(rds):
                        if s := bg_status(rds, bg_id): print(json.dumps(s, indent=2, default=str))
                input("\nPress Enter...")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nAborted")
