#!/usr/bin/env python3
"""RDS Blue/Green Instance Resize - Automates RDS/Aurora instance class changes via Blue/Green deployments."""
import os, pathlib, subprocess, sys, venv

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
                    BG_SWITCH_POLL_INTERVAL as BG_POLL, BG_TIMEOUT_MINUTES as BG_TIMEOUT, INSTANCE_SPECS)

def now_utc():
    return dt.datetime.now(getattr(dt, "UTC", dt.timezone.utc))

def time_range(minutes=15):
    end = now_utc()
    return end - dt.timedelta(minutes=minutes), end

def get_session():
    try:
        return boto3.Session()
    except (ProfileNotFound, NoRegionError) as e:
        print(f"Error: {e}"); sys.exit(2)

def print_banner(sess):
    print(f"=== RDS Blue/Green Resize ===\nProfile: {os.environ.get('AWS_PROFILE', 'default')}")
    if not sess.region_name:
        print("No region set. Export AWS_REGION=eu-west-1"); sys.exit(2)
    print(f"Region: {sess.region_name}\n")

def select_from_list(items, prompt="Choose"):
    while True:
        choice = input(f"{prompt}: ").strip()
        if choice.isdigit() and 1 <= int(choice) <= len(items):
            return items[int(choice) - 1]
        print("Invalid")

def list_dbs(rds):
    items = []
    try:
        for page in rds.get_paginator("describe_db_clusters").paginate():
            for c in page.get("DBClusters", []):
                writer = next((m.get("DBInstanceIdentifier") for m in c.get("DBClusterMembers", []) if m.get("IsClusterWriter")), None)
                cls = None
                if writer:
                    try:
                        cls = rds.describe_db_instances(DBInstanceIdentifier=writer)["DBInstances"][0].get("DBInstanceClass")
                    except: pass
                items.append({"type": "cluster", "id": c["DBClusterIdentifier"], "engine": c.get("Engine"),
                             "version": c.get("EngineVersion"), "class": cls, "storage_gb": None})
    except: pass
    try:
        for page in rds.get_paginator("describe_db_instances").paginate():
            for i in page.get("DBInstances", []):
                items.append({"type": "instance", "id": i["DBInstanceIdentifier"], "engine": i.get("Engine"),
                             "version": i.get("EngineVersion"), "class": i.get("DBInstanceClass"),
                             "storage_gb": i.get("AllocatedStorage"), "storage_type": i.get("StorageType")})
    except: pass
    return items

def choose_db(rds):
    items = list_dbs(rds)
    if not items:
        print("No databases found"); sys.exit(2)
    print("\nDatabases:")
    for i, item in enumerate(items, 1):
        storage = f", {item.get('storage_gb')}GB" if item.get('storage_gb') else ""
        print(f"  {i}) [{item['type']}] {item['id']} | {item.get('engine')} | {item.get('class')}{storage}")
    return select_from_list(items)

def is_cluster(rds, identifier):
    try:
        return True, rds.describe_db_clusters(DBClusterIdentifier=identifier)["DBClusters"][0]
    except ClientError:
        pass
    try:
        return False, rds.describe_db_instances(DBInstanceIdentifier=identifier)["DBInstances"][0]
    except ClientError:
        pass
    print(f"Not found: {identifier}"); sys.exit(2)

def get_metric(cw, metric, inst_id, minutes=15):
    start, end = time_range(minutes)
    resp = cw.get_metric_statistics(Namespace="AWS/RDS", MetricName=metric,
                                    Dimensions=[{"Name": "DBInstanceIdentifier", "Value": inst_id}],
                                    StartTime=start, EndTime=end, Period=300, Statistics=["Average"])
    dps = sorted(resp.get("Datapoints", []), key=lambda x: x["Timestamp"])
    return float(dps[-1]["Average"]) if dps else None

def prechecks(rds, cw, identifier):
    is_clu, desc = is_cluster(rds, identifier)
    if is_clu:
        writer = next((m for m in desc.get("DBClusterMembers", []) if m.get("IsClusterWriter")), None)
        inst_id = writer["DBInstanceIdentifier"] if writer else None
        if not inst_id:
            raise RuntimeError("No writer found")
    else:
        inst_id = desc["DBInstanceIdentifier"]
    return {
        "CPUUtilization": get_metric(cw, "CPUUtilization", inst_id),
        "FreeableMemory": get_metric(cw, "FreeableMemory", inst_id),
        "ReadIOPS": get_metric(cw, "ReadIOPS", inst_id),
        "WriteIOPS": get_metric(cw, "WriteIOPS", inst_id),
        "Connections": get_metric(cw, "DatabaseConnections", inst_id),
    }

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

def wait_snapshot(rds, is_clu, snap_id):
    print("Waiting for snapshot...")
    func = rds.describe_db_cluster_snapshots if is_clu else rds.describe_db_snapshots
    param = "DBClusterSnapshotIdentifier" if is_clu else "DBSnapshotIdentifier"
    key = "DBClusterSnapshots" if is_clu else "DBSnapshots"
    last_pct = -1
    while True:
        try:
            snap = func(**{param: snap_id})[key][0]
            status, pct = snap.get("Status"), snap.get("PercentProgress", 0)
            if pct != last_pct:
                print(f"  {pct}% - {status}"); last_pct = pct
            if status == "available":
                print("Snapshot ready"); return
        except: pass
        time.sleep(POLL_INTERVAL)

def create_snapshot(rds, identifier):
    is_clu, _ = is_cluster(rds, identifier)
    snap_id = f"{identifier}-pre-resize-{now_utc().strftime('%Y%m%d-%H%M%S')}"
    print(f"\nCreating snapshot: {snap_id}")
    if is_clu:
        rds.create_db_cluster_snapshot(DBClusterSnapshotIdentifier=snap_id, DBClusterIdentifier=identifier,
                                       Tags=[{"Key": "purpose", "Value": "pre-resize"}])
    else:
        rds.create_db_snapshot(DBSnapshotIdentifier=snap_id, DBInstanceIdentifier=identifier,
                              Tags=[{"Key": "purpose", "Value": "pre-resize"}])
    wait_snapshot(rds, is_clu, snap_id)
    return snap_id

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
        print(f"CRITICAL: CPU {proj_cpu:.0f}% or Memory {proj_mem:.1f}GiB"); return False
    print("Suitable"); return True

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

def pick_target_class(rds, identifier):
    is_clu, desc = is_cluster(rds, identifier)
    allowed = list_orderable(rds, desc.get("Engine", "").lower(), desc.get("EngineVersion", ""),
                             None if is_clu else desc.get("StorageType"))
    if not allowed:
        print("Can't retrieve classes"); sys.exit(2)
    preferred = [c for c in allowed if any(k in c for k in ("t4g", "t3", "m6g", "m5"))]
    ordered = preferred + [c for c in allowed if c not in preferred]
    print("\nAvailable classes:")
    for i, cls in enumerate(ordered[:50], 1):
        print(f"  {i}) {cls}")
    print("  0) Manual entry")
    while True:
        sel = input("Pick: ").strip()
        if sel.isdigit():
            idx = int(sel)
            if idx == 0:
                manual = input("Enter class (e.g. db.t3.medium): ").strip()
                return manual if manual in allowed else print("Not allowed") or None
            if 1 <= idx <= len(ordered[:50]):
                return ordered[idx - 1]

def estimate_eta(engine, storage_gb):
    eng = (engine or "").lower()
    if "aurora" in eng: return (5, 25)
    if not storage_gb: return (10, 45)
    if storage_gb <= 100: return (10, 25)
    if storage_gb <= 500: return (20, 60)
    return (30, 120)

def find_existing_bg_for_source(rds, identifier):
    """Find existing Blue/Green deployment for a source database."""
    is_clu, desc = is_cluster(rds, identifier)
    source_arn = (desc["DBClusterArn"] if is_clu else desc["DBInstanceArn"]).lower()
    
    for page in rds.get_paginator("describe_blue_green_deployments").paginate():
        for d in page.get("BlueGreenDeployments", []):
            if d.get("Status") != "DELETED":
                src = d.get("Source", "")
                if isinstance(src, str) and src.lower() == source_arn:
                    return d
    return None

def create_bg(rds, identifier, target_class):
    is_clu, desc = is_cluster(rds, identifier)
    name = f"bg-{identifier}-{now_utc().strftime('%Y%m%d-%H%M%S')}"
    arn = desc["DBClusterArn"] if is_clu else desc["DBInstanceArn"]
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

def bg_status(rds, bg_id):
    try:
        return rds.describe_blue_green_deployments(BlueGreenDeploymentIdentifier=bg_id)["BlueGreenDeployments"][0]
    except ClientError as e:
        return None if e.response.get("Error", {}).get("Code") == "BlueGreenDeploymentNotFoundFault" else (_ for _ in ()).throw(e)

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

def verify_endpoint(rds, identifier):
    is_clu, desc = is_cluster(rds, identifier)
    ep = desc.get("Endpoint") or {}
    print(f"\nVerified: {identifier} | {ep.get('Address')}:{ep.get('Port')}")

def list_bgs(rds):
    items = []
    for page in rds.get_paginator("describe_blue_green_deployments").paginate():
        for d in page.get("BlueGreenDeployments", []):
            if (st := d.get("Status")) != "DELETED":
                items.append({"id": d.get("BlueGreenDeploymentIdentifier", ""), "status": st,
                            "created": d.get("CreateTime")})
    return items

def choose_bg(rds):
    items = list_bgs(rds)
    if not items:
        print("No Blue/Green deployments"); return None
    print("\nBlue/Green Deployments:")
    for i, item in enumerate(items, 1):
        print(f"  {i}) {item['id'][:20]}... | {item['status']} | {item.get('created', '')}")
    choice = input("Pick (0=cancel): ").strip()
    return items[int(choice) - 1]["id"] if choice.isdigit() and 1 <= int(choice) <= len(items) else None

def delete_bg(rds, bg_id):
    print(f"Deleting BG: {bg_id}")
    rds.delete_blue_green_deployment(BlueGreenDeploymentIdentifier=bg_id)
    while bg_status(rds, bg_id):
        time.sleep(5)
    print("Deleted")

def find_old_resource(rds, base_id):
    """Find old resource with -old suffix."""
    for i in rds.describe_db_instances().get("DBInstances", []):
        if any(i["DBInstanceIdentifier"] == base_id + s for s in OLD_SUFFIXES):
            return {"type": "instance", "id": i["DBInstanceIdentifier"], "class": i.get("DBInstanceClass")}
    for c in rds.describe_db_clusters().get("DBClusters", []):
        if any(c["DBClusterIdentifier"] == base_id + s for s in OLD_SUFFIXES):
            writer = next((m.get("DBInstanceIdentifier") for m in c.get("DBClusterMembers", []) if m.get("IsClusterWriter")), None)
            cls = None
            if writer:
                try:
                    cls = rds.describe_db_instances(DBInstanceIdentifier=writer)["DBInstances"][0].get("DBInstanceClass")
                except: pass
            return {"type": "cluster", "id": c["DBClusterIdentifier"], "class": cls}
    return None

def rollback(rds, cw, identifier):
    """Rollback to previous instance class via reverse Blue/Green."""
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
    print_checks(metrics)
    
    print(f"Creating reverse Blue/Green to {target_class}...")
    bg_id = create_bg(rds, base_id, target_class)
    if not bg_id:
        return
    
    is_clu, desc = is_cluster(rds, base_id)
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

def delete_old(rds):
    items = []
    for i in rds.describe_db_instances().get("DBInstances", []):
        if any(i["DBInstanceIdentifier"].endswith(s) for s in OLD_SUFFIXES):
            items.append(("instance", i["DBInstanceIdentifier"]))
    for c in rds.describe_db_clusters().get("DBClusters", []):
        if any(c["DBClusterIdentifier"].endswith(s) for s in OLD_SUFFIXES):
            items.append(("cluster", c["DBClusterIdentifier"]))
    if not items:
        print("No old resources found"); return
    print("\nOld resources:")
    for i, (typ, rid) in enumerate(items, 1):
        print(f"  {i}) [{typ}] {rid}")
    sel = input("Pick to delete (0=cancel): ").strip()
    if not sel.isdigit() or not (1 <= int(sel) <= len(items)):
        return
    typ, rid = items[int(sel) - 1]
    print(f"Deleting {typ}: {rid}")
    if typ == "instance":
        rds.delete_db_instance(DBInstanceIdentifier=rid, SkipFinalSnapshot=True, DeleteAutomatedBackups=True)
    else:
        for m in rds.describe_db_clusters(DBClusterIdentifier=rid)["DBClusters"][0].get("DBClusterMembers", []):
            try:
                rds.delete_db_instance(DBInstanceIdentifier=m["DBInstanceIdentifier"], SkipFinalSnapshot=True)
            except: pass
        rds.delete_db_cluster(DBClusterIdentifier=rid, SkipFinalSnapshot=True)
    print("Deleted")

def main():
    sess = get_session()
    print_banner(sess)
    rds, cw = sess.client("rds"), sess.client("cloudwatch")
    
    while True:
        print("\n=== Main Menu ===\n1) New resize\n2) Resume existing\n0) Quit")
        if (m := input("Choice: ").strip()) == "0":
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
            
            print("\n1) Switch\n2) Delete BG\n3) Status\n0) Back")
            if (c := input("Action: ").strip()) == "1":
                switch_over(rds, bg_id)
                verify_endpoint(rds, identifier)
                print_checks(prechecks(rds, cw, identifier), "Post-switch")
            elif c == "2":
                delete_bg(rds, bg_id)
            elif c == "3":
                if s := bg_status(rds, bg_id): print(json.dumps(s, indent=2, default=str))
            continue
        
        db = choose_db(rds)
        identifier = db["id"]
        
        while True:
            print("\n1) Create BG\n2) Switch\n3) Rollback\n4) Cleanup\n5) Advanced\n0) Back")
            action = input("Action: ").strip()
            if action == "0": break
            
            if action == "1":
                target = pick_target_class(rds, identifier)
                is_clu, desc = is_cluster(rds, identifier)
                current = None
                if is_clu:
                    writer = next((m for m in desc.get("DBClusterMembers", []) if m.get("IsClusterWriter")), None)
                    if writer:
                        try:
                            current = rds.describe_db_instances(DBInstanceIdentifier=writer["DBInstanceIdentifier"])["DBInstances"][0].get("DBInstanceClass")
                        except: pass
                else:
                    current = desc.get("DBInstanceClass")
                
                metrics = prechecks(rds, cw, identifier)
                if not print_checks(metrics):
                    if input("Continue anyway? (yes): ").strip().lower() != "yes": continue
                
                if current and not check_suitability(identifier, current, target, metrics):
                    if input("Proceed? (yes): ").strip().lower() != "yes": continue
                
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
                print_checks(metrics)
                if input("Switch? (yes): ").strip().lower() == "yes":
                    switch_over(rds, bg_id)
                    verify_endpoint(rds, identifier)
                    print_checks(prechecks(rds, cw, identifier), "Post-switch")
                input("\nPress Enter...")
            
            elif action == "3":
                rollback(rds, cw, identifier)
                input("\nPress Enter...")
            
            elif action == "4":
                print("\n1) Delete BG\n2) Delete old resource")
                if (c := input("Action: ").strip()) == "1":
                    if bg_id := choose_bg(rds): delete_bg(rds, bg_id)
                elif c == "2":
                    delete_old(rds)
                input("\nPress Enter...")
            
            elif action == "5":
                print("\n1) Precheck\n2) Snapshot\n3) BG Status")
                if (c := input("Action: ").strip()) == "1":
                    print_checks(prechecks(rds, cw, identifier))
                elif c == "2":
                    create_snapshot(rds, identifier)
                elif c == "3":
                    if bg_id := choose_bg(rds):
                        if s := bg_status(rds, bg_id): print(json.dumps(s, indent=2, default=str))
                input("\nPress Enter...")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nAborted")
