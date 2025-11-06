# RDS Blue/Green Instance Resize

Automates RDS instance class changes (upgrades & downgrades) using AWS Blue/Green deployments with minimal downtime.

## Quick Start

```bash
assume <aws_profile> 
python rds_bg_resize.py
```

The script will automatically install `boto3` if needed.

## Features

- **Minimal downtime** - Seconds during switchover (vs 5-15 min for direct modify)
- **Both directions** - Upgrade or downgrade instance class
- **Pre-flight checks** - CloudWatch metrics analysis (CPU, memory, IOPS, connections)
- **AWS safety guardrails** - Blocks unsafe resizes that don't meet AWS recommendations
- **Automatic snapshots** - Backup before changes
- **Easy rollback** - Revert to previous instance class if needed

## How It Works

1. Select your database
2. Choose target instance class
3. Script runs pre-checks and creates snapshot
4. Blue/Green deployment provisions new instance
5. Switch over with ~30-60 seconds downtime
6. Verify with post-checks


## Why Blue/Green?

- **Minimal downtime**: Seconds vs 5-15 minutes for direct modify
- **Safety**: Test green environment before switching
- **Rollback**: Easy revert if issues occur
- **Zero data loss**: Continuous replication until switch

## Safety Thresholds (AWS Best Practices)

The script enforces AWS recommended thresholds to prevent unsafe resizes:

**Critical (blocks resize):**
- CPU utilization would exceed **80%**
- Free memory would drop below **1 GiB**

**Warning (allows override):**
- CPU utilization would exceed **60%**
- Free memory would drop below **2 GiB**

If a resize fails suitability checks, the script blocks proceeding and recommends choosing a larger instance class.

## Files

- `rds_bg_resize.py` - Main script (RDS instances only)
- `config.py` - Configuration (instance specs, thresholds, timeouts)