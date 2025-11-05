# RDS Blue/Green Instance Resize

Automates RDS/Aurora instance class changes (upgrades & downgrades) using AWS Blue/Green deployments with minimal downtime.

## Quick Start

```bash
assume <aws_profile> 
python rds_bg_resize.py
```

The script will automatically install `boto3` if needed.

## Features

- **Minimal downtime** - Seconds during switchover (vs 5-15 min for direct modify)
- **Both directions** - Upgrade or downgrade instance class
- **Pre-flight checks** - CloudWatch metrics analysis
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

## Files

- `rds_bg_resize.py` - Main script (433 lines)
- `config.py` - Configuration (instance specs, thresholds, timeouts)