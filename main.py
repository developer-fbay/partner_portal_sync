#!/usr/bin/env python3
"""
Close CRM to Supabase Sync Worker

Usage:
    python main.py                  # Run incremental sync (default)
    python main.py --mode full      # Run full re-sync
    python main.py --mode incremental
"""

import argparse
import logging
import sys
from datetime import datetime

from sync import run_sync

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

logger = logging.getLogger(__name__)


def main():
    parser = argparse.ArgumentParser(
        description="Sync Close CRM data to Supabase",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    # Incremental sync - all phases (default)
    python main.py
    
    # Full re-sync - all phases
    python main.py --mode full
    
    # Run one phase only
    python main.py --phase partners        # Sync partner status from Close
    python main.py --phase leads           # Sync leads (smart-view search fields only)
    python main.py --phase lead_details    # Full enrichment for leads already in DB
    python main.py --phase lead_magnets    # Sync LeadMaggy activities
    python main.py --phase activities      # Sync referrals + partner uploads
    python main.py --phase dealsheet       # Sync Google Sheet dealsheet data
    
Cron examples:
    # Incremental every 30 minutes
    */30 * * * * cd /app && python main.py >> /var/log/close-sync.log 2>&1
    
    # Full sync daily at 6 AM
    0 6 * * * cd /app && python main.py --mode full >> /var/log/close-sync.log 2>&1
    
    # Partners sync weekly (activate/deactivate based on Close status)
    0 7 * * 1 cd /app && python main.py --phase partners >> /var/log/close-sync.log 2>&1
        """,
    )
    
    parser.add_argument(
        "--mode",
        choices=["incremental", "full"],
        default="incremental",
        help="Sync mode: incremental (default) or full",
    )
    
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Enable debug logging",
    )
    
    parser.add_argument(
        "--max-leads",
        type=int,
        default=None,
        help="Limit number of leads to process (for testing)",
    )
    
    parser.add_argument(
        "--phase",
        choices=["all", "partners", "leads", "lead_details", "lead_magnets", "activities", "dealsheet"],
        default="all",
        help="Sync phase: all (default), partners, leads, lead_details, lead_magnets, activities, or dealsheet",
    )
    
    parser.add_argument(
        "--leads-only",
        action="store_true",
        help="Alias for --phase leads",
    )
    
    parser.add_argument(
        "--skip-lock",
        action="store_true",
        help="Skip advisory lock check (for testing only - risk of duplicate runs)",
    )
    
    args = parser.parse_args()
    
    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)
    
    logger.info(f"=== Close Sync Worker starting at {datetime.utcnow().isoformat()} ===")
    phase = "leads" if args.leads_only else args.phase
    
    logger.info(f"Mode: {args.mode}")
    logger.info(f"Phase: {phase}")
    if args.max_leads:
        logger.info(f"Max leads: {args.max_leads} (testing mode)")
    
    try:
        run_sync(
            mode=args.mode,
            max_leads=args.max_leads,
            phase=phase,
            skip_lock=args.skip_lock,
        )
        logger.info("=== Sync completed successfully ===")
        sys.exit(0)
        
    except KeyboardInterrupt:
        logger.info("Sync interrupted by user")
        sys.exit(130)
        
    except Exception as e:
        logger.exception(f"Sync failed with error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
