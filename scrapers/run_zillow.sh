#!/bin/bash
cd /Users/marcmunoz/smart_fetch_orchestrator/scrapers
source /Users/marcmunoz/.openclaw/workspace/zillow_monitor/venv/bin/activate
python3 zillow_for_sale.py --min-coc 20.0
