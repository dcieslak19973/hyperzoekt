#!/usr/bin/env python3
"""Summarize SurrealDB metrics JSON files in .data/ into a CSV for analysis.

Usage: ./scripts/metrics_summarize.py [.data]
"""
import sys
import json
from pathlib import Path
import csv

def main():
    data_dir = Path(sys.argv[1]) if len(sys.argv) > 1 else Path('.data')
    out = []
    for p in sorted(data_dir.glob('db_metrics_*.json')):
        name = p.stem
        parts = name.split('_')
        # expected: db_metrics_<cap>_<timeout>_<mode>
        if len(parts) < 4:
            continue
        cap = parts[2]
        to = parts[3]
        mode = parts[4] if len(parts) > 4 else 'unknown'
        # ensure cap and timeout are numeric; skip otherwise (robustness)
        try:
            int(cap)
            int(to)
        except Exception:
            # skip non-standard metric files (for example test artifacts)
            continue
        try:
            j = json.loads(p.read_text())
        except Exception as e:
            print(f"Failed to parse {p}: {e}")
            continue
        out.append({
            'file': p.name,
            'cap': cap,
            'timeout_ms': to,
            'mode': mode,
            'batches_sent': j.get('batches_sent'),
            'entities_sent': j.get('entities_sent'),
            'avg_batch_ms': j.get('avg_batch_ms'),
            'min_batch_ms': j.get('min_batch_ms'),
            'max_batch_ms': j.get('max_batch_ms'),
            # total_time_ms: prefer explicit field, otherwise estimate
            'total_time_ms': j.get('total_time_ms') if j.get('total_time_ms') is not None else (
                (j.get('avg_batch_ms') * j.get('batches_sent')) if (j.get('avg_batch_ms') is not None and j.get('batches_sent') is not None) else None
            ),
            'batch_failures': j.get('batch_failures'),
            'attempt_counts': json.dumps(j.get('attempt_counts', {})),
        })
    if not out:
        print('No metrics files found.')
        return
    w = csv.DictWriter(sys.stdout, fieldnames=list(out[0].keys()))
    w.writeheader()
    for r in out:
        w.writerow(r)

if __name__ == '__main__':
    main()
