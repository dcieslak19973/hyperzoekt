#!/usr/bin/env python3
"""
Simple metrics plotting for hyperzoekt batch sweep results.
Usage:
    python3 scripts/plot_metrics.py <metrics_csv> [out_png]

Reads the CSV produced by scripts/metrics_summarize.py and writes a PNG.
"""
import csv
import sys
from collections import defaultdict

try:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
except Exception as e:
    print("matplotlib not available:", e, file=sys.stderr)
    sys.exit(2)


def read_metrics(path):
    rows = []
    with open(path, newline='') as f:
        r = csv.DictReader(f)
        for row in r:
            # normalize types
            try:
                cap = int(row.get('cap') or 0)
            except:
                cap = 0
            try:
                timeout = int(row.get('timeout_ms') or 0)
            except:
                timeout = 0
            mode = row.get('mode') or 'unknown'
            try:
                avg = float(row.get('avg_batch_ms') or 0.0)
            except:
                avg = 0.0
            rows.append({'cap': cap, 'timeout': timeout, 'mode': mode, 'avg': avg})
    return rows


def plot(rows, out_path):
    # group by (mode, timeout)
    series = defaultdict(dict)
    caps = sorted({r['cap'] for r in rows})
    for r in rows:
        key = (r['mode'], r['timeout'])
        series[key][r['cap']] = r['avg']

    plt.figure(figsize=(8,5))
    markers = ['o','s','^','D','x','P']
    for i, ((mode, timeout), data) in enumerate(sorted(series.items())):
        y = [data.get(cap, 0.0) for cap in caps]
        label = f"{mode} ({timeout}ms)"
        plt.plot(caps, y, marker=markers[i % len(markers)], label=label)

    plt.xlabel('batch_capacity')
    plt.ylabel('avg_batch_ms')
    plt.title('hyperzoekt batch sweep: avg batch latency')
    plt.xticks(caps)
    plt.grid(True, linestyle='--', alpha=0.4)
    plt.legend()
    plt.tight_layout()
    plt.savefig(out_path)
    print(out_path)


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print('Usage: plot_metrics.py <metrics_csv> [out_png]', file=sys.stderr)
        sys.exit(1)
    csv_path = sys.argv[1]
    out_png = sys.argv[2] if len(sys.argv) > 2 else '.data/batch_plot.png'
    rows = read_metrics(csv_path)
    plot(rows, out_png)
