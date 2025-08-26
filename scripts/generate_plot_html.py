#!/usr/bin/env python3
"""
Generate an HTML plot (Plotly) from the metrics CSV without needing matplotlib.
Writes `.data/batch_plot.html` by default.
Usage:
    python3 scripts/generate_plot_html.py <csv> [out_html]
"""
import csv
import sys
from collections import defaultdict

if len(sys.argv) < 2:
    print('Usage: generate_plot_html.py <csv_path> [out_html]')
    sys.exit(1)

csv_path = sys.argv[1]
out_html = sys.argv[2] if len(sys.argv) > 2 else '.data/batch_sweep_plot.html'

# Read CSV
rows = []
with open(csv_path, newline='') as f:
    r = csv.DictReader(f)
    for row in r:
        cap = int(row.get('cap') or 0)
        timeout = int(row.get('timeout_ms') or 0)
        mode = row.get('mode') or 'unknown'
        try:
            avg = float(row.get('avg_batch_ms') or 0.0)
        except:
            avg = 0.0
        rows.append({'cap': cap, 'timeout': timeout, 'mode': mode, 'avg': avg})

# Organize series by (mode, timeout)
series = defaultdict(dict)
caps = sorted({r['cap'] for r in rows})
for r in rows:
    key = (r['mode'], r['timeout'])
    series[key][r['cap']] = r['avg']

# Build JS data
traces = []
for (mode, timeout), data in sorted(series.items()):
    y = [data.get(cap, None) for cap in caps]
    traces.append({
        'name': f"{mode} ({timeout}ms)",
        'x': caps,
        'y': y,
        'type': 'scatter',
        'mode': 'lines+markers'
    })

html = f"""
<!doctype html>
<html>
<head>
  <meta charset="utf-8" />
  <script src="https://cdn.plot.ly/plotly-latest.min.js"></script>
  <title>hyperzoekt batch sweep</title>
</head>
<body>
  <h2>hyperzoekt batch sweep: avg batch latency</h2>
  <div id="plot" style="width:900px;height:500px;"></div>
  <script>
    const data = {traces};
    const layout = {{
      xaxis: {{title: 'batch_capacity'}},
      yaxis: {{title: 'avg_batch_ms'}},
      legend: {{orientation: 'h'}},
      margin: {{t:40}}
    }};
    Plotly.newPlot('plot', data, layout);
  </script>
</body>
</html>
"""

# Ensure output dir exists
import os
os.makedirs(os.path.dirname(out_html), exist_ok=True)
with open(out_html, 'w') as f:
    f.write(html)
print(out_html)
