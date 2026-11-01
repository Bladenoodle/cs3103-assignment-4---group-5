#!/usr/bin/env python3
"""Summarize run_demo.py logs: one row per valid run plus the median.

usage: summarize.py <section-name> log1 [log2 ...]
A log with "load_clients.py failed" and no result block counts as a failed
attempt (harness crash) and is reported, not averaged.
"""
import re
import statistics
import sys

FIELDS = [
    ("P50",      r"P50 caption age:\s+([\d.]+)"),
    ("P95",      r"P95 caption age:\s+([\d.]+)"),
    ("P99",      r"P99 caption age:\s+([\d.]+)"),
    ("Fresh%",   r"Fresh Caption Ratio[^:]*:\s+([\d.]+)"),
    ("FFinal%",  r"Fresh Final Caption Ratio[^:]*:\s+([\d.]+)"),
    ("Compl%",   r"Final-caption completeness:\s+([\d.]+)"),
    ("Cov%",     r"Normal Update Coverage:\s+([\d.]+)"),
    ("Dup",      r"Duplicate messages observed:\s+(\d+)"),
    ("Stale",    r"Stale messages observed:\s+(\d+)"),
    ("SlowP95",  r"Slow clients:.*?P95=([\d.]+)"),
    ("NormP95",  r"Normal clients:.*?P95=([\d.]+)"),
    ("RecP95",   r"Recovery P95:\s+([\d.]+)"),
]


def parse(text):
    m = re.search(r"LiveRecall Results\s*\n=+", text)
    if not m:
        return None
    block = "\n".join(text[m.start():].splitlines()[:45])
    row = {}
    for name, rx in FIELDS:
        f = re.search(rx, block)
        if f:
            row[name] = float(f.group(1))
    return row


def main():
    section, paths = sys.argv[1], sys.argv[2:]
    rows, failed, order = [], 0, []
    for p in sorted(paths):
        text = open(p, errors="replace").read()
        row = parse(text)
        if row is None:
            if "load_clients.py failed" in text:
                failed += 1
            continue
        rows.append(row)
        order.append(p.rsplit("/", 1)[-1])
    cols = [n for n, _ in FIELDS if any(n in r for r in rows)]
    print(f"== {section}: {len(rows)} valid run(s), {failed} failed attempt(s) (harness crash, rerun)")
    if not rows:
        print("   NO VALID RUN")
        print()
        return
    print("   units: P50/P95/P99/SlowP95/NormP95/RecP95 in ms, % columns in percent")
    print("   " + "run".ljust(24) + "".join(c.rjust(9) for c in cols))
    for name, r in zip(order, rows):
        print("   " + name[:23].ljust(24) + "".join(
            (f"{r[c]:.2f}" if c in r else "-").rjust(9) for c in cols))
    print("   " + "MEDIAN".ljust(24) + "".join(
        (f"{statistics.median([r[c] for r in rows if c in r]):.2f}"
         if any(c in r for r in rows) else "-").rjust(9) for c in cols))
    print()


if __name__ == "__main__":
    main()
