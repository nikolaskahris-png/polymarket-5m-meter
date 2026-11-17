#!/bin/bash
# phase3/vps/sync.sh — κάθε 30': (1) νέος κώδικας από main, (2) εκτέλεση νέων εργασιών phase3/vps/jobs/*.sh (μία φορά η καθεμία),
# (3) αποστολή κατάστασης και αποτελεσμάτων εργασιών στο branch vps-data. Τα raw δεδομένα ΜΕΝΟΥΝ στον server.
set -uo pipefail
exec 9>/tmp/pm-sync.lock; flock -n 9 || exit 0
cd /opt/meter && git pull -q --ff-only origin main || echo "pull failed"
touch /data/jobs_done.txt
for j in /opt/meter/phase3/vps/jobs/*.sh; do
  [ -e "$j" ] || continue; n=$(basename "$j" .sh)
  grep -qx "$n" /data/jobs_done.txt && continue
  echo "$n" >> /data/jobs_done.txt
  mkdir -p /data/jobout/$n
  ( cd /opt/meter && JOBOUT=/data/jobout/$n timeout 6h bash "$j" ) > /data/jobout/$n/log.txt 2>&1
  echo "exit=$?" >> /data/jobout/$n/log.txt
done
cd /opt/vpsdata && git pull -q origin vps-data || true
mkdir -p status jobs
cp /data/status/latest.json status/ 2>/dev/null; cp /data/status/hour=*.json status/ 2>/dev/null
{ date -u +%FT%TZ; df -h /data | tail -1; du -sh /data/rec 2>/dev/null; du -sh /data/rec/* 2>/dev/null | tail -20; systemctl is-active pm-recorder; tail -5 /data/sync.log; } > status/disk.txt
for d in /data/jobout/*/; do n=$(basename "$d"); [ "$(du -sm "$d" | cut -f1)" -le 20 ] && { mkdir -p jobs/$n; cp -r "$d"* jobs/$n/; }; done
git add -A status jobs && git commit -qm "vps $(date -u +%FT%TZ)" && git push -q origin vps-data || true
