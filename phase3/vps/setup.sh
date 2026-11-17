#!/bin/bash
# phase3/vps/setup.sh — εγκατάσταση του recorder σε φρέσκο Ubuntu 24.04 (το καλεί το cloud-init ως root).
# Προϋπόθεση: /etc/pm-meter/token (fine-grained GitHub token, μόνο για polymarket-5m-meter, Contents read/write).
set -euo pipefail
REPO=nikolaskahris-png/polymarket-5m-meter
export DEBIAN_FRONTEND=noninteractive
apt-get update -y && apt-get install -y python3-venv git cron
mkdir -p /data/rec /data/status /data/jobout /opt
[ -d /opt/meter ] || git clone --depth 50 https://github.com/$REPO.git /opt/meter
python3 -m venv /opt/venv && /opt/venv/bin/pip install -q "websockets==13.1"
# κλώνος για το branch vps-data (εκεί γράφει ο server· το main μένει μόνο για κώδικα)
TOKEN=$(cat /etc/pm-meter/token)
if [ ! -d /opt/vpsdata ]; then
  git clone https://x-access-token:${TOKEN}@github.com/$REPO.git /opt/vpsdata
  cd /opt/vpsdata
  git config user.name "vps-meter"; git config user.email "vps-meter@users.noreply.github.com"
  if git ls-remote --exit-code --heads origin vps-data >/dev/null 2>&1; then git checkout vps-data
  else git checkout --orphan vps-data && git rm -rq . && echo "VPS output (status, job results)" > README.md && git add README.md && git commit -qm "vps-data init" && git push -q origin vps-data; fi
fi
cat > /etc/systemd/system/pm-recorder.service <<'UNIT'
[Unit]
Description=Polymarket 5m BTC order book recorder (phase3)
After=network-online.target
Wants=network-online.target
[Service]
ExecStart=/opt/venv/bin/python /opt/meter/phase3/recorder.py --data /data --levels 10
Restart=always
RestartSec=5
[Install]
WantedBy=multi-user.target
UNIT
systemctl daemon-reload && systemctl enable --now pm-recorder.service

echo "*/30 * * * * root bash /opt/meter/phase3/vps/sync.sh >> /data/sync.log 2>&1" > /etc/cron.d/pm-sync
bash /opt/meter/phase3/vps/sync.sh >> /data/sync.log 2>&1 || true
echo "setup done $(date -u)" > /data/setup_done
