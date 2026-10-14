#!/usr/bin/env bash
# Install/update the reporter and start its systemd service.
set -euo pipefail

repo_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
config_file=/etc/connectivity-reporter/config.local.json
config_input=
if [[ $# -gt 0 ]]; then
    if [[ $# -ne 2 || $1 != --config ]]; then
        echo "Usage: sudo ./install.sh [--config /path/to/config.json]" >&2
        exit 1
    fi
    config_input=$2
elif [[ -f $config_file ]]; then
    config_input=$config_file
elif [[ -f "$repo_dir/config.json" ]]; then
    config_input=$repo_dir/config.json
elif [[ -f "$repo_dir/config.local.json" ]]; then
    config_input=$repo_dir/config.local.json
else
    echo "Copy config.example.json to config.json and configure it first." >&2
    exit 1
fi
if [[ $EUID -ne 0 ]]; then
    echo "Run this installer with sudo." >&2
    exit 1
fi
for executable in python3 systemctl useradd getent install runuser; do
    command -v "$executable" >/dev/null || { echo "Missing requirement: $executable" >&2; exit 1; }
done
python3 -c 'import sys; sys.exit(0 if sys.version_info >= (3, 11) else "Python 3.11 or newer is required")'
if [[ ! -d /run/systemd/system ]]; then
    echo "This installer requires a Linux host running systemd." >&2
    exit 1
fi
# Validate before modifying an existing installation. Never print credentials.
PYTHONPATH="$repo_dir/src" python3 -m connectivity_reporter --config "$config_input" --check-config
python3 - "$config_input" <<'PY'
import json, sys
config = json.load(open(sys.argv[1]))
if config.get('database_path') != '/var/lib/connectivity-reporter/connectivity.sqlite3':
    sys.exit('Set database_path to /var/lib/connectivity-reporter/connectivity.sqlite3 before installing.')
PY

if ! getent passwd connectivity-reporter >/dev/null; then
    useradd --system --user-group --home-dir /var/lib/connectivity-reporter --shell /usr/sbin/nologin connectivity-reporter
fi
getent group connectivity-reporter >/dev/null || { echo "Service group connectivity-reporter is missing" >&2; exit 1; }
install -d -m 755 /opt/connectivity-reporter
install -d -m 750 -o root -g connectivity-reporter /etc/connectivity-reporter
install -d -m 700 -o connectivity-reporter -g connectivity-reporter /var/lib/connectivity-reporter

# No pip or package downloads needed: the reporter uses only Python's standard library.
python3 -m venv --without-pip /opt/connectivity-reporter/.venv
site_packages=$(/opt/connectivity-reporter/.venv/bin/python -c 'import sysconfig; print(sysconfig.get_path("purelib"))')

systemctl stop connectivity-reporter.service 2>/dev/null || {
    if systemctl cat connectivity-reporter.service >/dev/null 2>&1; then
        echo "Could not stop the existing reporter; installation aborted." >&2
        exit 1
    fi
}
install -d -m 755 "$site_packages/connectivity_reporter"
for source_file in "$repo_dir"/src/connectivity_reporter/*.py; do
    install -m 644 "$source_file" "$site_packages/connectivity_reporter/"
done
if [[ $(readlink -f -- "$config_input") != "$config_file" ]]; then
    install -m 640 -o root -g connectivity-reporter "$config_input" "$config_file"
else
    chown root:connectivity-reporter "$config_file"
    chmod 640 "$config_file"
fi
runuser -u connectivity-reporter -- /opt/connectivity-reporter/.venv/bin/python -m connectivity_reporter --config "$config_file" --check-config
install -m 644 "$repo_dir/deployment/connectivity-reporter.service" /etc/systemd/system/connectivity-reporter.service
systemctl daemon-reload
systemctl enable connectivity-reporter.service
systemctl restart connectivity-reporter.service
systemctl --no-pager --full status connectivity-reporter.service
printf '\nInstalled. Logs: sudo journalctl -u connectivity-reporter -f\n'
