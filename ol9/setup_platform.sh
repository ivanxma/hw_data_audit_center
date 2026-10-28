#!/usr/bin/env bash
set -euo pipefail
platform_verify() {
  . /etc/os-release
  [[ "$ID" == ol && "${VERSION_ID%%.*}" == 9 ]] || { echo 'Only Oracle Linux 9 is supported.' >&2; exit 1; }
}
platform_install_dependencies() {
  sudo dnf install -y --setopt=install_weak_deps=False python3.12 python3.12-pip python3.12-devel libaio ncurses-libs xz curl openssl firewalld podman >&2
}
platform_open_https() {
  sudo timeout 20 systemctl enable --now firewalld
  local zone
  zone="$(sudo timeout 10 firewall-cmd --get-active-zones | awk 'NR==1{print $1}')"
  zone="${zone:-public}"
  sudo timeout 10 firewall-cmd --zone="$zone" --permanent --add-port="${AUDIT_HTTPS_PORT:-443}/tcp"
  sudo timeout 10 firewall-cmd --reload
}
