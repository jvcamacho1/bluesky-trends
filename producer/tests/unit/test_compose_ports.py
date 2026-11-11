"""Garante que nenhuma porta do Compose escapa do loopback.

Porta publicada pelo Docker ignora o firewall do host: um `ports:` sem
127.0.0.1: fica exposto na internet mesmo com ufw fechado. Este teste falha
se algum mapeamento nao comecar com 127.0.0.1:.
"""

import re
from pathlib import Path

import yaml

COMPOSE = Path(__file__).resolve().parents[3] / "docker-compose.yml"


def _published_ports(service_name: str, service: dict) -> list[str]:
    return [str(entry) for entry in service.get("ports", []) or []]


def test_every_published_port_binds_to_loopback():
    compose = yaml.safe_load(COMPOSE.read_text(encoding="utf-8"))
    offenders = []
    for name, service in (compose.get("services") or {}).items():
        for entry in _published_ports(name, service):
            if not entry.startswith("127.0.0.1:"):
                offenders.append(f"{name}: {entry}")
    assert not offenders, (
        "portas publicadas fora de 127.0.0.1 (use 127.0.0.1:host:container):\n"
        + "\n".join(offenders)
    )


def test_no_service_uses_the_other_site_port():
    compose = yaml.safe_load(COMPOSE.read_text(encoding="utf-8"))
    for name, service in (compose.get("services") or {}).items():
        for entry in _published_ports(name, service):
            host_side = entry.split(":")[-2]
            assert host_side != "8000", (
                f"{name} publica a porta 8000, ja usada por outro site"
            )


def test_every_service_has_memory_and_cpu_limits():
    compose = yaml.safe_load(COMPOSE.read_text(encoding="utf-8"))
    for name, service in (compose.get("services") or {}).items():
        limits = ((service.get("deploy") or {}).get("resources") or {}).get(
            "limits"
        ) or {}
        assert limits.get("memory"), f"{name} sem limite de memoria"
        assert limits.get("cpus"), f"{name} sem limite de cpus"


def _to_gb(value: str) -> float:
    text = str(value).strip().lower()
    match = re.fullmatch(r"([0-9.]+)\s*(g|gb|m|mb)?", text)
    assert match, f"limite de memoria ilegivel: {value}"
    number = float(match.group(1))
    unit = match.group(2) or "m"
    return number if unit.startswith("g") else number / 1024


def test_total_budget_fits_the_shared_vps():
    compose = yaml.safe_load(COMPOSE.read_text(encoding="utf-8"))
    total_memory = 0.0
    total_cpus = 0.0
    for service in (compose.get("services") or {}).values():
        limits = ((service.get("deploy") or {}).get("resources") or {}).get(
            "limits"
        ) or {}
        total_memory += _to_gb(limits.get("memory", "0"))
        total_cpus += float(limits.get("cpus", 0))
    assert total_memory <= 7.0, (
        f"memoria total {total_memory:.2f} GB acima do orcamento de 7 GB"
    )
    assert total_cpus <= 3.0, f"cpus totais {total_cpus:.2f} acima do orcamento de 3"
