#!/usr/bin/env python3
"""Redacts every secret/token/password/key from deploy/.env plus Bearer/header secrets from stdin -> stdout."""
import re, sys
from pathlib import Path

env_path = Path("/mnt/d/Interworks/Safi/deploy/.env")
secrets = []
for line in env_path.read_text(encoding="utf-8").splitlines():
    line = line.strip()
    if line and not line.startswith("#") and "=" in line:
        k, v = line.split("=", 1)
        if any(tok in k.upper() for tok in ("TOKEN", "SECRET", "PASSWORD", "KEY")) and len(v.strip()) >= 6:
            secrets.append(v.strip())

text = sys.stdin.read()
for s in sorted(secrets, key=len, reverse=True):
    text = text.replace(s, "[REDACTED]")
text = re.sub(r"Bearer\s+[A-Za-z0-9._\-]{8,}", "Bearer [REDACTED]", text)
text = re.sub(r"gh[opsu]_[A-Za-z0-9]{16,}", "[REDACTED_GH_TOKEN]", text)
sys.stdout.write(text)
