#!/usr/bin/env python3
"""
Sync the RBAC rules from config/rbac/role.yaml into the Helm chart ClusterRole template.

Usage: python3 hack/sync-helm-rbac.py <role.yaml> <helm-rbac.yaml>

The Helm template file contains two YAML documents separated by '---':
  1. ClusterRole  (rules are replaced here)
  2. ClusterRoleBinding  (left untouched)
"""
import re
import sys

role_path, helm_path = sys.argv[1], sys.argv[2]

with open(role_path) as f:
    role_lines = f.readlines()

# Extract the 'rules:' block from the generated role.yaml (everything from 'rules:' to EOF)
rules_start = next(i for i, l in enumerate(role_lines) if l.startswith("rules:"))
rules_block = "".join(role_lines[rules_start:]).rstrip("\n")

with open(helm_path) as f:
    helm_content = f.read()

# Replace the rules block in the ClusterRole document (before the '---' separator)
if not re.search(r"rules:.*?(?=\n---)", helm_content, flags=re.DOTALL):
    print("error: no rules block found in", helm_path)
    sys.exit(1)

updated = re.sub(
    r"rules:.*?(?=\n---)",
    rules_block,
    helm_content,
    count=1,
    flags=re.DOTALL,
)

if updated == helm_content:
    print(f"already in sync: {helm_path}")
    sys.exit(0)

with open(helm_path, "w") as f:
    f.write(updated)

print(f"synced rules from {role_path} → {helm_path}")
