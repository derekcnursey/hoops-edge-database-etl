#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import re
from pathlib import Path
from typing import Dict, List

import yaml


def extract_block(s: str, start: int, open_ch: str = "{", close_ch: str = "}"):
    depth = 0
    i = start
    while i < len(s):
        ch = s[i]
        if ch == open_ch:
            depth += 1
        elif ch == close_ch:
            depth -= 1
            if depth == 0:
                return s[start : i + 1], i + 1
        i += 1
    return None, None


def parse_required_params(text: str) -> Dict[str, List[str]]:
    paths_idx = text.find("paths")
    if paths_idx == -1:
        raise ValueError("paths not found in docs file")
    brace_start = text.find("{", paths_idx)
    if brace_start == -1:
        raise ValueError("paths block not found")

    paths_block, _ = extract_block(text, brace_start, "{", "}")
    if not paths_block:
        raise ValueError("failed to extract paths block")

    # parse per-path blocks
    paths: Dict[str, str] = {}
    i = 0
    depth = 0
    while i < len(paths_block):
        ch = paths_block[i]
        if ch == "{":
            depth += 1
            i += 1
            continue
        if ch == "}":
            depth -= 1
            i += 1
            continue
        if depth == 1 and ch == "/":
            j = i
            while j < len(paths_block) and paths_block[j] != ":":
                j += 1
            path = paths_block[i:j].strip()
            k = paths_block.find("{", j)
            if k == -1:
                i = j + 1
                continue
            block, nxt = extract_block(paths_block, k, "{", "}")
            if block:
                paths[path] = block
                i = nxt
                continue
        i += 1

    required: Dict[str, List[str]] = {}
    for path, block in paths.items():
        m = re.search(r"\bget\s*:\s*\{", block)
        if not m:
            continue
        get_block, _ = extract_block(block, m.end() - 1, "{", "}")
        if not get_block:
            continue
        m2 = re.search(r"parameters\s*:\s*\[", get_block)
        if not m2:
            continue
        arr_block, _ = extract_block(get_block, m2.end() - 1, "[", "]")
        if not arr_block:
            continue
        params: List[str] = []
        j = 0
        while j < len(arr_block):
            if arr_block[j] == "{":
                obj, nxt = extract_block(arr_block, j, "{", "}")
                if not obj:
                    break
                name_m = re.search(r'name\s*:\s*"([^"]+)"', obj)
                req_m = re.search(r"required\s*:\s*true", obj)
                if name_m and req_m:
                    params.append(name_m.group(1))
                j = nxt
                continue
            j += 1
        if params:
            required[path] = sorted(set(params))
    return required


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--docs", default="docs/file1.json", help="Path to local docs JSON-like file")
    parser.add_argument("--config", default="config.yaml", help="Path to config.yaml")
    parser.add_argument("--out", default="docs/required_params_from_file1.json", help="Output JSON of required params")
    args = parser.parse_args()

    docs_text = Path(args.docs).read_text(encoding="utf-8")
    required = parse_required_params(docs_text)
    Path(args.out).write_text(json.dumps(required, indent=2), encoding="utf-8")

    cfg = yaml.safe_load(Path(args.config).read_text(encoding="utf-8"))
    endpoints = cfg.get("endpoints", {})

    updated = 0
    for name, spec in endpoints.items():
        path = spec.get("path")
        if not path:
            continue
        if path in required:
            spec["required_params"] = required[path]
            updated += 1

    Path(args.config).write_text(yaml.safe_dump(cfg, sort_keys=False), encoding="utf-8")
    print(f"updated {updated} endpoints in {args.config}")


if __name__ == "__main__":
    main()
