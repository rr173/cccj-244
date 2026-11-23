"""Command line interface: catalog / query / verify."""

from __future__ import annotations

import argparse
import json
import os
import sys

from .catalog import CatalogError, build_catalog, verify_identity
from .format import STATUS_VALUE
from .query import query_window
from .release import ReleaseError, release_archive


def _cmd_catalog(args: argparse.Namespace) -> int:
    catalog = build_catalog(args.archive, args.catalog)
    summary = {
        "catalog": args.catalog,
        "source": catalog["source"],
        "fragments": len(catalog["fragments"]),
        "blocks": len(catalog["blocks"]),
        "damaged_blocks": sum(
            1 for block in catalog["blocks"] if block["status"] == "damaged"
        ),
    }
    print(json.dumps(summary, indent=2, ensure_ascii=False))
    return 0


def _cmd_query(args: argparse.Namespace) -> int:
    result = query_window(
        args.catalog,
        args.start,
        args.end,
        args.columns,
        full_verify=args.full_verify,
    )
    if args.format == "json":
        print(json.dumps(result.to_json(), indent=2, ensure_ascii=False))
        return 0

    header = f"{'row':>8} {'col':>4} {'status':>14} {'value':>22}  block"
    print(header)
    for cell in result.cells:
        value = str(cell.value) if cell.status == STATUS_VALUE else "-"
        print(
            f"{cell.row:>8} {cell.column_id:>4} {cell.status:>14} "
            f"{value:>22}  {cell.block_id or '-'}"
        )
    print(
        f"\nbytes_read={result.bytes_read} "
        f"spans={[(s.offset, s.end) for s in result.spans]} "
        f"blocks={result.blocks_touched}"
    )
    return 0


def _cmd_verify(args: argparse.Namespace) -> int:
    from .catalog import load_catalog

    catalog = load_catalog(args.catalog)
    verify_identity(catalog, args.archive or None, full=True)
    print(json.dumps({"ok": True, "source": catalog["source"]}, ensure_ascii=False))
    return 0


def _cmd_release(args: argparse.Namespace) -> int:
    manifest = release_archive(
        args.archive, args.authorization, args.delivery
    )
    summary = {
        "delivery": args.delivery,
        "parts": len(manifest["parts"]),
        "total_bytes": sum(entry["bytes"] for entry in manifest["parts"]),
        "manifest": os.path.join(args.delivery, "parts.json"),
    }
    print(json.dumps(summary, indent=2, ensure_ascii=False))
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="narc",
        description="Offline cataloging and narrow-window reads for "
                    "instrument archives",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    catalog_cmd = sub.add_parser("catalog", help="build a JSON catalog")
    catalog_cmd.add_argument("archive")
    catalog_cmd.add_argument("catalog")
    catalog_cmd.set_defaults(func=_cmd_catalog)

    query_cmd = sub.add_parser("query", help="read a half-open row window")
    query_cmd.add_argument("catalog")
    query_cmd.add_argument("--start", type=int, required=True)
    query_cmd.add_argument("--end", type=int, required=True)
    query_cmd.add_argument(
        "--columns", type=lambda value: [int(part) for part in value.split(",")],
        required=True,
    )
    query_cmd.add_argument(
        "--full-verify", action="store_true",
        help="re-hash the full source before reading",
    )
    query_cmd.add_argument(
        "--format", choices=("table", "json"), default="table"
    )
    query_cmd.set_defaults(func=_cmd_query)

    verify_cmd = sub.add_parser(
        "verify", help="full identity check against the bound source"
    )
    verify_cmd.add_argument("catalog")
    verify_cmd.add_argument("archive", nargs="?")
    verify_cmd.set_defaults(func=_cmd_verify)

    release_cmd = sub.add_parser(
        "release",
        help="publish an authorized offline release into a new directory",
    )
    release_cmd.add_argument("archive", help="the frozen v1 source archive")
    release_cmd.add_argument(
        "authorization", help="JSON authorization slip"
    )
    release_cmd.add_argument(
        "delivery",
        help="delivery directory to create (must not exist yet)",
    )
    release_cmd.set_defaults(func=_cmd_release)
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        return args.func(args)
    except (CatalogError, ReleaseError, OSError, ValueError) as exc:
        print(f"narc: error: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
