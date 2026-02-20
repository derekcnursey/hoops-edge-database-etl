import argparse
import subprocess


def main() -> None:
    parser = argparse.ArgumentParser(description="Delete fct_games partitions for a given asof across seasons.")
    parser.add_argument("--asof", required=True, help="asof date to delete, e.g. 2026-01-28")
    parser.add_argument("--season-start", type=int, default=2010)
    parser.add_argument("--season-end", type=int, default=2026)
    parser.add_argument("--bucket", default="hoops-edge")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    for season in range(args.season_start, args.season_end + 1):
        uri = f"s3://{args.bucket}/silver/fct_games/season={season}/asof={args.asof}/"
        cmd = ["aws", "s3", "rm", "--recursive", uri]
        print(" ".join(cmd))
        if not args.dry_run:
            subprocess.check_call(cmd)


if __name__ == "__main__":
    main()
