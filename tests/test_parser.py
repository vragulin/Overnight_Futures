
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("--dry-run", action="store_true", help="compute but do not modify the database")
args = parser.parse_args()

print("dry_run:", args.dry_run)  # True if `--dry-run` given, else False