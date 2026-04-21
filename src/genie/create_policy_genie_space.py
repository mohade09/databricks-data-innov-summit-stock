"""
Create the Return Policy Analytics Genie Space.

Internal analytics space for return policy optimization, used by the
Supervisor Agent and operations team. Connects to return analysis,
abuse signals, and customer returns gold tables.

Usage:
  python3 src/genie/create_policy_genie_space.py --profile fe-vm-vdm-serverless-iwnbow
  python3 src/genie/create_policy_genie_space.py --profile fe-vm-vdm-serverless-iwnbow --replace
"""

import argparse
import json
import subprocess
import sys
import uuid


GENIE_SPACE_CONFIG = {
    "title": "Return Policy Analytics",
    "description": (
        "Internal analytics for return policy optimization. "
        "Analyze return patterns, abuse signals, refund costs, and "
        "category-level metrics to inform policy decisions. "
        "Used by the Supervisor Agent and operations team."
    ),
    "tables": [
        "debadm.ecom_gold.abuse_signals",
        "debadm.ecom_gold.customer_returns",
        "debadm.ecom_gold.return_analysis",
        "debadm.ecom_gold.return_eligibility",
    ],
    "sample_questions": [
        "What is the return rate by product category this quarter?",
        "Which categories have the highest refund cost?",
        "Show me the monthly trend of defective returns",
        "What percentage of returns are due to changed mind vs defective?",
        "Which brands have the highest return rate?",
        "How many high-risk abuse customers do we have?",
        "What is the average processing time for returns by category?",
        "Show me the rejection rate trend over the last 6 months",
        "Which product categories should we tighten the return window for?",
        "Compare return reasons across Electronics vs Clothing",
        "Top 10 customers by total refund value received",
        "What is our overall defective rate and how does it compare by brand?",
    ],
}


def run_cli(args: list[str], profile: str) -> dict:
    cmd = ["databricks"] + args + ["--profile", profile, "--output", "json"]
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        print(f"ERROR: {result.stderr.strip()}", file=sys.stderr)
        sys.exit(1)
    return json.loads(result.stdout) if result.stdout.strip() else {}


def find_existing_space(profile: str) -> str | None:
    spaces = run_cli(["api", "get", "/api/2.0/genie/spaces"], profile)
    for space in spaces.get("spaces", []):
        if space.get("title") == GENIE_SPACE_CONFIG["title"]:
            return space["space_id"]
    return None


def delete_space(space_id: str, profile: str):
    print(f"Deleting existing space {space_id}...")
    run_cli(["api", "delete", f"/api/2.0/genie/spaces/{space_id}"], profile)
    print("Deleted.")


def get_warehouse_id(profile: str) -> str:
    result = run_cli(["api", "get", "/api/2.0/sql/warehouses"], profile)
    warehouses = result.get("warehouses", [])
    running = [w for w in warehouses if w.get("state") == "RUNNING"]
    if running:
        return running[0]["id"]
    if warehouses:
        return warehouses[0]["id"]
    print("ERROR: No SQL warehouses found.", file=sys.stderr)
    sys.exit(1)


def create_space(warehouse_id: str, profile: str) -> dict:
    serialized = {
        "version": 2,
        "config": {
            "sample_questions": [
                {"id": uuid.uuid4().hex, "question": [q]}
                for q in GENIE_SPACE_CONFIG["sample_questions"]
            ]
        },
        "data_sources": {
            "tables": [
                {"identifier": t} for t in sorted(GENIE_SPACE_CONFIG["tables"])
            ]
        },
        "instructions": {},
    }

    payload = {
        "title": GENIE_SPACE_CONFIG["title"],
        "description": GENIE_SPACE_CONFIG["description"],
        "warehouse_id": warehouse_id,
        "serialized_space": json.dumps(serialized),
    }

    import tempfile
    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
        json.dump(payload, f)
        tmp_path = f.name

    return run_cli(["api", "post", "/api/2.0/genie/spaces", "--json", f"@{tmp_path}"], profile)


def main():
    parser = argparse.ArgumentParser(description="Create Return Policy Analytics Genie Space")
    parser.add_argument("--profile", required=True, help="Databricks CLI profile name")
    parser.add_argument("--warehouse-id", default=None, help="SQL warehouse ID")
    parser.add_argument("--replace", action="store_true", help="Delete existing space first")
    args = parser.parse_args()

    existing = find_existing_space(args.profile)
    if existing:
        if args.replace:
            delete_space(existing, args.profile)
        else:
            print(f"Genie Space already exists: {existing}")
            print("Use --replace to recreate.")
            sys.exit(0)

    warehouse_id = args.warehouse_id or get_warehouse_id(args.profile)
    print(f"Using warehouse: {warehouse_id}")
    print("Creating Genie Space...")

    result = create_space(warehouse_id, args.profile)
    space_id = result.get("space_id", "unknown")

    print(f"\nGenie Space created successfully!")
    print(f"  Space ID:  {space_id}")
    print(f"  Title:     {GENIE_SPACE_CONFIG['title']}")
    print(f"  Tables:    {len(GENIE_SPACE_CONFIG['tables'])}")
    print(f"  Questions: {len(GENIE_SPACE_CONFIG['sample_questions'])}")

    try:
        profiles = subprocess.run(
            ["databricks", "auth", "profiles", "--output", "json"],
            capture_output=True, text=True
        )
        for p in json.loads(profiles.stdout).get("profiles", []):
            if p.get("name") == args.profile:
                host = p.get("host", "").rstrip("/")
                print(f"\n  URL: {host}/genie/rooms/{space_id}")
                break
    except Exception:
        pass

    config = {
        "space_id": space_id,
        "display_name": GENIE_SPACE_CONFIG["title"],
        "warehouse_id": warehouse_id,
        "tables": GENIE_SPACE_CONFIG["tables"],
        "sample_questions": GENIE_SPACE_CONFIG["sample_questions"],
    }
    config_path = "src/genie/genie_policy_space_config.json"
    with open(config_path, "w") as f:
        json.dump(config, f, indent=2)
    print(f"  Config saved to {config_path}")


if __name__ == "__main__":
    main()
