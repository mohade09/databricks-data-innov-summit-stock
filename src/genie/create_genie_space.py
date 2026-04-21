"""
Create the ShopEase Customer Support Genie Space.

This script creates a Genie Space under the authenticated user's identity
using the Databricks CLI profile. The Genie Space connects to 5 gold tables
and provides a natural language interface for product returns and invoice lookup.

Prerequisites:
  - Databricks CLI authenticated: `databricks auth login`
  - Gold layer tables populated in debadm.ecom_gold.*
  - SQL warehouse running

Usage:
  python src/genie/create_genie_space.py --profile fe-vm-vdm-serverless-iwnbow

  # With custom warehouse:
  python src/genie/create_genie_space.py --profile fe-vm-vdm-serverless-iwnbow --warehouse-id <id>

  # Delete existing space first:
  python src/genie/create_genie_space.py --profile fe-vm-vdm-serverless-iwnbow --replace
"""

import argparse
import json
import subprocess
import sys
import uuid


GENIE_SPACE_CONFIG = {
    "title": "ShopEase Customer Support",
    "description": (
        "Customer-facing chatbot for product returns and past invoice lookup. "
        "Ask about orders, returns, refunds, or invoices."
    ),
    "tables": [
        "debadm.ecom_gold.customer_orders",
        "debadm.ecom_gold.customer_returns",
        "debadm.ecom_gold.invoice_details",
        "debadm.ecom_gold.refund_status",
        "debadm.ecom_gold.return_eligibility",
    ],
    "sample_questions": [
        "Show me my recent orders",
        "Can I return the headphones from my last order?",
        "What is the status of my return RET-00123?",
        "Where is my refund?",
        "Show me invoice INV-2025-00456",
        "Has my payment for order ORD-12345 been processed?",
        "Which of my items are still eligible for return?",
        "How much did I spend last month?",
        "Show me all my invoices from January",
        "What products have I returned?",
    ],
}


def run_cli(args: list[str], profile: str) -> dict:
    """Run a Databricks CLI command and return parsed JSON output."""
    cmd = ["databricks"] + args + ["--profile", profile, "--output", "json"]
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        print(f"ERROR: {result.stderr.strip()}", file=sys.stderr)
        sys.exit(1)
    return json.loads(result.stdout) if result.stdout.strip() else {}


def find_existing_space(profile: str) -> str | None:
    """Find an existing ShopEase Genie Space by title."""
    spaces = run_cli(["api", "get", "/api/2.0/genie/spaces"], profile)
    for space in spaces.get("spaces", []):
        if space.get("title") == GENIE_SPACE_CONFIG["title"]:
            return space["space_id"]
    return None


def delete_space(space_id: str, profile: str):
    """Delete a Genie Space by ID."""
    print(f"Deleting existing space {space_id}...")
    run_cli(["api", "delete", f"/api/2.0/genie/spaces/{space_id}"], profile)
    print("Deleted.")


def build_payload(warehouse_id: str) -> dict:
    """Build the Genie Space creation payload with serialized_space."""
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

    return {
        "title": GENIE_SPACE_CONFIG["title"],
        "description": GENIE_SPACE_CONFIG["description"],
        "warehouse_id": warehouse_id,
        "serialized_space": json.dumps(serialized),
    }


def create_space(warehouse_id: str, profile: str) -> dict:
    """Create the Genie Space via Databricks REST API."""
    payload = build_payload(warehouse_id)

    # Write payload to temp file for CLI
    import tempfile
    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
        json.dump(payload, f)
        tmp_path = f.name

    result = run_cli(["api", "post", "/api/2.0/genie/spaces", "--json", f"@{tmp_path}"], profile)
    return result


def get_warehouse_id(profile: str) -> str:
    """Get the first available SQL warehouse ID."""
    result = run_cli(["api", "get", "/api/2.0/sql/warehouses"], profile)
    warehouses = result.get("warehouses", [])
    running = [w for w in warehouses if w.get("state") == "RUNNING"]
    if running:
        return running[0]["id"]
    if warehouses:
        return warehouses[0]["id"]
    print("ERROR: No SQL warehouses found.", file=sys.stderr)
    sys.exit(1)


def main():
    parser = argparse.ArgumentParser(description="Create ShopEase Customer Support Genie Space")
    parser.add_argument("--profile", required=True, help="Databricks CLI profile name")
    parser.add_argument("--warehouse-id", default=None, help="SQL warehouse ID (auto-detected if omitted)")
    parser.add_argument("--replace", action="store_true", help="Delete existing space with same name before creating")
    args = parser.parse_args()

    # Check for existing space
    existing = find_existing_space(args.profile)
    if existing:
        if args.replace:
            delete_space(existing, args.profile)
        else:
            print(f"Genie Space already exists: {existing}")
            print(f"URL: Use --replace flag to recreate it.")
            sys.exit(0)

    # Get warehouse
    warehouse_id = args.warehouse_id or get_warehouse_id(args.profile)
    print(f"Using warehouse: {warehouse_id}")

    # Create
    print("Creating Genie Space...")
    result = create_space(warehouse_id, args.profile)
    space_id = result.get("space_id", "unknown")

    print(f"\nGenie Space created successfully!")
    print(f"  Space ID:  {space_id}")
    print(f"  Title:     {GENIE_SPACE_CONFIG['title']}")
    print(f"  Tables:    {len(GENIE_SPACE_CONFIG['tables'])}")
    print(f"  Questions: {len(GENIE_SPACE_CONFIG['sample_questions'])}")
    print(f"  Warehouse: {warehouse_id}")

    # Extract host from profile
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

    # Save space_id to config
    config_path = "src/genie/genie_space_config.json"
    config = {
        "space_id": space_id,
        "display_name": GENIE_SPACE_CONFIG["title"],
        "warehouse_id": warehouse_id,
        "tables": GENIE_SPACE_CONFIG["tables"],
        "sample_questions": GENIE_SPACE_CONFIG["sample_questions"],
    }
    with open(config_path, "w") as f:
        json.dump(config, f, indent=2)
    print(f"\n  Config saved to {config_path}")


if __name__ == "__main__":
    main()
