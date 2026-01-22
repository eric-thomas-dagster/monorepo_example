"""Validate all code locations in the workspace."""

import subprocess
import sys
from pathlib import Path

def validate_code_location(location_path: Path, location_name: str):
    """Validate a single code location."""
    print(f"\n{'='*60}")
    print(f"Validating {location_name}...")
    print(f"{'='*60}")

    try:
        # Try to import the definitions
        sys.path.insert(0, str(location_path))
        module_name = location_path.name

        definitions_module = __import__(f"{module_name}.definitions", fromlist=["defs"])
        defs_func = getattr(definitions_module, "defs")

        # Call the defs function to ensure it works
        definitions = defs_func()

        print(f"✓ {location_name} loaded successfully")
        print(f"  Assets: {len(list(definitions.get_asset_graph().all_asset_keys))}")

        # List assets
        for asset_key in definitions.get_asset_graph().all_asset_keys:
            print(f"    - {asset_key.to_user_string()}")

        return True
    except Exception as e:
        print(f"✗ {location_name} failed to load:")
        print(f"  Error: {str(e)}")
        import traceback
        traceback.print_exc()
        return False

def main():
    """Validate all code locations."""
    workspace_root = Path(__file__).parent
    code_locations_dir = workspace_root / "code_locations"

    print("Validating Multi-Code Location Demo Workspace")
    print(f"Workspace: {workspace_root}")

    locations = [
        "fintech_alpha",
        "insurance_beta",
        "healthcare_gamma",
        "shared"
    ]

    results = {}
    for location_name in locations:
        location_path = code_locations_dir / location_name / location_name
        results[location_name] = validate_code_location(location_path, location_name)

    print(f"\n{'='*60}")
    print("VALIDATION SUMMARY")
    print(f"{'='*60}")

    for location_name, success in results.items():
        status = "✓ PASS" if success else "✗ FAIL"
        print(f"{status} - {location_name}")

    all_passed = all(results.values())
    if all_passed:
        print("\n✓ All code locations validated successfully!")
        sys.exit(0)
    else:
        print("\n✗ Some code locations failed validation")
        sys.exit(1)

if __name__ == "__main__":
    main()
