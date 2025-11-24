#!/usr/bin/env python3
"""
Deploy Cloudflare Worker from wrangler.template.toml

This script:
1. Prompts for required configuration values
2. Renders wrangler.template.toml -> wrangler.toml (gitignored)
3. Deploys the worker using npx wrangler deploy
"""

import argparse
import os
import sys
from pathlib import Path


def render_template(template_path: Path, output_path: Path, variables: dict) -> None:
    """Render template file with variable substitution."""
    print(f"📝 Rendering template from {template_path}")

    with open(template_path, 'r') as f:
        content = f.read()

    # Replace all {{VAR}} placeholders
    for key, value in variables.items():
        placeholder = f"{{{{{key}}}}}"
        content = content.replace(placeholder, value)

    # Check for any remaining placeholders
    if "{{" in content and "}}" in content:
        remaining = [line for line in content.split('\n') if "{{" in line and "}}" in line]
        print("⚠️  Warning: Template still contains unreplaced placeholders:")
        for line in remaining:
            print(f"   {line.strip()}")

    with open(output_path, 'w') as f:
        f.write(content)

    print(f"✅ Rendered configuration to {output_path}")


def prompt_for_variables(args) -> dict:
    """Prompt user for required variables or use command-line arguments."""
    variables = {}

    # Worker name
    if args.worker_name:
        variables['WORKER_NAME'] = args.worker_name
    else:
        default_name = "otlp2parquet-worker"
        worker_name = input(f"Worker name [{default_name}]: ").strip()
        variables['WORKER_NAME'] = worker_name or default_name

    # Catalog mode
    if args.catalog_mode:
        catalog_mode = args.catalog_mode
    else:
        print("\nCatalog mode:")
        print("  none    - Plain Parquet files (no Iceberg catalog)")
        print("  iceberg - R2 Data Catalog with Iceberg tables (requires API token)")
        catalog_mode = input("Catalog mode [none]: ").strip().lower() or "none"
        if catalog_mode not in ["none", "iceberg"]:
            print(f"❌ Error: Invalid catalog mode '{catalog_mode}'. Must be 'none' or 'iceberg'")
            sys.exit(1)

    variables['CATALOG_MODE'] = catalog_mode

    # Bucket name
    if args.bucket_name:
        variables['BUCKET_NAME'] = args.bucket_name
    else:
        bucket_name = input("R2 Bucket name: ").strip()
        if not bucket_name:
            print("❌ Error: Bucket name is required")
            sys.exit(1)
        variables['BUCKET_NAME'] = bucket_name

    # Cloudflare account ID
    if args.account_id:
        variables['CLOUDFLARE_ACCOUNT_ID'] = args.account_id
    else:
        account_id = input("Cloudflare Account ID: ").strip()
        if not account_id:
            print("❌ Error: Cloudflare Account ID is required")
            sys.exit(1)
        variables['CLOUDFLARE_ACCOUNT_ID'] = account_id

    # R2 credentials (always required for writing Parquet files)
    if args.access_key_id:
        variables['R2_ACCESS_KEY_ID'] = args.access_key_id
    else:
        access_key = input("R2 Access Key ID: ").strip()
        if not access_key:
            print("❌ Error: R2 Access Key ID is required")
            sys.exit(1)
        variables['R2_ACCESS_KEY_ID'] = access_key

    if args.secret_access_key:
        variables['R2_SECRET_ACCESS_KEY'] = args.secret_access_key
    else:
        secret_key = input("R2 Secret Access Key: ").strip()
        if not secret_key:
            print("❌ Error: R2 Secret Access Key is required")
            sys.exit(1)
        variables['R2_SECRET_ACCESS_KEY'] = secret_key

    # Cloudflare API token (only required for iceberg mode)
    if catalog_mode == "iceberg":
        if args.api_token:
            variables['CLOUDFLARE_API_TOKEN'] = args.api_token
        else:
            print("\n⚠️  Iceberg mode requires Cloudflare API token for R2 Data Catalog")
            api_token = input("Cloudflare API Token: ").strip()
            if not api_token:
                print("❌ Error: Cloudflare API Token is required for iceberg mode")
                sys.exit(1)
            variables['CLOUDFLARE_API_TOKEN'] = api_token
    else:
        # For 'none' mode, set empty token to avoid template errors
        variables['CLOUDFLARE_API_TOKEN'] = ""

    return variables


def deploy_worker(cloudflare_dir: Path, dry_run: bool = False) -> None:
    """Deploy the worker using wrangler."""
    if dry_run:
        print("🔍 Dry run mode - skipping deployment")
        print(f"Would run: npx wrangler deploy (in {cloudflare_dir})")
        return

    print(f"\n🚀 Deploying worker from {cloudflare_dir}")

    import subprocess
    result = subprocess.run(
        ["npx", "wrangler", "deploy"],
        cwd=cloudflare_dir,
        check=False
    )

    if result.returncode == 0:
        print("\n✅ Worker deployed successfully!")
    else:
        print(f"\n❌ Deployment failed with exit code {result.returncode}")
        sys.exit(result.returncode)


def main():
    parser = argparse.ArgumentParser(
        description="Deploy Cloudflare Worker from template",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Interactive mode (prompts for all values)
  %(prog)s

  # Non-interactive mode with plain Parquet (no catalog)
  %(prog)s --worker-name my-worker --catalog-mode none \\
           --bucket-name my-bucket --account-id abc123 \\
           --access-key-id KEY --secret-access-key SECRET

  # Non-interactive mode with Iceberg catalog
  %(prog)s --worker-name my-worker --catalog-mode iceberg \\
           --bucket-name my-bucket --account-id abc123 \\
           --access-key-id KEY --secret-access-key SECRET \\
           --api-token TOKEN

  # Dry run (render template only, skip deployment)
  %(prog)s --dry-run
        """
    )

    parser.add_argument(
        '--worker-name',
        help='Worker name (default: otlp2parquet-worker)'
    )
    parser.add_argument(
        '--catalog-mode',
        choices=['none', 'iceberg'],
        help='Catalog mode: "none" for plain Parquet, "iceberg" for R2 Data Catalog (default: none)'
    )
    parser.add_argument(
        '--bucket-name',
        help='R2 bucket name (required)'
    )
    parser.add_argument(
        '--account-id',
        help='Cloudflare Account ID (required)'
    )
    parser.add_argument(
        '--access-key-id',
        help='R2 Access Key ID (required)'
    )
    parser.add_argument(
        '--secret-access-key',
        help='R2 Secret Access Key (required)'
    )
    parser.add_argument(
        '--api-token',
        help='Cloudflare API Token (required for iceberg mode)'
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Render template but skip deployment'
    )

    args = parser.parse_args()

    # Find repo root
    script_dir = Path(__file__).parent
    repo_root = script_dir.parent
    cloudflare_dir = repo_root / "crates" / "otlp2parquet-cloudflare"
    template_path = cloudflare_dir / "wrangler.template.toml"
    output_path = cloudflare_dir / "wrangler.toml"

    # Verify template exists
    if not template_path.exists():
        print(f"❌ Error: Template not found at {template_path}")
        sys.exit(1)

    print("🎯 Cloudflare Worker Deployment Script")
    print("=" * 50)

    # Gather variables
    variables = prompt_for_variables(args)

    # Show summary
    print("\n📋 Configuration Summary:")
    print(f"  Worker Name: {variables['WORKER_NAME']}")
    print(f"  Catalog Mode: {variables['CATALOG_MODE']}")
    print(f"  Bucket Name: {variables['BUCKET_NAME']}")
    print(f"  Account ID: {variables['CLOUDFLARE_ACCOUNT_ID']}")
    print(f"  Access Key: {variables['R2_ACCESS_KEY_ID'][:8]}...")
    print(f"  Secret Key: {'*' * 20}")
    if variables['CATALOG_MODE'] == 'iceberg':
        print(f"  API Token: {variables['CLOUDFLARE_API_TOKEN'][:8]}...")

    # Render template
    print()
    render_template(template_path, output_path, variables)

    # Deploy
    deploy_worker(cloudflare_dir, dry_run=args.dry_run)

    if not args.dry_run:
        print(f"\n📁 Note: {output_path} has been created and is gitignored")
        print(f"💡 To delete this worker later, run: npx wrangler delete {variables['WORKER_NAME']}")


if __name__ == "__main__":
    main()
