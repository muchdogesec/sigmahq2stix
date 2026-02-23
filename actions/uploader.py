"""
Uploader script for sigma2stix GitHub Action
Retrieves changed YAML files from GitHub and uploads them to SIEMRULES API
"""

import os
import random
import sys
import argparse
from urllib.parse import urljoin
import uuid
import requests
import time
import json
import tempfile
import shutil
from pathlib import Path
from typing import List, Dict, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed
from git import Repo
import yaml


### ENVIRONMENT VARIABLES
GITHUB_REPO_URL = (
    os.environ.get("GITHUB_REPO_URL", "https://github.com/SigmaHQ/sigma").strip("/")
    + "/"
)  # External repository URL to clone
SIEMRULES_BASE_URL = os.environ.get("SIEMRULES_BASE_URL") # Base URL for SIEMRULES API, e.g. "https://api.siemrules.com"
if not SIEMRULES_BASE_URL:
    print("ERROR: SIEMRULES_BASE_URL environment variable not set")
    sys.exit(1)
SIEMRULES_API_KEY = os.environ.get("SIEMRULES_API_KEY")
DETECTION_PACK_ID = os.environ.get("DETECTION_PACK_ID")  # Optional: Detection pack to add rules to
PROCESS_DEPRECATED = os.environ.get("PROCESS_DEPRECATED", "false").lower() in ["true", "1", "y", "yes"]  # Whether to process rules in 'deprecated' directory

MAX_WORKERS = int(os.environ.get("MAX_WORKERS", "10"))
STATUS_CHECK_INTERVAL = int(os.environ.get("STATUS_CHECK_INTERVAL", "10"))
MAX_STATUS_CHECKS = int(os.environ.get("MAX_STATUS_CHECKS", "180"))
LAST_COMMIT_FILE = os.environ.get("LAST_COMMIT_FILE", "artifacts/last_commit.txt")

# Empty tree commit ID for diffing from the beginning of the repository
EMPTY_TREE_SHA = "4b825dc642cb6eb9a060e54bf8d69288fbee4904"


def get_team_id() -> str:
    """
    Retrieves the team ID from SIEMRULES API using the provided API key

    Returns:
        The team ID as a string
    """
    url = f"{SIEMRULES_BASE_URL}/v1/team/"
    headers = {"API-KEY": SIEMRULES_API_KEY}
    response = requests.get(url, headers=headers, timeout=10)
    return "identity--"+ response.json()['details']['id']

IDENTITY_ID = get_team_id()
print(f"Retrieved team identity ID: {IDENTITY_ID}")

def calculate_rule_id(orig_id: str) -> str:
    NAMESPACE = uuid.UUID("8ef05850-cb0d-51f7-80be-50e4376dbe63")
    return "indicator--"+str(uuid.uuid5(NAMESPACE, f"{orig_id}+{IDENTITY_ID}"))

def rule_exists(orig_id: str, rule) -> bool:
    indicator_id = calculate_rule_id(orig_id)
    url = f"{SIEMRULES_BASE_URL}/v1/base-rules/{indicator_id}/versions/"
    headers = {"API-KEY": SIEMRULES_API_KEY}
    response = requests.get(url, headers=headers, timeout=10)
    return indicator_id, response.ok


def retrieve_files(repo_path: str, start_commit: str = None, end_commit: str = None) -> List[Path]:
    """
    Gets .yml/.yaml files from repo.
    If start_commit is provided, gets files changed between start_commit and end_commit (or HEAD).
    If start_commit is not provided, gets all files from end_commit (or HEAD).

    Args:
        repo_path: Path to the git repository
        start_commit: Start commit ID to compare from (optional)
        end_commit: End commit ID to compare to (optional, defaults to HEAD)

    Returns:
        List of Path objects for YAML files
    """
    try:
        repo = Repo(repo_path)
        end_ref = repo.commit(end_commit or "HEAD")  # Ensure we have a commit object for end_ref

        # Mode 1: Diff from start_commit to end_commit
        print(f"Retrieving files changed from commit {start_commit} to {end_commit or 'HEAD'}")
        diffs = end_ref.diff(start_commit)

        changed_files = []
        for diff in diffs:
            # Check if file is added or modified (not deleted)
            if diff.a_path and (
                diff.a_path.endswith(".yml") or diff.a_path.endswith(".yaml")
            ):
                file_path = Path(repo_path) / diff.a_path
                if file_path.exists():
                    changed_files.append(file_path)
                    print(f"  Found (changed): {diff.a_path}")

        print(f"Total changed YAML files found: {len(changed_files)}")
        return changed_files
    except Exception as e:
        print(f"Error retrieving files: {e}")
        return []


def upload_file(file_path: Path, repo_path: str, commit_id: str = None) -> Dict:
    """
    Uploads a YAML file to SIEMRULES API

    Args:
        file_path: Path to the YAML file
        repo_path: Path to the repository root
        commit_id: Commit ID to associate with the upload (optional)

    Returns:
        Dict with upload response containing job_id, file_id, status
    """
    print(f"Uploading: {file_path.name}")

    url = f"{SIEMRULES_BASE_URL}/v1/files/yml/"
    headers = {"API-KEY": SIEMRULES_API_KEY, "Content-Type": "application/yaml"}

    source_url = rewrite_path(file_path, repo_path, commit_id)

    try:
        data = yaml.safe_load(
            file_path.open("r")
        )  # Validate YAML format before uploading
        data.setdefault('references', []).append(source_url)  # Add source URL to references
        for k in ['date', 'modified']:
            if isinstance(data.get(k), str):
                data[k] = data[k].replace("/", "-")  # Replace slashes in date to avoid issues

        indicator_id, exists = rule_exists(data['id'], data)
        response = None
        if exists: # modify existing rule
            url = f"{SIEMRULES_BASE_URL}/v1/base-rules/{indicator_id}/modify/yml/"
            print(f"  ⚠️ Rule with ID {data['id']} already exists ({indicator_id}). Attempting to modify existing rule.")
            for key in ['id', 'author', 'date', 'modified']:
                data.pop(key, None)  # Remove keys if they exist to avoid conflicts
        data_str = yaml.dump(data)  # Convert back to string for upload
        response = requests.post(url, headers=headers, data=data_str, timeout=30)

        if not response.ok:
            raise Exception(
                f"HTTP {response.status_code} - {response.text}"
            )  # Include response text for debugging

        result = response.json()
        result["source_url"] = source_url
        print(f"  ✓ Uploaded: {file_path.name} (Job ID: {result.get('id')})")
        return result

    except Exception as e:
        print(f"  ✗ Upload failed for {file_path.name}: {str(e)[:200]}")  # Truncate error message for readability
        return {"source_url": source_url, "error": str(e), "status": "upload_failed"}


def check_job_status(job_id: str) -> Dict:
    """
    Checks the status of a job

    Args:
        job_id: Job ID to check

    Returns:
        Dict with job status information
    """
    url = f"{SIEMRULES_BASE_URL}/v1/jobs/{job_id}/"
    headers = {
        "API-KEY": SIEMRULES_API_KEY,
    }

    try:
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()
        return response.json()

    except Exception as e:
        print(f"  Error checking job {job_id}: {e}")
        return {"id": job_id, "status": "check_failed", "error": str(e)}


def check_all_statuses(jobs: List[Dict]) -> Tuple[List[Dict], List[Dict]]:
    """
    Checks status of all jobs concurrently, waiting for completion

    Args:
        jobs: List of job dictionaries from upload_file

    Returns:
        Tuple of (succeeded_jobs, failed_jobs)
    """
    print(f"\nMonitoring {len(jobs)} jobs...")

    # Filter out jobs that failed to upload
    valid_jobs = [j for j in jobs if "id" in j and j.get("status") != "upload_failed"]
    failed_uploads = [j for j in jobs if j.get("status") == "upload_failed"]

    if not valid_jobs:
        print("No valid jobs to monitor")
        return [], failed_uploads

    pending_jobs = {job["id"]: job for job in valid_jobs}
    succeeded = []
    failed = []

    for check_round in range(MAX_STATUS_CHECKS):
        if not pending_jobs:
            break

        print(f"\nStatus check round {check_round + 1}/{MAX_STATUS_CHECKS}")
        print(f"  Pending jobs: {len(pending_jobs)}")

        # Check all pending jobs concurrently
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            future_to_job = {
                executor.submit(check_job_status, job_id): job_id
                for job_id in pending_jobs.keys()
            }

            for future in as_completed(future_to_job):
                job_id = future_to_job[future]
                original_job = pending_jobs[job_id]

                try:
                    status_result = future.result()
                    current_status = status_result.get("state", "unknown")

                    if current_status == "completed":
                        print(f"  ✓ Job {job_id} succeeded")
                        succeeded.append({**original_job, **status_result})
                        del pending_jobs[job_id]

                    elif current_status == "failed":
                        print(f"  ✗ Job {job_id} failed")
                        failed.append({**original_job, **status_result})
                        del pending_jobs[job_id]

                    elif current_status in ["pending", "processing"]:
                        # Still processing, keep in pending
                        pass
                    else:
                        print(f"  ? Job {job_id} has unknown status: {current_status}")

                except Exception as e:
                    print(f"  Error processing job {job_id}: {e}")

        # Wait before next check if there are still pending jobs
        if pending_jobs:
            time.sleep(STATUS_CHECK_INTERVAL)

    # Any remaining pending jobs are considered timed out
    for job_id, job_data in pending_jobs.items():
        print(f"  ⏱ Job {job_id} timed out")
        failed.append(
            {**job_data, "status": "timeout", "error": "Status check timeout"}
        )

    # Include upload failures in failed list
    failed.extend(failed_uploads)

    return succeeded, failed


def save_last_commit(commit_sha: str, output_file: str):
    """
    Saves the last processed commit SHA to a file

    Args:
        commit_sha: The commit SHA to save
        output_file: Path to the file to save the commit SHA
    """
    output_path = Path(output_file)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with open(output_path, "w") as f:
        f.write(commit_sha)
    print(f"\n✓ Saved last commit SHA to {output_file}: {commit_sha}")


def rewrite_path(file_path: str, repo_path: str, commit_id: str = None) -> str:
    """
    Rewrites file path to be relative to the repository root

    Args:
        file_path: Original file path
        repo_path: Path to the repository root
        commit_id: Commit ID to associate with the file path (optional)

    Returns:
        Rewritten file path
    """
    commit_id = commit_id or "HEAD"
    relative_path = f"blob/{commit_id}/" + str(Path(file_path).relative_to(repo_path))
    return urljoin(GITHUB_REPO_URL, relative_path)


def load_last_commit(input_file: str) -> str:
    """
    Loads the last processed commit SHA from a file

    Args:
        input_file: Path to the file containing the commit SHA

    Returns:
        The commit SHA or None if file doesn't exist
    """
    input_path = Path(input_file)
    if input_path.exists():
        with open(input_path, "r") as f:
            commit_sha = f.read().strip()
        print(f"Loaded last commit SHA from {input_file}: {commit_sha}")
        return commit_sha
    return None


def add_rules_to_detection_pack(succeeded_jobs: List[Dict], detection_pack_id: str) -> bool:
    """
    Adds successfully uploaded rules to a detection pack
    
    Args:
        succeeded_jobs: List of successful job results
        detection_pack_id: ID of the detection pack to add rules to
        
    Returns:
        True if successful, False otherwise
    """
    if not succeeded_jobs:
        print("No successful jobs to add to detection pack")
        return True
    
    # Extract file_ids from successful jobs
    rule_ids = []
    for job in succeeded_jobs:
        job = job.get('metadata', job)
        if job.get('file_id'):
            rule_ids.append(job['file_id'])
        elif job.get('extra') and job['extra'].get('indicator_id'):
            rule_ids.append(job['extra']['indicator_id'].rpartition("--")[-1])  # Extract rule ID from indicator ID
    
    if not rule_ids:
        print("No valid file_ids found in successful jobs")
        return False
    
    print(f"Adding {len(rule_ids)} rules to detection pack {detection_pack_id}")
    
    url = f"{SIEMRULES_BASE_URL}/v1/detection-packs/{detection_pack_id}/add-rules/"
    headers = {
        'API-KEY': SIEMRULES_API_KEY,
        'Content-Type': 'application/json'
    }
    payload = {"rule_ids": rule_ids}
    
    try:
        response = requests.post(url, headers=headers, json=payload, timeout=30)
        
        if response.ok:
            print(f"✓ Successfully added {len(rule_ids)} rules to detection pack")
            return True
        else:
            print(f"✗ Failed to add rules to detection pack")
            print(f"  Status Code: {response.status_code}")
            print(f"  Response: {response.text}")
            return False
            
    except Exception as e:
        print(f"✗ Exception while adding rules to detection pack: {e}")
        return False


def save_artifacts(succeeded: List[Dict], failed: List[Dict], output_dir: str = ".", detection_pack_success: bool = None, commit_sha: str = None):
    """
    Saves results to artifact files and writes GitHub Action summary

    Args:
        succeeded: List of successful job results
        failed: List of failed job results
        output_dir: Directory to save artifact files
        detection_pack_success: Whether adding to detection pack was successful (optional)
        commit_sha: Current commit SHA for badge generation (optional)
    """
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    # Save succeeded results
    succeeded_file = output_path / "succeeded_jobs.json"
    with open(succeeded_file, "w") as f:
        json.dump(succeeded, f, indent=2)
    print(f"\n✓ Saved {len(succeeded)} succeeded jobs to {succeeded_file}")

    # Save failed results
    failed_file = output_path / "failed_jobs.json"
    with open(failed_file, "w") as f:
        json.dump(failed, f, indent=2)
    print(f"✗ Saved {len(failed)} failed jobs to {failed_file}")

    # Save badge data
    badges_file = output_path / "badges.json"
    badge_data = {
        "last_commit": {
            "schemaVersion": 1,
            "label": "last commit",
            "message": commit_sha[:7] if commit_sha else "unknown",
            "color": "blue"
        },
        "results": {
            "schemaVersion": 1,
            "label": "rules",
            "message": f"{len(succeeded)} passed | {len(failed)} failed",
            "color": "red" if failed else "green"
        }
    }
    with open(badges_file, "w") as f:
        json.dump(badge_data, f, indent=2)
    print(f"✓ Saved badge data to {badges_file}")

    # Write GitHub Action summary if available
    summary_file = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary_file:
        with open(summary_file, "a") as f:
            f.write("# SIEMRULES Upload Summary\n\n")
            f.write(f"## Results\n\n")
            f.write(f"- ✅ **Succeeded**: {len(succeeded)}\n")
            f.write(f"- ❌ **Failed**: {len(failed)}\n")
            f.write(f"- 📊 **Total**: {len(succeeded) + len(failed)}\n\n")

            if succeeded:
                f.write("### ✅ Succeeded Jobs\n\n")
                for job in succeeded[:10]:  # Show first 10
                    file_name = job.get("source_url", "unknown").rpartition("/")[-1]
                    job_id = job.get("id", "N/A")
                    f.write(f"- `{file_name}` (Job ID: `{job_id}`)\n")
                if len(succeeded) > 10:
                    f.write(f"\n_... and {len(succeeded) - 10} more_\n")
                f.write("\n")

            if failed:
                f.write("### ❌ Failed Jobs\n\n")
                for job in failed[:10]:  # Show first 10
                    file_name = job.get("source_url", "unknown").rpartition("/")[-1]
                    error = job.get("error", "Unknown error")
                    f.write(f"- `{file_name}`: {error}\n")
                if len(failed) > 10:
                    f.write(f"\n_... and {len(failed) - 10} more_\n")
                f.write("\n")

            f.write(f"## Artifacts\n\n")
            f.write(f"- 📄 Succeeded jobs: `succeeded_jobs.json`\n")
            f.write(f"- 📄 Failed jobs: `failed_jobs.json`\n\n")
            
            if detection_pack_success is not None:
                f.write(f"## Detection Pack\n\n")
                if detection_pack_success:
                    f.write(f"✅ Successfully added {len(succeeded)} rules to detection pack\n")
                else:
                    f.write(f"❌ Failed to add rules to detection pack\n")

        print(f"✓ Updated GitHub Action summary")


def parse_args():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(
        description="Upload Sigma rules to SIEMRULES API"
    )
    parser.add_argument(
        "--start-commit",
        help="Start commit SHA (defaults to last_commit_id or empty tree if not available)",
        default=None
    )
    parser.add_argument(
        "--end-commit",
        help="End commit SHA (defaults to HEAD if not provided)",
        default=None
    )
    args = parser.parse_args()
        
    # Determine start_commit (with default logic)
    if not args.start_commit:
        # Try to load last commit from file
        last_commit_path = Path(LAST_COMMIT_FILE)
        if last_commit_path.exists():
            args.start_commit = load_last_commit(LAST_COMMIT_FILE)
            print(f"Loaded start commit from {LAST_COMMIT_FILE}: {args.start_commit}")
        else:
            # Default to empty tree (will diff from beginning)
            args.start_commit = EMPTY_TREE_SHA
            print(f"No last commit found, using empty tree: {args.start_commit}")
    
    # Determine end_commit (defaults to HEAD)
    args.end_commit = args.end_commit or "HEAD"
    return args


def main():
    """Main execution function"""
    args = parse_args()
    
    print("=" * 60)
    print("SIEMRULES Uploader")
    print("=" * 60)

    # Validate environment variables
    if not SIEMRULES_BASE_URL:
        print("ERROR: SIEMRULES_BASE_URL environment variable not set")
        sys.exit(1)

    if not SIEMRULES_API_KEY:
        print("ERROR: SIEMRULES_API_KEY environment variable not set")
        sys.exit(1)

    # Determine repository path
    temp_dir = None
    if not GITHUB_REPO_URL:
        print("ERROR: GITHUB_REPO_URL environment variable not set")
        sys.exit(1)

    if os.getenv("TESTING_LOCAL"):
        repo_path = "/home/fqrious/dev/dogesec/sigma2stix/server"
    else:
        print(f"\nCloning external repository: {GITHUB_REPO_URL}")
        temp_dir = tempfile.mkdtemp(prefix="sigma2stix_")
        try:
            Repo.clone_from(
                GITHUB_REPO_URL, temp_dir, depth=50
            )  # Shallow clone with some history
            repo_path = temp_dir
            print(f"  ✓ Cloned to: {temp_dir}")
        except Exception as e:
            print(f"  ✗ Failed to clone repository: {e}")
            if temp_dir and os.path.exists(temp_dir):
                shutil.rmtree(temp_dir)
            sys.exit(1)


    print(f"\nConfiguration:")
    print(f"  Repository: {repo_path}")
    print(f"  Start Commit: {args.start_commit}")
    print(f"  End Commit: {args.end_commit or 'HEAD'}")
    print(f"  API Base URL: {SIEMRULES_BASE_URL}")
    print(f"  Max Workers: {MAX_WORKERS}")
    print()

    try:
        # Step 1: Retrieve files
        files_to_process = retrieve_files(repo_path, args.start_commit, args.end_commit)
        if not PROCESS_DEPRECATED:
            files_to_process = [f for f in files_to_process if set(['unsupported', 'deprecated']).isdisjoint(f.parts)]  # Filter out unsupported directory

        if not files_to_process:
            print("\nNo YAML files found. Nothing to upload.")
            save_artifacts([], [], "artifacts")
            return
        # files_to_process = files_to_process[
        #     :6
        # ]  # Limit to first 10 files for processing

        # Get current commit SHA for saving later (use end_commit if provided, otherwise HEAD)
        repo = Repo(repo_path)
        if args.end_commit:
            current_commit_sha = repo.commit(args.end_commit).hexsha
        else:
            current_commit_sha = repo.head.commit.hexsha
        print(f"Current commit to save: {current_commit_sha}")

        # Step 2: Upload files
        print(f"\n{'=' * 60}")
        print("Uploading Files")
        print("=" * 60)


        upload_results = []
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = [
                executor.submit(upload_file, file_path, repo_path, current_commit_sha)
                for file_path in files_to_process
            ]
            for future in as_completed(futures):
                try:
                    result = future.result()
                    upload_results.append(result)
                except Exception as e:
                    print(f"Upload exception: {e}")

        # Step 3: Check status of all jobs
        print(f"\n{'=' * 60}")
        print("Checking Job Status")
        print("=" * 60)

        succeeded, failed = check_all_statuses(upload_results)

        # Step 4: Add rules to detection pack (if configured)
        detection_pack_success = None
        if DETECTION_PACK_ID and succeeded:
            print(f"\n{'=' * 60}")
            print("Adding Rules to Detection Pack")
            print("=" * 60)
            detection_pack_success = add_rules_to_detection_pack(succeeded, DETECTION_PACK_ID)
        elif DETECTION_PACK_ID:
            print(f"\nSkipping detection pack addition (no successful uploads)")
        
        # Step 5: Save artifacts
        print(f"\n{'=' * 60}")
        print("Saving Artifacts")
        print("=" * 60)

        save_artifacts(succeeded, failed, "artifacts", detection_pack_success, current_commit_sha)

        # Step 6: Save current commit SHA as last processed commit
        save_last_commit(current_commit_sha, LAST_COMMIT_FILE)

        # Summary
        print(f"\n{'=' * 60}")
        print("Summary")
        print("=" * 60)
        print(f"✅ Succeeded: {len(succeeded)}")
        print(f"❌ Failed: {len(failed)}")
        print(f"📊 Total: {len(succeeded) + len(failed)}")
        if detection_pack_success is not None:
            if detection_pack_success:
                print(f"📦 Detection Pack: ✅ Successfully added {len(succeeded)} rules")
            else:
                print(f"📦 Detection Pack: ❌ Failed to add rules")

        # Exit with error code if any jobs failed
        if failed:
            sys.exit(1)

    finally:
        # Clean up temporary directory if it was created
        if temp_dir and os.path.exists(temp_dir):
            print(f"\nCleaning up temporary directory: {temp_dir}")
            shutil.rmtree(temp_dir)


if __name__ == "__main__":
    main()
