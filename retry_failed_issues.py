"""
Zephyr Migration Script - RETRY FAILED ISSUES ONLY

This script processes ONLY the specified failed issue keys.
"""

import argparse
import csv
import hashlib
import json
import sys
import time
from collections import defaultdict
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple, Set

import requests

try:
    import jwt
except ImportError:
    print("❌ PyJWT not installed. Run: pip install PyJWT")
    sys.exit(1)


# List of failed issue keys to retry
FAILED_ISSUE_KEYS = {
    "GCTEST-94808",
    "GCTEST-95392",
    "GCTEST-95397",
    "GCTEST-96437",
    "GCTEST-94722",
    "GCTEST-95937"
}


# =====================================================================
# CONFIG
# =====================================================================

class Config:
    def __init__(self, data: Dict[str, Any]):
        self.jira_url = data["jira_url"].rstrip("/")
        self.jira_email = data["jira_email"]
        self.jira_token = data["jira_api_token"]
        self.zephyr_base = data["zephyr_base_url"].rstrip("/")
        self.access_key = data["zephyr_access_key"]
        self.secret_key = data["zephyr_secret_key"]
        self.account_id = data["zephyr_account_id"]
        self.rate_limit_delay = data.get("rate_limit_delay", 1.5)


def load_config(path: str) -> Config:
    with open(path, "r", encoding="utf-8") as f:
        return Config(json.load(f))


def load_user_mapping(path: str) -> Dict[str, str]:
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        return {}
    except Exception as e:
        print(f"⚠️ Warning: Could not load user mapping file: {e}")
        return {}


# =====================================================================
# HELPERS
# =====================================================================

def normalize_whitespace(s: str) -> str:
    return " ".join((s or "").strip().split())


def normalize_user_name(name: str) -> str:
    if not name:
        return ""
    name = name.replace("(Inactive)", "").replace("(inactive)", "")
    return normalize_whitespace(name)


# =====================================================================
# JWT + HTTP
# =====================================================================

def build_canonical_qsh(method: str, path: str, params: Optional[Dict[str, Any]]) -> str:
    method = method.upper()
    if params:
        query = "&".join([f"{k}={params[k]}" for k in sorted(params.keys())])
    else:
        query = ""
    return f"{method}&{path}&{query}"


def generate_zephyr_jwt(cfg: Config, canonical: str) -> str:
    payload = {
        "sub": cfg.account_id,
        "qsh": hashlib.sha256(canonical.encode("utf-8")).hexdigest(),
        "iss": cfg.access_key,
        "iat": int(time.time()),
        "exp": int(time.time()) + 3600,
    }
    token = jwt.encode(payload, cfg.secret_key, algorithm="HS256")
    return token.decode("utf-8") if isinstance(token, bytes) else token


def jira_request(cfg: Config, method: str, path: str, params: Optional[Dict[str, Any]] = None,
                 json_body: Any = None, max_retries: int = 3) -> Optional[requests.Response]:
    url = f"{cfg.jira_url}{path}"
    headers = {"Accept": "application/json", "Content-Type": "application/json"}

    for attempt in range(max_retries):
        try:
            return requests.request(method.upper(), url, auth=(cfg.jira_email, cfg.jira_token),
                                  headers=headers, params=params, json=json_body, timeout=25)
        except requests.exceptions.Timeout:
            if attempt < max_retries - 1:
                wait = (attempt + 1) * 2
                print(f"      ⏱️ Jira timeout, retrying in {wait}s...")
                time.sleep(wait)
            else:
                return None
        except Exception as e:
            print(f"      ❌ Jira error: {e!r}")
            return None
    return None


def zephyr_request(cfg: Config, method: str, path: str, params: Optional[Dict[str, Any]] = None,
                   json_body: Any = None, max_retries: int = 3) -> Optional[requests.Response]:
    canonical = build_canonical_qsh(method, path, params or {})
    token = generate_zephyr_jwt(cfg, canonical)

    url = f"{cfg.zephyr_base}{path}"
    headers = {"Authorization": f"JWT {token}", "zapiAccessKey": cfg.access_key,
               "Content-Type": "application/json"}

    for attempt in range(max_retries):
        try:
            return requests.request(method.upper(), url, headers=headers, params=params,
                                  json=json_body, timeout=25)
        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError):
            if attempt < max_retries - 1:
                wait = (attempt + 1) * 2
                print(f"      ⏱️ Zephyr error, retrying in {wait}s...")
                time.sleep(wait)
            else:
                return None
        except Exception as e:
            print(f"      ❌ Zephyr unexpected error: {e!r}")
            return None
    return None


# =====================================================================
# JIRA / ZEPHYR LOOKUPS
# =====================================================================

def get_project_id(cfg: Config, project_key: str) -> int:
    resp = jira_request(cfg, "GET", f"/rest/api/2/project/{project_key}")
    if resp and resp.status_code == 200:
        return int(resp.json()["id"])
    print(f"❌ Failed to fetch project {project_key}")
    sys.exit(1)


def get_versions_map(cfg: Config, project_key: str) -> Dict[str, int]:
    resp = jira_request(cfg, "GET", f"/rest/api/2/project/{project_key}/versions")
    mapping: Dict[str, int] = {}
    if resp and resp.status_code == 200:
        for v in resp.json():
            if v.get("name") and v.get("id"):
                mapping[v["name"]] = int(v["id"])
    mapping["Unscheduled"] = -1
    return mapping


def get_components_map(cfg: Config, project_key: str) -> Dict[str, str]:
    """Get all components for a project. Returns {component_name: component_id}"""
    resp = jira_request(cfg, "GET", f"/rest/api/2/project/{project_key}/components")
    mapping: Dict[str, str] = {}
    if resp and resp.status_code == 200:
        for c in resp.json():
            if c.get("name") and c.get("id"):
                mapping[c["name"]] = str(c["id"])
    return mapping


def get_or_create_component_id(cfg: Config, project_key: str, components_map: Dict[str, str],
                               component_name: str) -> Optional[str]:
    """Get or create a component, returns component ID"""
    if not component_name or not component_name.strip():
        return None
    
    component_name = component_name.strip()
    
    # Check if already exists
    if component_name in components_map:
        return components_map[component_name]
    
    # Create new component
    body = {"name": component_name, "project": project_key,
            "description": "Created by migration"}
    
    resp = jira_request(cfg, "POST", "/rest/api/2/component", json_body=body)
    if resp and resp.status_code in (200, 201):
        try:
            comp_id = str(resp.json()["id"])
            components_map[component_name] = comp_id
            return comp_id
        except Exception:
            return None
    
    return None


def update_issue_component(cfg: Config, issue_key: str, component_id: str) -> bool:
    """Update the component field on a Jira issue"""
    if not component_id:
        return True
    
    body = {"fields": {"components": [{"id": component_id}]}}
    
    resp = jira_request(cfg, "PUT", f"/rest/api/2/issue/{issue_key}", json_body=body)
    return resp and resp.status_code in (200, 204)


def get_issue_id(cfg: Config, issue_key: str, cache: Dict[str, Optional[str]]) -> Optional[str]:
    if issue_key in cache:
        return cache[issue_key]

    resp = jira_request(cfg, "GET", f"/rest/api/2/issue/{issue_key}", params={"fields": "id"})
    if resp and resp.status_code == 200:
        iid = resp.json()["id"]
        cache[issue_key] = iid
        return iid

    print(f"   ⚠️  Jira issue not found: {issue_key}")
    cache[issue_key] = None
    return None


def get_all_users(cfg: Config) -> List[Dict[str, Any]]:
    all_users = []
    start_at = 0
    max_results = 100
    
    while True:
        resp = jira_request(cfg, "GET", "/rest/api/3/users/search",
                          params={"startAt": start_at, "maxResults": max_results})
        
        if not resp or resp.status_code != 200:
            break
            
        users = resp.json()
        if not users:
            break
            
        all_users.extend(users)
        
        if len(users) < max_results:
            break
            
        start_at += max_results
    
    return all_users


def get_existing_test_steps(cfg: Config, project_id: int, issue_id: str) -> List[Dict[str, Any]]:
    path = f"/public/rest/api/1.0/teststep/{issue_id}"
    params = {"projectId": project_id}
    
    resp = zephyr_request(cfg, "GET", path, params=params)
    
    if resp and resp.status_code == 200:
        try:
            data = resp.json()
            if isinstance(data, dict):
                return list(data.values())
            return data if isinstance(data, list) else []
        except Exception:
            return []
    return []


def get_user_account_id(cfg: Config, name: str, cache: Dict[str, Optional[str]],
                       user_mapping: Dict[str, str], all_users: List[Dict[str, Any]]) -> Optional[str]:
    if not name:
        return None

    original = normalize_user_name(name)

    if original in cache:
        return cache[original]

    # Check user mapping
    if original in user_mapping:
        account_id = user_mapping[original]
        cache[original] = account_id
        return account_id
    
    if name in user_mapping:
        account_id = user_mapping[name]
        cache[original] = account_id
        return account_id

    # Search in all_users
    for u in all_users:
        display = normalize_whitespace(u.get("displayName", ""))
        if display.lower() == original.lower():
            account_id = u.get("accountId")
            cache[original] = account_id
            return account_id

    cache[original] = None
    return None


def get_or_create_version_id(cfg: Config, project_id: int, versions_map: Dict[str, int],
                            version_name: str) -> int:
    if not version_name or not version_name.strip() or version_name.lower() == "unscheduled":
        return -1

    version_name = version_name.strip()

    if version_name in versions_map:
        return versions_map[version_name]

    body = {"name": version_name, "projectId": project_id,
            "description": "Created by migration", "released": False}

    resp = jira_request(cfg, "POST", "/rest/api/2/version", json_body=body)
    if resp and resp.status_code in (200, 201):
        try:
            vid = int(resp.json()["id"])
            versions_map[version_name] = vid
            return vid
        except Exception:
            return -1

    return -1


# =====================================================================
# EXECUTION HELPERS
# =====================================================================

STATUS_MAP = {"PASS": 1, "FAIL": 2, "WIP": 3, "BLOCKED": 4, "UNEXECUTED": -1}


def get_or_create_cycle_id(cfg: Config, project_id: int, version_id: int, cycle_name: str,
                          cache: Dict[Tuple[str, int], Optional[str]]) -> Optional[str]:
    if not cycle_name:
        cycle_name = "Ad hoc"

    key = (cycle_name, version_id)
    if key in cache:
        return cache[key]

    body = {"name": cycle_name, "projectId": project_id, "versionId": version_id,
            "description": "Created by migration"}

    resp = zephyr_request(cfg, "POST", "/public/rest/api/1.0/cycle", json_body=body)
    if resp and resp.status_code in (200, 201):
        cid = str(resp.json().get("id") or resp.json().get("cycleId"))
        cache[key] = cid
        return cid

    cache[key] = None
    return None


def get_or_create_folder_id(cfg: Config, project_id: int, version_id: int, cycle_id: str,
                           folder_name: str, cache: Dict) -> Optional[str]:
    if not folder_name or not folder_name.strip():
        return None

    key = (folder_name, project_id, version_id, cycle_id)
    if key in cache:
        return cache[key]

    # Fetch existing folders
    list_key = (project_id, version_id, cycle_id)
    if not hasattr(get_or_create_folder_id, "_folders_cache"):
        get_or_create_folder_id._folders_cache = {}
    folders_cache = get_or_create_folder_id._folders_cache

    if list_key not in folders_cache:
        params = {"projectId": project_id, "versionId": version_id, "cycleId": cycle_id}
        resp = zephyr_request(cfg, "GET", "/public/rest/api/1.0/folders", params=params)
        if resp and resp.status_code == 200:
            try:
                folders_cache[list_key] = resp.json()
            except Exception:
                folders_cache[list_key] = []
        else:
            folders_cache[list_key] = []

    # Find existing folder
    folder_id = None
    for f in folders_cache[list_key]:
        if f.get("name", "").strip().lower() == folder_name.strip().lower():
            folder_id = str(f.get("id"))
            break

    # Create if not found
    if not folder_id:
        body = {"name": folder_name, "projectId": project_id, "versionId": version_id,
                "cycleId": cycle_id, "description": folder_name}
        resp = zephyr_request(cfg, "POST", "/public/rest/api/1.0/folder", json_body=body)
        if resp and resp.status_code in (200, 201):
            try:
                data = resp.json()
                folder_id = str(data.get("id") or data.get("folderId"))
                folders_cache[list_key].append(data)
            except Exception:
                pass

    cache[key] = folder_id
    return folder_id


def parse_date_to_millis(date_str: str) -> Optional[int]:
    if not date_str or not date_str.strip():
        return None

    date_str = date_str.strip()
    fmts = ["%m-%d-%Y %H:%M:%S", "%Y-%m-%d %H:%M:%S", "%d/%b/%y %I:%M %p",
            "%d/%b/%Y %H:%M", "%Y-%m-%d", "%m/%d/%Y %H:%M:%S", "%d-%m-%Y %H:%M:%S",
            "%d/%b/%y %I:%M %p", "%d/%b/%y %H:%M"]
    
    for fmt in fmts:
        try:
            dt = datetime.strptime(date_str, fmt)
            return int(dt.timestamp() * 1000)
        except Exception:
            continue

    return None


def create_execution(cfg: Config, project_id: int, version_id: int, cycle_id: str,
                    folder_id: Optional[str], issue_id: str) -> Optional[str]:
    path = "/public/rest/api/1.0/execution"

    body: Dict[str, Any] = {"issueId": int(issue_id), "projectId": project_id,
                           "versionId": version_id, "cycleId": cycle_id}
    if folder_id:
        body["folderId"] = folder_id

    resp = zephyr_request(cfg, "POST", path, json_body=body)
    
    if not resp:
        print(f"      ❌ No response from Zephyr API")
        return None
    
    if resp.status_code not in (200, 201):
        print(f"      ❌ Zephyr API error: {resp.status_code}")
        try:
            print(f"      ❌ Response: {resp.text[:200]}")
        except:
            pass
        return None

    try:
        data = resp.json()
        exec_id = data.get("execution", {}).get("id") or data.get("id")
        if not exec_id:
            print(f"      ❌ No execution ID in response: {data}")
            return None
        return str(exec_id)
    except Exception as e:
        print(f"      ❌ Failed to parse response: {e}")
        return None


def add_execution_comment(cfg: Config, execution_id: str, comment_text: str) -> bool:
    """Add a comment to an execution using the separate comment endpoint"""
    if not comment_text or not comment_text.strip():
        return True
    
    path = f"/public/rest/api/1.0/execution/{execution_id}/comment"
    body = {"comment": comment_text.strip()}
    
    resp = zephyr_request(cfg, "POST", path, json_body=body)
    return resp and resp.status_code in (200, 201)


def delete_execution(cfg: Config, execution_id: str, issue_id: str) -> bool:
    """Delete an execution if it failed to be updated properly."""
    path = f"/public/rest/api/1.0/execution/{execution_id}"
    params = {"issueId": issue_id}
    
    print(f"      🗑️  Attempting to DELETE execution {execution_id}...")
    resp = zephyr_request(cfg, "DELETE", path, params=params)
    
    if not resp:
        print(f"      ❌ DELETE failed: No response")
        return False
    
    if resp.status_code in (200, 204):
        print(f"      ✅ DELETE successful")
        return True
    else:
        print(f"      ❌ DELETE failed: {resp.status_code}")
        try:
            print(f"      ❌ Response: {resp.text[:200]}")
        except:
            pass
        return False


def execute_execution(cfg: Config, execution_id: str, issue_id: str, project_id: int,
                     version_id: int, cycle_id: str, exec_data: Dict[str, Any],
                     executed_by_id: Optional[str], assigned_to_id: Optional[str]) -> bool:
    path = f"/public/rest/api/1.0/execution/{execution_id}"
    params = {"projectId": project_id, "issueId": issue_id}

    status_key = STATUS_MAP.get(exec_data["status"].upper(), -1)

    body: Dict[str, Any] = {"status": {"id": status_key}, "projectId": project_id,
                           "versionId": version_id, "cycleId": cycle_id, "issueId": int(issue_id)}

    millis = parse_date_to_millis(exec_data.get("executed_on", ""))
    if millis:
        body["executedOn"] = millis

    if executed_by_id:
        body["executedBy"] = executed_by_id

    if assigned_to_id:
        body["assignedTo"] = assigned_to_id

    resp = zephyr_request(cfg, "PUT", path, params=params, json_body=body)
    return resp and resp.status_code in (200, 204)


# =====================================================================
# TEST STEP HELPERS
# =====================================================================

def create_test_step(cfg: Config, project_id: int, issue_id: str, position: int,
                    step_text: str, data_text: str, result_text: str) -> bool:
    path = f"/public/rest/api/1.0/teststep/{issue_id}"
    params = {"projectId": project_id}

    body = {"step": step_text or "", "data": data_text or "", "result": result_text or "",
            "orderId": position}

    resp = zephyr_request(cfg, "POST", path, params=params, json_body=body)
    return resp and resp.status_code in (200, 201)


def sync_steps_for_issue(cfg: Config, project_id: int, issue_id: str, issue_key: str,
                        steps: List[Dict[str, str]]) -> None:
    if not steps:
        return

    # Check if steps already exist
    existing_steps = get_existing_test_steps(cfg, project_id, issue_id)
    
    if existing_steps:
        print(f"   ℹ️  {issue_key} already has {len(existing_steps)} test step(s), skipping")
        return

    print(f"   🧩 Creating {len(steps)} test step(s) for {issue_key}...")
    for idx, s in enumerate(steps, start=1):
        ok = create_test_step(cfg, project_id, issue_id, idx, s.get("step", ""),
                            s.get("data", ""), s.get("result", ""))
        if ok:
            print(f"      ✅ Step {idx} created")


# =====================================================================
# CSV LOADING
# =====================================================================

def pick(row: Dict[str, str], *names: str, default: str = "") -> str:
    for n in names:
        for key in row.keys():
            if key.strip().lower() == n.strip().lower():
                val = (row[key] or "").strip()
                if val:
                    return val
    return default


def load_executions_from_csv(csv_path: str, retry_issue_keys: Set[str]) -> Dict[str, Dict[str, Any]]:
    """
    Load CSV and filter ONLY the failed issue keys.
    Groups by ExecutionId to avoid duplicates.
    """
    executions: Dict[str, Dict[str, Any]] = {}
    
    with open(csv_path, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for raw in reader:
            issue_key = pick(raw, "issue key", "issuekey")
            if not issue_key:
                continue
            
            # ONLY process if in failed list
            if issue_key not in retry_issue_keys:
                continue

            execution_id = pick(raw, "executionid", "execution id", default="")
            
            # Create unique key
            if execution_id:
                unique_key = execution_id
            else:
                cycle = pick(raw, "cyclename", "test cycle", default="Ad hoc")
                folder = pick(raw, "foldername", "folder", default="")
                unique_key = f"{issue_key}_{cycle}_{folder}"

            # Initialize execution if not seen
            if unique_key not in executions:
                executions[unique_key] = {
                    "issue_key": issue_key,
                    "cycle_name": pick(raw, "cyclename", "test cycle", default="Ad hoc"),
                    "folder_name": pick(raw, "foldername", "folder", default=""),
                    "version_name": pick(raw, "version", "fixversion", default="Unscheduled"),
                    "component_name": pick(raw, "component", default=""),
                    "status": pick(raw, "executionstatus", "status", default="UNEXECUTED"),
                    "executed_on": pick(raw, "executed on", "executedon", default=""),
                    "executed_by": pick(raw, "executed by", "executedby", default=""),
                    "assigned_to": pick(raw, "assigned to", "assignedto", default=""),
                    "comment": pick(raw, "comments", "comment", default=""),
                    "steps": []
                }

            # Add step if present
            step_text = pick(raw, "step", "teststep", "test step", default="")
            if step_text:
                step_data = pick(raw, "test data", "data", default="")
                expected_result = pick(raw, "expected result", "test result", "result", default="")
                
                result_parts = []
                if expected_result:
                    result_parts.append(f"Expected: {expected_result}")
                result_text = "\n".join(result_parts) if result_parts else ""
                
                executions[unique_key]["steps"].append({
                    "step": step_text,
                    "data": step_data,
                    "result": result_text
                })

    return executions


def generate_failure_report(failed_items: List[Dict[str, Any]], output_path: str = "retry_failed_executions.csv"):
    if not failed_items:
        print("\n✅ No failures to report!")
        return
    
    all_keys = set()
    for item in failed_items:
        all_keys.update(item.keys())
    
    fieldnames = sorted(all_keys)
    
    with open(output_path, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(failed_items)
    
    print(f"\n📄 Failure report generated: {output_path}")
    print(f"   Total failures: {len(failed_items)}")


# =====================================================================
# MAIN MIGRATION
# =====================================================================

def migrate_executions(csv_path: str, cfg: Config, target_project_key: str,
                      user_mapping_path: Optional[str] = None) -> None:
    print("=" * 70)
    print("ZEPHYR SQUAD – RETRY FAILED ISSUES ONLY")
    print("=" * 70)
    print(f"CSV:     {csv_path}")
    print(f"Project: {target_project_key}")
    print(f"Processing {len(FAILED_ISSUE_KEYS)} failed issue keys")
    print("=" * 70)

    user_mapping = load_user_mapping(user_mapping_path) if user_mapping_path else {}
    if user_mapping:
        print(f"✅ Loaded {len(user_mapping)} user mappings\n")

    project_id = get_project_id(cfg, target_project_key)
    versions_map = get_versions_map(cfg, target_project_key)
    components_map = get_components_map(cfg, target_project_key)
    
    # Load executions (ONLY failed keys)
    executions = load_executions_from_csv(csv_path, FAILED_ISSUE_KEYS)

    print(f"✅ Project ID: {project_id}")
    print(f"✅ Versions loaded: {len(versions_map)}")
    print(f"✅ Components loaded: {len(components_map)}")
    print(f"✅ Unique executions to create: {len(executions)}\n")
    
    print(f"⏳ Fetching all Jira users...")
    all_users = get_all_users(cfg)
    print(f"✅ Found {len(all_users)} Jira users\n")

    # Caches
    issue_id_cache: Dict[str, Optional[str]] = {}
    cycle_cache: Dict[Tuple[str, int], Optional[str]] = {}
    folder_cache: Dict = {}
    user_cache: Dict[str, Optional[str]] = {}
    steps_created_for_issue: Set[str] = set()

    total = len(executions)
    created = 0
    failed = 0
    failed_items: List[Dict[str, Any]] = []

    for idx, (exec_id, exec_data) in enumerate(executions.items(), start=1):
        issue_key = exec_data["issue_key"]
        print(f"\n[{idx}/{total}] {issue_key} – {exec_data['status']} "
              f"cycle='{exec_data['cycle_name']}' folder='{exec_data['folder_name']}'")

        issue_id = get_issue_id(cfg, issue_key, issue_id_cache)
        if not issue_id:
            failed += 1
            failed_items.append({**exec_data, "failure_reason": "Issue not found"})
            continue

        # Update component if specified
        if exec_data.get("component_name"):
            component_id = get_or_create_component_id(cfg, target_project_key, components_map,
                                                     exec_data["component_name"])
            if component_id:
                update_issue_component(cfg, issue_key, component_id)
                print(f"   📦 Set component: {exec_data['component_name']}")

        # Create test steps once per issue
        if issue_key not in steps_created_for_issue and exec_data["steps"]:
            sync_steps_for_issue(cfg, project_id, issue_id, issue_key, exec_data["steps"])
            steps_created_for_issue.add(issue_key)

        version_id = get_or_create_version_id(cfg, project_id, versions_map, exec_data["version_name"])
        cycle_id = get_or_create_cycle_id(cfg, project_id, version_id, exec_data["cycle_name"], cycle_cache)
        
        if not cycle_id:
            failed += 1
            failed_items.append({**exec_data, "failure_reason": "Could not create cycle"})
            continue

        folder_id = None
        if exec_data["folder_name"]:
            folder_id = get_or_create_folder_id(cfg, project_id, version_id, cycle_id,
                                               exec_data["folder_name"], folder_cache)

        # Resolve users
        executed_by_id = None
        if exec_data["executed_by"]:
            executed_by_id = get_user_account_id(cfg, exec_data["executed_by"], user_cache,
                                                user_mapping, all_users)

        assigned_to_id = None
        if exec_data["assigned_to"]:
            assigned_to_id = get_user_account_id(cfg, exec_data["assigned_to"], user_cache,
                                                user_mapping, all_users)

        # Create execution
        new_exec_id = create_execution(cfg, project_id, version_id, cycle_id, folder_id, issue_id)
        if not new_exec_id:
            failed += 1
            failed_items.append({**exec_data, "failure_reason": "Could not create execution"})
            continue

        # Execute it (set status, assignee, etc)
        ok = execute_execution(cfg, new_exec_id, issue_id, project_id, version_id, cycle_id,
                              exec_data, executed_by_id, assigned_to_id)
        if ok:
            # Add comment separately if present
            if exec_data.get("comment"):
                comment_ok = add_execution_comment(cfg, new_exec_id, exec_data["comment"])
                if comment_ok:
                    print(f"   💬 Added comment")
                else:
                    print(f"   ⚠️  Could not add comment")
            
            created += 1
            print(f"   ✅ Created execution: {new_exec_id}")
        else:
            # CRITICAL FIX: Delete the UNEXECUTED execution if we failed to set status
            print(f"   ⚠️  Failed to execute, deleting UNEXECUTED execution {new_exec_id}...")
            delete_execution(cfg, new_exec_id, issue_id)
            failed += 1
            failed_items.append({**exec_data, "failure_reason": "Could not execute execution (deleted)",
                               "execution_id": new_exec_id})

        time.sleep(cfg.rate_limit_delay)

    # Generate failure report
    if failed_items:
        generate_failure_report(failed_items)

    print("\n" + "=" * 70)
    print("RETRY MIGRATION COMPLETE")
    print("=" * 70)
    print(f"Total executions processed: {total}")
    print(f"✅ Created & executed: {created}")
    print(f"❌ Failed: {failed}")
    print("=" * 70)


def main():
    parser = argparse.ArgumentParser(description="Retry failed Zephyr executions")
    parser.add_argument("--csv", required=True, help="Path to CSV")
    parser.add_argument("--config", required=True, help="Path to config")
    parser.add_argument("--target-project-key", required=True, help="Target project (GCTEST)")
    parser.add_argument("--user-mapping", help="Path to user mapping JSON")
    
    args = parser.parse_args()

    cfg = load_config(args.config)
    migrate_executions(args.csv, cfg, args.target_project_key,
                      user_mapping_path=args.user_mapping)


if __name__ == "__main__":
    main()