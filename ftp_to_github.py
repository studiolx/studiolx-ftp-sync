"""
ftp_to_github.py
Downloads inventory files from vendor FTP servers and commits them
directly to this GitHub repository. Google Apps Script then fetches
the raw file URL — no Drive permissions needed.
"""

import ftplib
import io
import os
import base64
import json
import csv
import urllib.request
import urllib.error

# ── CONFIGURATION ─────────────────────────────────────────────────────────────

def excel_to_csv(file_bytes, filename):
    """Convert Excel file bytes to CSV string. Requires openpyxl."""
    try:
        import openpyxl
        wb = openpyxl.load_workbook(io.BytesIO(file_bytes), read_only=True, data_only=True)
        ws = wb.active
        output = io.StringIO()
        writer = csv.writer(output)
        for row in ws.iter_rows(values_only=True):
            writer.writerow(['' if v is None else str(v) for v in row])
        wb.close()
        return output.getvalue().encode('utf-8')
    except Exception as e:
        print(f'  Excel conversion failed: {e} — uploading as-is')
        return file_bytes


VENDORS = [
    {
        'name': 'SW Corp (Anzzi / Spa World / Meditub)',
        'ftp_host': 'c122276.sgvps.net',
        'ftp_port': 21,
        'ftp_user': 'swinventory@swcorp.com',
        'ftp_pass': os.environ['FTP_SWCORP_PASS'],
        'ftp_path': '/SW Inventory Feed_Private Dealer.csv',
        'repo_path': 'inventory/swcorp.csv',  # path in the GitHub repo
    },
    # Add Eglo and Nourison here when credentials are available:
    # {
    #     'name': 'Eglo',
    #     'ftp_host': '',
    #     'ftp_port': 21,
    #     'ftp_user': '',
    #     'ftp_pass': os.environ.get('FTP_EGLO_PASS', ''),
    #     'ftp_path': '/inventory.csv',
    #     'repo_path': 'inventory/eglo.csv',
    # },
    {
        'name': 'Nourison',
        'ftp_host': 'b2b.nourison.net',
        'ftp_port': 21,
        'ftp_user': '100559',
        'ftp_pass': os.environ['FTP_NOURISON_PASS'],
        'ftp_path': '/100559',
        'repo_path': 'inventory/nourison.csv',
        'convert_excel': True,  # Convert Excel to CSV before committing
    },
]

GITHUB_TOKEN  = os.environ['GITHUB_TOKEN']
GITHUB_REPO   = os.environ['GITHUB_REPOSITORY']  # e.g. "yourusername/studiolx-ftp-sync"
GITHUB_BRANCH = 'main'

# ── GITHUB API HELPERS ────────────────────────────────────────────────────────

def github_request(method, path, data=None):
    """Make a GitHub API request."""
    url = f'https://api.github.com{path}'
    headers = {
        'Authorization': f'token {GITHUB_TOKEN}',
        'Accept': 'application/vnd.github.v3+json',
        'Content-Type': 'application/json',
    }
    body = json.dumps(data).encode() if data else None
    req = urllib.request.Request(url, data=body, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req) as resp:
            return json.loads(resp.read())
    except urllib.error.HTTPError as e:
        body = e.read().decode()
        raise Exception(f'GitHub API {method} {path} failed: {e.code} {body}')


def get_file_sha(repo_path):
    """Get the SHA of an existing file (needed for updates). Returns None if file doesn't exist."""
    try:
        result = github_request('GET', f'/repos/{GITHUB_REPO}/contents/{repo_path}')
        return result.get('sha')
    except Exception:
        return None


def commit_file(repo_path, content_bytes, commit_message):
    """Create or update a file in the GitHub repo."""
    sha = get_file_sha(repo_path)
    content_b64 = base64.b64encode(content_bytes).decode()
    data = {
        'message': commit_message,
        'content': content_b64,
        'branch': GITHUB_BRANCH,
    }
    if sha:
        data['sha'] = sha  # Required for updates
    github_request('PUT', f'/repos/{GITHUB_REPO}/contents/{repo_path}', data)


# ── FTP DOWNLOAD ──────────────────────────────────────────────────────────────

def download_from_ftp(host, port, user, password, remote_path):
    """Connect to FTP server and download a file. Returns bytes."""
    print(f'  Connecting to FTP {host}:{port} as {user}...')
    buf = io.BytesIO()
    with ftplib.FTP() as ftp:
        ftp.connect(host, port, timeout=30)
        ftp.login(user, password)
        ftp.set_pasv(True)
        print(f'  Downloading: {remote_path}')
        ftp.retrbinary(f'RETR {remote_path}', buf.write)
    size_kb = buf.tell() / 1024
    print(f'  Downloaded {size_kb:.1f} KB')
    return buf.getvalue()


# ── MAIN ──────────────────────────────────────────────────────────────────────

def main():
    from datetime import datetime
    print('=== FTP → GitHub Inventory Sync ===')
    print()

    success = 0
    failed  = 0

    for vendor in VENDORS:
        print(f'── {vendor["name"]} ──')
        try:
            # Download from FTP
            file_bytes = download_from_ftp(
                vendor['ftp_host'],
                vendor['ftp_port'],
                vendor['ftp_user'],
                vendor['ftp_pass'],
                vendor['ftp_path'],
            )

            # Convert Excel to CSV if needed
            if vendor.get('convert_excel') and not vendor['ftp_path'].endswith('.csv'):
                print(f'  Converting Excel to CSV...')
                file_bytes = excel_to_csv(file_bytes, vendor['ftp_path'])

            # Commit to GitHub repo
            commit_msg = f'inventory: update {vendor["name"]} — {datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")}'
            commit_file(vendor['repo_path'], file_bytes, commit_msg)
            print(f'  ✓ Committed to {vendor["repo_path"]}')
            success += 1

        except Exception as e:
            print(f'  ✗ ERROR: {e}')
            failed += 1

    print()
    print(f'=== Done: {success} succeeded, {failed} failed ===')
    if failed > 0:
        exit(1)


if __name__ == '__main__':
    main()
