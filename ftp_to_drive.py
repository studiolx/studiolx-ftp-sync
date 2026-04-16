"""
ftp_to_drive.py
Downloads inventory files from vendor FTP servers and uploads to Google Drive.
Runs daily via GitHub Actions.
"""

import ftplib
import io
import os
import json
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload

# ── CONFIGURATION ─────────────────────────────────────────────────────────────

VENDORS = [
    {
        'name': 'SW Corp (Anzzi / Spa World / Meditub)',
        'ftp_host': '208.79.218.238',
        'ftp_port': 21,
        'ftp_user': 'swinventory@swcorp.com',
        'ftp_pass': os.environ['FTP_SWCORP_PASS'],
        'ftp_path': '/SW Inventory Feed_Private Dealer.csv',
        'drive_folder': 'SWCorp',
        'drive_filename': 'SW Inventory Feed_Private Dealer.csv',
    },
    # Add Eglo and Nourison here when credentials are available:
    # {
    #     'name': 'Eglo',
    #     'ftp_host': '',
    #     'ftp_port': 21,
    #     'ftp_user': '',
    #     'ftp_pass': os.environ['FTP_EGLO_PASS'],
    #     'ftp_path': '/inventory.csv',
    #     'drive_folder': 'Eglo',
    #     'drive_filename': 'eglo_inventory.csv',
    # },
    # {
    #     'name': 'Nourison',
    #     'ftp_host': '',
    #     'ftp_port': 21,
    #     'ftp_user': '',
    #     'ftp_pass': os.environ['FTP_NOURISON_PASS'],
    #     'ftp_path': '/inventory.csv',
    #     'drive_folder': 'Nourison',
    #     'drive_filename': 'nourison_inventory.csv',
    # },
]

# Google Drive folder path: BC_Inventory_Cache/FTP_Drops/<vendor_folder>/
DRIVE_ROOT_FOLDER = 'BC_Inventory_Cache'
DRIVE_FTP_FOLDER  = 'FTP_Drops'

# ── GOOGLE DRIVE HELPERS ──────────────────────────────────────────────────────

def get_drive_service():
    """Build Google Drive service from service account credentials."""
    creds_json = os.environ['GOOGLE_SERVICE_ACCOUNT_JSON']
    creds_dict = json.loads(creds_json)
    creds = service_account.Credentials.from_service_account_info(
        creds_dict,
        scopes=['https://www.googleapis.com/auth/drive']
    )
    return build('drive', 'v3', credentials=creds)


def get_or_create_folder(service, name, parent_id=None):
    """Get a Drive folder by name (and optionally parent), or create it."""
    query = f"name='{name}' and mimeType='application/vnd.google-apps.folder' and trashed=false"
    if parent_id:
        query += f" and '{parent_id}' in parents"
    results = service.files().list(q=query, fields='files(id, name)').execute()
    files = results.get('files', [])
    if files:
        return files[0]['id']
    # Create folder
    meta = {
        'name': name,
        'mimeType': 'application/vnd.google-apps.folder',
    }
    if parent_id:
        meta['parents'] = [parent_id]
    folder = service.files().create(body=meta, fields='id').execute()
    print(f"  Created Drive folder: {name}")
    return folder['id']


def upload_to_drive(service, file_bytes, filename, folder_id):
    """Upload or overwrite a file in the specified Drive folder."""
    # Check if file already exists
    query = f"name='{filename}' and '{folder_id}' in parents and trashed=false"
    results = service.files().list(q=query, fields='files(id, name)').execute()
    files = results.get('files', [])

    media = MediaIoBaseUpload(
        io.BytesIO(file_bytes),
        mimetype='text/csv',
        resumable=True
    )

    if files:
        # Update existing file
        file_id = files[0]['id']
        service.files().update(fileId=file_id, media_body=media).execute()
        print(f"  Updated existing file: {filename}")
    else:
        # Create new file
        meta = {'name': filename, 'parents': [folder_id]}
        service.files().create(body=meta, media_body=media, fields='id').execute()
        print(f"  Created new file: {filename}")


# ── FTP DOWNLOAD ──────────────────────────────────────────────────────────────

def download_from_ftp(host, port, user, password, remote_path):
    """Connect to FTP server and download a file. Returns bytes."""
    print(f"  Connecting to FTP {host}:{port} as {user}...")
    buf = io.BytesIO()
    with ftplib.FTP() as ftp:
        ftp.connect(host, port, timeout=30)
        ftp.login(user, password)
        ftp.set_pasv(True)
        print(f"  Downloading: {remote_path}")
        ftp.retrbinary(f'RETR {remote_path}', buf.write)
    size_kb = buf.tell() / 1024
    print(f"  Downloaded {size_kb:.1f} KB")
    return buf.getvalue()


# ── MAIN ──────────────────────────────────────────────────────────────────────

def main():
    print("=== FTP → Google Drive Inventory Sync ===")
    print()

    # Set up Drive service
    print("Authenticating with Google Drive...")
    service = get_drive_service()

    # Get or create root folder structure: BC_Inventory_Cache/FTP_Drops/
    root_id = get_or_create_folder(service, DRIVE_ROOT_FOLDER)
    ftp_drops_id = get_or_create_folder(service, DRIVE_FTP_FOLDER, root_id)

    success = 0
    failed = 0

    for vendor in VENDORS:
        print(f"\n── {vendor['name']} ──")
        try:
            # Download from FTP
            file_bytes = download_from_ftp(
                vendor['ftp_host'],
                vendor['ftp_port'],
                vendor['ftp_user'],
                vendor['ftp_pass'],
                vendor['ftp_path']
            )

            # Get or create vendor subfolder in Drive
            vendor_folder_id = get_or_create_folder(
                service,
                vendor['drive_folder'],
                ftp_drops_id
            )

            # Upload to Drive
            upload_to_drive(
                service,
                file_bytes,
                vendor['drive_filename'],
                vendor_folder_id
            )

            print(f"  ✓ {vendor['name']} complete")
            success += 1

        except Exception as e:
            print(f"  ✗ ERROR: {e}")
            failed += 1

    print(f"\n=== Done: {success} succeeded, {failed} failed ===")
    if failed > 0:
        exit(1)  # Non-zero exit causes GitHub Actions to mark run as failed


if __name__ == '__main__':
    main()
