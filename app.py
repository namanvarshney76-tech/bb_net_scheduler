#!/usr/bin/env python3
"""
BigBasket Automation Scheduler for GitHub Actions
Runs Gmail to Drive and Drive to Sheets workflows
Logs execution summary to Google Sheets
"""

import schedule
import time
import logging
import json
import os
import io
import pandas as pd
from datetime import datetime, timedelta, timezone
from typing import Dict, Any, List, Set, Tuple
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload, MediaIoBaseDownload
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import base64
import warnings
import re
import zipfile
import math
import sys

warnings.filterwarnings("ignore")

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('bigbasket_automation.log', encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

class BigBasketScheduler:
    def __init__(self, run_once=False):
        self.gmail_service = None
        self.drive_service = None
        self.sheets_service = None
        self.creds = None
        self.run_once = run_once
        
        # API scopes
        self.scopes = [
            'https://www.googleapis.com/auth/gmail.readonly',
            'https://www.googleapis.com/auth/drive',
            'https://www.googleapis.com/auth/spreadsheets',
            'https://www.googleapis.com/auth/gmail.send'
        ]
        
        # Email notification recipients
        self.email_recipients = [
            'keyur@thebakersdozen.in',
            'me'  # Will send to authenticated user
        ]
        
        # Hardcoded configurations
        self.gmail_config = {
            'sender': 'bbnet2@bigbasket.com',
            'search_term': 'grn',
            'days_back': 2,
            'max_results': 10,
            'gdrive_folder_id': '1l5L9IdQ8WcV6AZ04JCeuyxvbNkLPJnHt'
        }
        
        self.excel_config = {
            'excel_folder_id': '1fdio9_h28UleeRjgRnWF32S8kg_fgWbs',
            'spreadsheet_id': '170WUaPhkuxCezywEqZXJtHRw3my3rpjB9lJOvfLTeKM',
            'sheet_name': 'test',
            'summary_sheet_name': 'net_workflow_log',
            'header_row': 2
        }
        
        self.execution_stats = {
            'start_time': None,
            'end_time': None,
            'emails_checked': 0,
            'attachments_found': 0,
            'attachments_saved': 0,
            'attachments_skipped': 0,
            'attachments_failed': 0,
            'files_found': 0,
            'files_processed': 0,
            'files_skipped': 0,
            'files_failed': 0,
            'duplicates_removed': 0,
            'errors': []
        }
    
    def _setup_credentials_from_env(self):
        """Setup credentials from environment variables for GitHub Actions"""
        try:
            # Check if we're in GitHub Actions
            if os.environ.get('GITHUB_ACTIONS') != 'true':
                return True
                
            logger.info("Setting up credentials from environment variables...")
            
            # Get credentials JSON from environment
            credentials_json = os.environ.get('GOOGLE_CREDENTIALS')
            token_encoded = os.environ.get('GOOGLE_TOKEN')
            
            if not credentials_json:
                logger.error("GOOGLE_CREDENTIALS environment variable not set")
                return False
                
            if not token_encoded:
                logger.error("GOOGLE_TOKEN environment variable not set")
                return False
            
            try:
                # Parse and save credentials.json
                credentials_data = json.loads(credentials_json)
                with open('credentials.json', 'w') as f:
                    json.dump(credentials_data, f)
                logger.info("Saved credentials.json from environment")
                
                # Decode and save token.json
                token_json = base64.b64decode(token_encoded).decode('utf-8')
                with open('token.json', 'w') as f:
                    f.write(token_json)
                logger.info("Saved token.json from environment")
                
                return True
                
            except json.JSONDecodeError as e:
                logger.error(f"Invalid JSON in GOOGLE_CREDENTIALS: {str(e)}")
                return False
            except Exception as e:
                logger.error(f"Error processing credentials from env: {str(e)}")
                return False
                
        except Exception as e:
            logger.error(f"Failed to setup credentials from environment: {str(e)}")
            return False
    
    def authenticate(self):
        """Authenticate with Google APIs with GitHub Actions support"""
        try:
            logger.info("Starting authentication process...")
            
            # Setup credentials from environment if in GitHub Actions
            if os.environ.get('GITHUB_ACTIONS') == 'true':
                if not self._setup_credentials_from_env():
                    logger.error("Failed to setup credentials from environment")
                    return False
            
            # Check for existing token
            if os.path.exists('token.json'):
                try:
                    self.creds = Credentials.from_authorized_user_file('token.json', self.scopes)
                    logger.info("Loaded credentials from token.json")
                except Exception as e:
                    logger.warning(f"Failed to load from token.json: {str(e)}")
                    self.creds = None
            
            # Refresh or create new credentials
            if not self.creds or not self.creds.valid:
                if self.creds and self.creds.expired and self.creds.refresh_token:
                    logger.info("Refreshing expired credentials...")
                    try:
                        self.creds.refresh(Request())
                        logger.info("Credentials refreshed successfully")
                    except Exception as refresh_error:
                        logger.error(f"Failed to refresh token: {str(refresh_error)}")
                        # For GitHub Actions, we can't do interactive auth
                        if os.environ.get('GITHUB_ACTIONS') == 'true':
                            logger.error("Cannot refresh token in GitHub Actions. Check if token is valid.")
                            return False
                        # For local, try to get new credentials
                        self.creds = self._create_new_credentials()
                else:
                    # For GitHub Actions, we should have valid credentials already
                    if os.environ.get('GITHUB_ACTIONS') == 'true':
                        logger.error("No valid credentials found in GitHub Actions")
                        return False
                    # For local, get new credentials
                    self.creds = self._create_new_credentials()
                
                # Save credentials for future use
                if self.creds:
                    with open('token.json', 'w') as token:
                        token.write(self.creds.to_json())
            
            if not self.creds:
                logger.error("Failed to obtain valid credentials")
                return False
            
            # Build services
            self.gmail_service = build('gmail', 'v1', credentials=self.creds)
            self.drive_service = build('drive', 'v3', credentials=self.creds)
            self.sheets_service = build('sheets', 'v4', credentials=self.creds)
            
            logger.info("Authentication successful!")
            return True
            
        except Exception as e:
            logger.error(f"Authentication failed: {str(e)}")
            return False
    
    def _create_new_credentials(self):
        """Create new credentials from credentials.json (local use only)"""
        try:
            if not os.path.exists('credentials.json'):
                logger.error("credentials.json not found!")
                return None
            
            logger.info("Starting OAuth flow for new credentials...")
            flow = InstalledAppFlow.from_client_secrets_file(
                'credentials.json', self.scopes
            )
            creds = flow.run_local_server(port=0)
            logger.info("OAuth flow completed successfully")
            return creds
            
        except Exception as e:
            logger.error(f"Failed to create new credentials: {str(e)}")
            return None
    
    def _refresh_services(self):
        """Refresh all services with current credentials"""
        try:
            if self.creds and self.creds.expired and self.creds.refresh_token:
                self.creds.refresh(Request())
            
            self.gmail_service = build('gmail', 'v1', credentials=self.creds)
            self.drive_service = build('drive', 'v3', credentials=self.creds)
            self.sheets_service = build('sheets', 'v4', credentials=self.creds)
            return True
        except Exception as e:
            logger.error(f"Failed to refresh services: {str(e)}")
            return False
    
    def search_emails(self, sender: str, search_term: str, days_back: int, max_results: int):
        """Search for emails with attachments"""
        try:
            query_parts = ["has:attachment"]
            
            if sender:
                query_parts.append(f'from:"{sender}"')
            
            if search_term:
                if "," in search_term:
                    keywords = [k.strip() for k in search_term.split(",")]
                    keyword_query = " OR ".join([f'"{k}"' for k in keywords if k])
                    if keyword_query:
                        query_parts.append(f"({keyword_query})")
                else:
                    query_parts.append(f'"{search_term}"')
            
            start_date = datetime.now() - timedelta(days=days_back)
            query_parts.append(f"after:{start_date.strftime('%Y/%m/%d')}")
            
            query = " ".join(query_parts)
            logger.info(f"Gmail search query: {query}")
            
            max_results = max(max_results, 1) if max_results else 1
            
            result = self.gmail_service.users().messages().list(
                userId='me', q=query, maxResults=max_results
            ).execute()
            
            messages = result.get('messages', [])
            self.execution_stats['emails_checked'] = len(messages)
            logger.info(f"Found {len(messages)} emails matching criteria")
            
            return messages
            
        except Exception as e:
            logger.error(f"Email search failed: {str(e)}")
            self.execution_stats['errors'].append(f"Email search: {str(e)}")
            return []
    
    def process_gmail_workflow(self):
        """Process Gmail attachment download workflow"""
        try:
            logger.info("=" * 60)
            logger.info("STARTING GMAIL TO DRIVE WORKFLOW")
            logger.info("=" * 60)
            
            self.execution_stats['attachments_skipped'] = 0
            self.execution_stats['attachments_found'] = 0
            self.execution_stats['attachments_failed'] = 0
            
            config = self.gmail_config
            
            emails = self.search_emails(
                sender=config['sender'],
                search_term=config['search_term'],
                days_back=config['days_back'],
                max_results=config['max_results']
            )
            
            if not emails:
                logger.info("No emails found matching criteria")
                return {'success': True, 'processed': 0}
            
            base_folder_name = "Gmail_Attachments_BigBasket"
            base_folder_id = self._create_drive_folder(base_folder_name, config.get('gdrive_folder_id'))
            
            if not base_folder_id:
                logger.error("Failed to create base folder in Google Drive")
                return {'success': False, 'processed': 0}
            
            processed_count = 0
            skipped_count = 0
            failed_count = 0
            total_attachments_found = 0
            
            for i, email in enumerate(emails):
                try:
                    logger.info(f"Processing email {i+1}/{len(emails)}")
                    
                    email_details = self._get_email_details(email['id'])
                    subject = email_details.get('subject', 'No Subject')[:50]
                    sender = email_details.get('sender', 'Unknown')
                    
                    logger.info(f"Email: {subject} from {sender}")
                    
                    message = self.gmail_service.users().messages().get(
                        userId='me', id=email['id'], format='full'
                    ).execute()
                    
                    if not message or not message.get('payload'):
                        continue
                    
                    saved, skipped, failed, found = self._extract_attachments_from_email(
                        email['id'], message['payload'], email_details, config, base_folder_id
                    )
                    
                    processed_count += saved
                    skipped_count += skipped
                    failed_count += failed
                    total_attachments_found += found
                    
                except Exception as e:
                    error_msg = f"Failed to process email {email.get('id', 'unknown')}: {str(e)}"
                    logger.error(error_msg)
                    self.execution_stats['errors'].append(error_msg)
            
            self.execution_stats['attachments_saved'] = processed_count
            self.execution_stats['attachments_skipped'] = skipped_count
            self.execution_stats['attachments_failed'] = failed_count
            self.execution_stats['attachments_found'] = total_attachments_found
            
            logger.info(f"Gmail workflow completed! Found: {total_attachments_found}, Saved: {processed_count}, Skipped: {skipped_count}, Failed: {failed_count}")
            
            return {'success': True, 'processed': processed_count}
            
        except Exception as e:
            error_msg = f"Gmail workflow failed: {str(e)}"
            logger.error(error_msg)
            self.execution_stats['errors'].append(error_msg)
            return {'success': False, 'processed': 0}
    
    def _get_source_file_names_from_sheet(self) -> Set[str]:
        """Get list of source file names that have already been processed"""
        try:
            config = self.excel_config
            result = self.sheets_service.spreadsheets().values().get(
                spreadsheetId=config['spreadsheet_id'],
                range=f"{config['sheet_name']}!A1:ZZ"
            ).execute()
            
            values = result.get('values', [])
            if not values:
                return set()
            
            headers = values[0] if values else []
            source_file_col_idx = None
            
            for i, header in enumerate(headers):
                if header and 'source_file_name' in str(header).lower():
                    source_file_col_idx = i
                    break
            
            if source_file_col_idx is None:
                logger.info("No 'source_file_name' column found in sheet")
                return set()
            
            source_files = set()
            for row in values[1:]:
                if len(row) > source_file_col_idx:
                    source_file = str(row[source_file_col_idx]).strip()
                    if source_file:
                        source_files.add(source_file)
            
            logger.info(f"Found {len(source_files)} unique source files in sheet")
            return source_files
            
        except Exception as e:
            logger.error(f"Failed to get source file names from sheet: {str(e)}")
            return set()
    
    def _ensure_source_file_name_column(self):
        """Ensure the sheet has 'source_file_name' column"""
        try:
            config = self.excel_config
            sheet_name = config['sheet_name']
            spreadsheet_id = config['spreadsheet_id']
            
            result = self.sheets_service.spreadsheets().values().get(
                spreadsheetId=spreadsheet_id,
                range=f"{sheet_name}!1:1"
            ).execute()
            
            values = result.get('values', [])
            if not values:
                return True
            
            headers = values[0]
            
            source_file_name_exists = any(
                header and 'source_file_name' in str(header).lower()
                for header in headers
            )
            
            if not source_file_name_exists:
                headers.append('source_file_name')
                
                self.sheets_service.spreadsheets().values().update(
                    spreadsheetId=spreadsheet_id,
                    range=f"{sheet_name}!1:1",
                    valueInputOption='RAW',
                    body={'values': [headers]}
                ).execute()
                
                logger.info("Added 'source_file_name' column to sheet headers")
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to ensure source_file_name column: {str(e)}")
            return False
    
    def process_excel_workflow(self):
        """Process Excel GRN workflow from Drive files with source_file_name filtering"""
        try:
            logger.info("=" * 60)
            logger.info("STARTING DRIVE TO SHEETS WORKFLOW")
            logger.info("=" * 60)
            
            config = self.excel_config
            
            excel_files = self._get_excel_files_filtered(
                config['excel_folder_id'],
                self.gmail_config['days_back'],
                self.gmail_config['max_results']
            )
            
            logger.info(f"Found {len(excel_files)} Excel files")
            self.execution_stats['files_found'] = len(excel_files)
            
            if not excel_files:
                logger.info("No Excel files found in the specified folder")
                return {'success': True, 'processed': 0}
            
            self._ensure_source_file_name_column()
            processed_source_files = self._get_source_file_names_from_sheet()
            
            files_to_process = []
            for file in excel_files:
                if file['name'] in processed_source_files:
                    logger.info(f"[SKIP] Skipping {file['name']} - already in source_file_name column")
                    self.execution_stats['files_skipped'] += 1
                else:
                    files_to_process.append(file)
            
            logger.info(f"After filtering: {len(files_to_process)} files to process")
            
            processed_count = 0
            failed_count = 0
            is_first_file = True
            
            sheet_has_headers = self._check_sheet_headers(config['spreadsheet_id'], config['sheet_name'])
            
            for i, file in enumerate(files_to_process):
                try:
                    logger.info(f"Processing Excel file {i+1}/{len(files_to_process)}: {file['name']}")
                    
                    df = self._read_excel_file_robust(file['id'], file['name'], config['header_row'])
                    
                    if df.empty:
                        logger.warning(f"No data extracted from {file['name']}")
                        failed_count += 1
                        continue
                    
                    logger.info(f"Data shape: {df.shape} - Columns: {list(df.columns)[:3]}...")
                    
                    df['source_file_name'] = file['name']
                    
                    append_headers = is_first_file and not sheet_has_headers
                    self._append_to_sheet(
                        config['spreadsheet_id'],
                        config['sheet_name'],
                        df,
                        append_headers
                    )
                    
                    logger.info(f"[OK] Appended {len(df)} rows from {file['name']}")
                    processed_count += 1
                    is_first_file = False
                    sheet_has_headers = True
                    
                except Exception as e:
                    error_msg = f"Failed to process Excel file {file.get('name', 'unknown')}: {str(e)}"
                    logger.error(error_msg)
                    self.execution_stats['errors'].append(error_msg)
                    failed_count += 1
            
            self.execution_stats['files_processed'] = processed_count
            self.execution_stats['files_failed'] = failed_count
            
            if processed_count > 0:
                logger.info("Removing duplicates from Google Sheet...")
                duplicates_removed = self._remove_duplicates_from_sheet(
                    config['spreadsheet_id'],
                    config['sheet_name']
                )
                self.execution_stats['duplicates_removed'] = duplicates_removed
            
            logger.info(f"Excel workflow completed! Found: {len(excel_files)}, Processed: {processed_count}, Skipped: {self.execution_stats['files_skipped']}, Failed: {failed_count}")
            
            return {'success': True, 'processed': processed_count}
            
        except Exception as e:
            error_msg = f"Excel workflow failed: {str(e)}"
            logger.error(error_msg)
            self.execution_stats['errors'].append(error_msg)
            return {'success': False, 'processed': 0}
    
    def _get_email_details(self, message_id: str) -> Dict:
        """Get email details"""
        try:
            message = self.gmail_service.users().messages().get(
                userId='me', id=message_id, format='metadata'
            ).execute()
            
            headers = message['payload'].get('headers', [])
            
            details = {
                'id': message_id,
                'sender': next((h['value'] for h in headers if h['name'] == "From"), "Unknown"),
                'subject': next((h['value'] for h in headers if h['name'] == "Subject"), "(No Subject)"),
                'date': next((h['value'] for h in headers if h['name'] == "Date"), "")
            }
            
            return details
            
        except Exception:
            return {'id': message_id, 'sender': 'Unknown', 'subject': 'Unknown', 'date': ''}
    
    def _create_drive_folder(self, folder_name: str, parent_folder_id: str = None) -> str:
        """Create a folder in Google Drive"""
        try:
            query = f"name='{folder_name}' and mimeType='application/vnd.google-apps.folder' and trashed=false"
            if parent_folder_id:
                query += f" and '{parent_folder_id}' in parents"
            
            existing = self.drive_service.files().list(q=query, fields='files(id, name)').execute()
            files = existing.get('files', [])
            
            if files:
                return files[0]['id']
            
            folder_metadata = {
                'name': folder_name,
                'mimeType': 'application/vnd.google-apps.folder'
            }
            
            if parent_folder_id:
                folder_metadata['parents'] = [parent_folder_id]
            
            folder = self.drive_service.files().create(
                body=folder_metadata,
                fields='id'
            ).execute()
            
            return folder.get('id')
            
        except Exception as e:
            logger.error(f"Failed to create folder {folder_name}: {str(e)}")
            return ""
    
    def _sanitize_filename(self, filename: str) -> str:
        """Clean up filenames"""
        cleaned = re.sub(r'[<>:"/\\|?*]', '_', filename)
        if len(cleaned) > 100:
            name_parts = cleaned.split('.')
            if len(name_parts) > 1:
                extension = name_parts[-1]
                base_name = '.'.join(name_parts[:-1])
                cleaned = f"{base_name[:95]}.{extension}"
            else:
                cleaned = cleaned[:100]
        return cleaned
    
    def _get_existing_files_in_folder(self, folder_id: str) -> Set[str]:
        """Get all existing filenames in a Drive folder"""
        try:
            existing_files = set()
            page_token = None
            
            while True:
                query = f"'{folder_id}' in parents and trashed=false"
                results = self.drive_service.files().list(
                    q=query,
                    fields="nextPageToken, files(name)",
                    pageToken=page_token,
                    pageSize=1000
                ).execute()
                
                files = results.get('files', [])
                for file in files:
                    existing_files.add(file['name'])
                
                page_token = results.get('nextPageToken')
                if not page_token:
                    break
            
            return existing_files
            
        except Exception as e:
            logger.error(f"Failed to get existing files in folder {folder_id}: {str(e)}")
            return set()
    
    def _extract_attachments_from_email(self, message_id: str, payload: Dict, sender_info: Dict, config: dict, base_folder_id: str) -> tuple:
        """Extract Excel attachments with duplicate protection"""
        saved_count = 0
        skipped_count = 0
        failed_count = 0
        found_count = 0
        
        if "parts" in payload:
            for part in payload["parts"]:
                saved, skipped, failed, found = self._extract_attachments_from_email(
                    message_id, part, sender_info, config, base_folder_id
                )
                saved_count += saved
                skipped_count += skipped
                failed_count += failed
                found_count += found
        elif payload.get("filename") and "attachmentId" in payload.get("body", {}):
            filename = payload.get("filename", "")
            
            if not filename.lower().endswith(('.xls', '.xlsx', '.xlsm')):
                return (0, 0, 0, 0)
            
            found_count += 1
            
            clean_filename = self._sanitize_filename(filename)
            final_filename = f"{message_id}_{clean_filename}"

            try:
                sender_email = sender_info.get('sender', 'Unknown')
                if "<" in sender_email and ">" in sender_email:
                    sender_email = sender_email.split("<")[1].split(">")[0].strip()
                
                sender_folder_name = self._sanitize_filename(sender_email)
                type_folder_id = self._create_drive_folder(sender_folder_name, base_folder_id)
                
                existing_files = self._get_existing_files_in_folder(type_folder_id)
                if final_filename in existing_files:
                    logger.info(f"[SKIP] Skipping {final_filename} - already exists in Drive folder")
                    return (0, 1, 0, found_count)
                
                attachment_id = payload["body"].get("attachmentId")
                att = self.gmail_service.users().messages().attachments().get(
                    userId='me', messageId=message_id, id=attachment_id
                ).execute()
                
                file_data = base64.urlsafe_b64decode(att["data"].encode("UTF-8"))
                
                file_metadata = {
                    'name': final_filename,
                    'parents': [type_folder_id]
                }
                
                media = MediaIoBaseUpload(
                    io.BytesIO(file_data),
                    mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
                )
                
                self.drive_service.files().create(
                    body=file_metadata,
                    media_body=media,
                    fields='id'
                ).execute()
                
                logger.info(f"[OK] Uploaded: {final_filename}")
                saved_count += 1
                
            except Exception as e:
                logger.error(f"Error processing attachment {filename}: {str(e)}")
                failed_count += 1
        
        return (saved_count, skipped_count, failed_count, found_count)
    
    def _get_excel_files_filtered(self, folder_id: str, days_back: int, max_results: int):
        """Get Excel files from Drive folder"""
        try:
            if not folder_id or folder_id.strip() == '':
                logger.error("Folder ID is empty or invalid")
                return []
            
            start_date = datetime.now(timezone.utc) - timedelta(days=days_back)
            start_date_str = start_date.isoformat()
            query = (f"'{folder_id}' in parents and "
                     f"(mimeType='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet' or "
                     f"mimeType='application/vnd.ms-excel') and "
                     f"createdTime > '{start_date_str}' and trashed=false")
            
            results = self.drive_service.files().list(
                q=query,
                fields="files(id, name, createdTime)",
                orderBy='createdTime desc',
                pageSize=max_results
            ).execute()
            
            files = results.get('files', [])
            logger.info(f"Found {len(files)} Excel files in folder {folder_id}")
            return files
            
        except Exception as e:
            logger.error(f"Failed to get Excel files from folder {folder_id}: {str(e)}")
            
            try:
                folders = self.drive_service.files().list(
                    q="mimeType='application/vnd.google-apps.folder' and trashed=false",
                    fields="files(id, name)",
                    pageSize=10
                ).execute()
                
                folder_list = folders.get('files', [])
                logger.info(f"Available folders: {[(f['name'], f['id'][:10] + '...') for f in folder_list]}")
            except Exception as debug_e:
                logger.error(f"Could not list folders for debugging: {debug_e}")
            
            return []
    
    def _read_excel_file_robust(self, file_id: str, filename: str, header_row: int) -> pd.DataFrame:
        """Read Excel file with robust parsing"""
        try:
            request = self.drive_service.files().get_media(fileId=file_id)
            file_stream = io.BytesIO()
            downloader = MediaIoBaseDownload(file_stream, request)
            done = False
            while not done:
                status, done = downloader.next_chunk()
            
            file_stream.seek(0)
            
            try:
                file_stream.seek(0)
                if header_row == -1:
                    df = pd.read_excel(file_stream, engine="openpyxl", header=None)
                else:
                    df = pd.read_excel(file_stream, engine="openpyxl", header=header_row)
                if not df.empty:
                    return self._clean_dataframe(df)
            except Exception:
                pass
            
            if filename.lower().endswith('.xls'):
                try:
                    file_stream.seek(0)
                    if header_row == -1:
                        df = pd.read_excel(file_stream, engine="xlrd", header=None)
                    else:
                        df = pd.read_excel(file_stream, engine="xlrd", header=header_row)
                    if not df.empty:
                        return self._clean_dataframe(df)
                except Exception:
                    pass
            
            df = self._try_raw_xml_extraction(file_stream, header_row)
            if not df.empty:
                return self._clean_dataframe(df)
            
            return pd.DataFrame()
            
        except Exception as e:
            logger.error(f"Error reading {filename}: {str(e)}")
            return pd.DataFrame()
    
    def _try_raw_xml_extraction(self, file_stream: io.BytesIO, header_row: int) -> pd.DataFrame:
        """Raw XML extraction for corrupted Excel files"""
        try:
            file_stream.seek(0)
            with zipfile.ZipFile(file_stream, 'r') as zip_ref:
                file_list = zip_ref.namelist()
                shared_strings = {}
                
                shared_strings_file = 'xl/sharedStrings.xml'
                if shared_strings_file in file_list:
                    try:
                        with zip_ref.open(shared_strings_file) as ss_file:
                            ss_content = ss_file.read().decode('utf-8', errors='ignore')
                            string_pattern = r'<t[^>]*>([^<]*)</t>'
                            strings = re.findall(string_pattern, ss_content, re.DOTALL)
                            for i, string_val in enumerate(strings):
                                shared_strings[str(i)] = string_val.strip()
                    except Exception:
                        pass
                
                worksheet_files = [f for f in file_list if 'xl/worksheets/' in f and f.endswith('.xml')]
                if not worksheet_files:
                    return pd.DataFrame()
                
                with zip_ref.open(worksheet_files[0]) as xml_file:
                    content = xml_file.read().decode('utf-8', errors='ignore')
                    cell_pattern = r'<c[^>]*r="([A-Z]+\d+)"[^>]*(?:t="([^"]*)")?[^>]*>(?:.*?<v[^>]*>([^<]*)</v>)?(?:.*?<is><t[^>]*>([^<]*)</t></is>)?'
                    cells = re.findall(cell_pattern, content, re.DOTALL)
                    
                    if not cells:
                        return pd.DataFrame()
                    
                    cell_data = {}
                    max_row = 0
                    max_col = 0
                    
                    for cell_ref, cell_type, v_value, is_value in cells:
                        col_letters = ''.join([c for c in cell_ref if c.isalpha()])
                        row_num = int(''.join([c for c in cell_ref if c.isdigit()]))
                        col_num = 0
                        for c in col_letters:
                            col_num = col_num * 26 + (ord(c) - ord('A') + 1)
                        
                        if is_value:
                            cell_value = is_value.strip()
                        elif cell_type == 's' and v_value:
                            cell_value = shared_strings.get(v_value, v_value)
                        elif v_value:
                            cell_value = v_value.strip()
                        else:
                            cell_value = ""
                        
                        cell_data[(row_num, col_num)] = self._clean_cell_value(cell_value)
                        max_row = max(max_row, row_num)
                        max_col = max(max_col, col_num)
                    
                    if not cell_data:
                        return pd.DataFrame()
                    
                    data = []
                    for row in range(1, max_row + 1):
                        row_data = []
                        for col in range(1, max_col + 1):
                            row_data.append(cell_data.get((row, col), ""))
                        if any(cell for cell in row_data):
                            data.append(row_data)
                    
                    if len(data) < max(1, header_row + 2):
                        return pd.DataFrame()
                    
                    if header_row == -1:
                        headers = [f"Column_{i+1}" for i in range(len(data[0]))]
                        return pd.DataFrame(data, columns=headers)
                    else:
                        if len(data) > header_row:
                            headers = [str(h) if h else f"Column_{i+1}" for i, h in enumerate(data[header_row])]
                            return pd.DataFrame(data[header_row+1:], columns=headers)
                        else:
                            return pd.DataFrame()
                
        except Exception:
            return pd.DataFrame()
    
    def _clean_cell_value(self, value):
        """Clean cell values"""
        if value is None:
            return None
        value = str(value).strip().replace("'", "")
        try:
            if '.' in value or 'e' in value.lower():
                return float(value)
            else:
                return int(value)
        except ValueError:
            return value
    
    def _clean_dataframe(self, df):
        """Clean DataFrame"""
        if df.empty:
            return df
        
        string_columns = df.select_dtypes(include=['object']).columns
        for col in string_columns:
            df[col] = df[col].astype(str).str.strip().str.replace("'", "", regex=False)
        
        if len(df.columns) >= 2:
            second_col = df.columns[1]
            mask = ~(
                df[second_col].isna() | 
                (df[second_col].astype(str).str.strip() == "") |
                (df[second_col].astype(str).str.strip() == "nan")
            )
            df = df[mask]
        
        df = df.drop_duplicates()
        
        return df
    
    def _check_sheet_headers(self, spreadsheet_id: str, sheet_name: str) -> bool:
        """Check if sheet has headers"""
        try:
            result = self.sheets_service.spreadsheets().values().get(
                spreadsheetId=spreadsheet_id,
                range=f"{sheet_name}!A1"
            ).execute()
            return bool(result.get('values', []))
        except:
            return False
    
    def _append_to_sheet(self, spreadsheet_id: str, sheet_name: str, df: pd.DataFrame, append_headers: bool):
        """Append DataFrame to Google Sheet"""
        try:
            def process_value(v):
                if math.isnan(v) if isinstance(v, float) else pd.isna(v):
                    return ''
                if isinstance(v, (int, float)):
                    return v
                return str(v) if v is not None else ''
            
            values = []
            if append_headers:
                values.append([str(col) for col in df.columns])
            
            data_rows = [[process_value(cell) for cell in row] for row in df.itertuples(index=False)]
            values += data_rows
            
            if not values:
                return
            
            result = self.sheets_service.spreadsheets().values().get(
                spreadsheetId=spreadsheet_id,
                range=f"{sheet_name}!A:A"
            ).execute()
            existing_rows = result.get('values', [])
            start_row = len(existing_rows) + 1 if existing_rows else 1
            
            self.sheets_service.spreadsheets().values().append(
                spreadsheetId=spreadsheet_id,
                range=f"{sheet_name}!A{start_row}",
                valueInputOption="RAW",
                body={"values": values}
            ).execute()
            
        except Exception as e:
            logger.error(f"Error appending to sheet: {str(e)}")
            raise
    
    def _remove_duplicates_from_sheet(self, spreadsheet_id: str, sheet_name: str) -> int:
        """Remove duplicates based on Skucode and PoNo"""
        try:
            result = self.sheets_service.spreadsheets().values().get(
                spreadsheetId=spreadsheet_id,
                range=f"{sheet_name}!A1:ZZ"
            ).execute()
            values = result.get('values', [])
            
            if not values:
                return 0
            
            max_len = max(len(row) for row in values)
            for row in values:
                row.extend([''] * (max_len - len(row)))
            
            headers = [values[0][i] if values[0][i] else f"Column_{i+1}" for i in range(max_len)]
            
            rows = values[1:]
            df = pd.DataFrame(rows, columns=headers)
            before = len(df)
            
            if "Skucode" in df.columns and "PoNo" in df.columns:
                df = df.drop_duplicates(subset=["Skucode", "PoNo"], keep="first")
            
            after_dup = len(df)
            removed_dup = before - after_dup
            
            df.replace('', pd.NA, inplace=True)
            df.dropna(how='all', inplace=True)
            df.dropna(how='all', axis=1, inplace=True)
            df.fillna('', inplace=True)
            
            after_clean = len(df)
            removed_clean = after_dup - after_clean
            
            if "PoNo" in df.columns:
                df = df.sort_values(by="PoNo", ascending=True)
            
            def process_value(v):
                if pd.isna(v) or v == '':
                    return ''
                try:
                    if '.' in str(v) or 'e' in str(v).lower():
                        return float(v)
                    return int(v)
                except (ValueError, TypeError):
                    return str(v)
            
            values = []
            values.append([str(col) for col in df.columns])
            for row in df.itertuples(index=False):
                values.append([process_value(cell) for cell in row])
            
            self.sheets_service.spreadsheets().values().clear(
                spreadsheetId=spreadsheet_id,
                range=sheet_name
            ).execute()
            
            body = {"values": values}
            self.sheets_service.spreadsheets().values().update(
                spreadsheetId=spreadsheet_id,
                range=f"{sheet_name}!A1",
                valueInputOption="RAW",
                body=body
            ).execute()
            
            logger.info(f"Duplicates removed: {removed_dup}, Blank rows removed: {removed_clean}")
            return removed_dup
            
        except Exception as e:
            logger.error(f"Error removing duplicates: {str(e)}")
            return 0
    
    def _log_execution_summary(self):
        """Log execution summary to Google Sheets"""
        try:
            config = self.excel_config
            summary_sheet = config['summary_sheet_name']
            
            if self.execution_stats['start_time'] and self.execution_stats['end_time']:
                duration = self.execution_stats['end_time'] - self.execution_stats['start_time']
                duration_str = str(duration).split('.')[0]
            else:
                duration_str = "N/A"
            
            summary_row = [
                self.execution_stats['start_time'].strftime('%Y-%m-%d %H:%M:%S') if self.execution_stats['start_time'] else '',
                self.execution_stats['end_time'].strftime('%Y-%m-%d %H:%M:%S') if self.execution_stats['end_time'] else '',
                duration_str,
                self.execution_stats['emails_checked'],
                self.execution_stats['attachments_found'],
                self.execution_stats['attachments_saved'],
                self.execution_stats['attachments_skipped'],
                self.execution_stats['attachments_failed'],
                self.execution_stats['files_found'],
                self.execution_stats['files_processed'],
                self.execution_stats['files_skipped'],
                self.execution_stats['files_failed'],
                self.execution_stats['duplicates_removed'],
                len(self.execution_stats['errors']),
                '; '.join(self.execution_stats['errors'][:3]) if self.execution_stats['errors'] else 'None'
            ]
            
            try:
                result = self.sheets_service.spreadsheets().values().get(
                    spreadsheetId=config['spreadsheet_id'],
                    range=f"{summary_sheet}!A1"
                ).execute()
                has_headers = bool(result.get('values', []))
            except:
                has_headers = False
            
            values = []
            if not has_headers:
                headers = [
                    'Start Time', 'End Time', 'Duration', 'Emails Checked',
                    'Attachments Found', 'Attachments Saved', 'Attachments Skipped', 'Attachments Failed',
                    'Files Found', 'Files Processed', 'Files Skipped', 'Files Failed',
                    'Duplicates Removed', 'Error Count', 'Errors'
                ]
                values.append(headers)
            
            values.append(summary_row)
            
            self.sheets_service.spreadsheets().values().append(
                spreadsheetId=config['spreadsheet_id'],
                range=f"{summary_sheet}!A1",
                valueInputOption="RAW",
                body={"values": values}
            ).execute()
            
            logger.info("Execution summary logged to Google Sheets")
            
        except Exception as e:
            logger.error(f"Failed to log execution summary: {str(e)}")
    
    def _create_email_message(self, sender: str, to: str, subject: str, body_text: str, body_html: str = None):
        """Create an email message"""
        message = MIMEMultipart('alternative')
        message['From'] = sender
        message['To'] = to
        message['Subject'] = subject
        
        part1 = MIMEText(body_text, 'plain')
        part2 = MIMEText(body_html if body_html else body_text, 'html')
        
        message.attach(part1)
        message.attach(part2)
        
        raw_message = base64.urlsafe_b64encode(message.as_bytes()).decode()
        return {'raw': raw_message}
    
    def _send_email_notification(self):
        """Send email notification with workflow summary"""
        try:
            logger.info("Preparing email notification...")
            
            if self.execution_stats['start_time'] and self.execution_stats['end_time']:
                duration = self.execution_stats['end_time'] - self.execution_stats['start_time']
                duration_str = str(duration).split('.')[0]
            else:
                duration_str = "N/A"
            
            subject = f"Big Basket (Net) Automation Workflow Summary - {datetime.now().strftime('%Y-%m-%d %H:%M')}"
            
            body_text = f"""
BIGBASKET AUTOMATION WORKFLOW SUMMARY

Execution Time: {self.execution_stats['start_time'].strftime('%Y-%m-%d %H:%M:%S') if self.execution_stats['start_time'] else 'N/A'}
Duration: {duration_str}
Days Back Parameter: {self.gmail_config['days_back']}

--- GMAIL TO DRIVE WORKFLOW ---
Number of emails checked: {self.execution_stats['emails_checked']}
Number of attachments found: {self.execution_stats['attachments_found']}
Number of attachments uploaded: {self.execution_stats['attachments_saved']}
Number of attachments skipped: {self.execution_stats['attachments_skipped']}
Number of attachments failed: {self.execution_stats['attachments_failed']}

--- DRIVE TO SHEETS WORKFLOW ---
Number of files found: {self.execution_stats['files_found']}
Number of files processed: {self.execution_stats['files_processed']}
Number of files skipped: {self.execution_stats['files_skipped']}
Number of files failed: {self.execution_stats['files_failed']}
Duplicate records removed: {self.execution_stats['duplicates_removed']}

--- ERRORS ---
Total errors: {len(self.execution_stats['errors'])}
"""
            
            if self.execution_stats['errors']:
                body_text += "\nError details:\n"
                for i, error in enumerate(self.execution_stats['errors'][:5], 1):
                    body_text += f"{i}. {error}\n"
                if len(self.execution_stats['errors']) > 5:
                    body_text += f"... and {len(self.execution_stats['errors']) - 5} more errors\n"
            else:
                body_text += "No errors encountered.\n"
            
            body_text += "\n--- END OF REPORT ---"
            
            success_color = "green" if len(self.execution_stats['errors']) == 0 else "orange"
            status_text = "SUCCESS" if len(self.execution_stats['errors']) == 0 else "COMPLETED WITH ERRORS"
            
            body_html = f"""
<html>
<head>
    <style>
        body {{ font-family: Arial, sans-serif; margin: 20px; }}
        .header {{ background-color: #f0f0f0; padding: 15px; border-radius: 5px; margin-bottom: 20px; }}
        .section {{ margin-bottom: 20px; border-left: 4px solid #4CAF50; padding-left: 15px; }}
        .stats-table {{ border-collapse: collapse; width: 100%; margin: 15px 0; }}
        .stats-table th, .stats-table td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
        .stats-table th {{ background-color: #f2f2f2; }}
        .error {{ color: #d32f2f; }}
        .success {{ color: {success_color}; font-weight: bold; }}
    </style>
</head>
<body>
    <div class="header">
        <h1>BigBasket Automation Workflow Summary</h1>
        <p><strong>Execution Time:</strong> {self.execution_stats['start_time'].strftime('%Y-%m-%d %H:%M:%S') if self.execution_stats['start_time'] else 'N/A'}</p>
        <p><strong>Duration:</strong> {duration_str}</p>
        <p><strong>Days Back Parameter:</strong> {self.gmail_config['days_back']}</p>
        <p class="success">Status: {status_text}</p>
    </div>
    
    <div class="section">
        <h2>Gmail to Drive Workflow</h2>
        <table class="stats-table">
            <tr><th>Metric</th><th>Count</th></tr>
            <tr><td>Emails checked</td><td>{self.execution_stats['emails_checked']}</td></tr>
            <tr><td>Attachments found</td><td>{self.execution_stats['attachments_found']}</td></tr>
            <tr><td>Attachments uploaded</td><td>{self.execution_stats['attachments_saved']}</td></tr>
            <tr><td>Attachments skipped</td><td>{self.execution_stats['attachments_skipped']}</td></tr>
            <tr><td>Attachments failed</td><td>{self.execution_stats['attachments_failed']}</td></tr>
        </table>
    </div>
    
    <div class="section">
        <h2>Drive to Sheets Workflow</h2>
        <table class="stats-table">
            <tr><th>Metric</th><th>Count</th></tr>
            <tr><td>Files found</td><td>{self.execution_stats['files_found']}</td></tr>
            <tr><td>Files processed</td><td>{self.execution_stats['files_processed']}</td></tr>
            <tr><td>Files skipped</td><td>{self.execution_stats['files_skipped']}</td></tr>
            <tr><td>Files failed</td><td>{self.execution_stats['files_failed']}</td></tr>
            <tr><td>Duplicate records removed</td><td>{self.execution_stats['duplicates_removed']}</td></tr>
        </table>
    </div>
    
    <div class="section">
        <h2>Errors Summary</h2>
        <p><strong>Total errors:</strong> {len(self.execution_stats['errors'])}</p>
"""
            
            if self.execution_stats['errors']:
                body_html += "<ul>"
                for i, error in enumerate(self.execution_stats['errors'][:5], 1):
                    body_html += f'<li class="error">{error}</li>'
                if len(self.execution_stats['errors']) > 5:
                    body_html += f'<li>... and {len(self.execution_stats['errors']) - 5} more errors</li>'
                body_html += "</ul>"
            else:
                body_html += '<p style="color: green;">No errors encountered.</p>'
            
            body_html += """
    </div>
    
    <hr>
    <p><em>This is an automated email from BigBasket Automation Scheduler.</em></p>
</body>
</html>
"""
            
            try:
                profile = self.gmail_service.users().getProfile(userId='me').execute()
                user_email = profile.get('emailAddress')
            except Exception:
                user_email = "automation@bigbasket.com"
            
            for recipient in self.email_recipients:
                to_email = user_email if recipient == 'me' else recipient
                try:
                    message = self._create_email_message(
                        sender=user_email,
                        to=to_email,
                        subject=subject,
                        body_text=body_text,
                        body_html=body_html
                    )
                    
                    self.gmail_service.users().messages().send(
                        userId='me',
                        body=message
                    ).execute()
                    
                    logger.info(f"Email notification sent to {to_email}")
                except Exception as e:
                    logger.error(f"Failed to send email to {to_email}: {str(e)}")
            
            logger.info("Email notifications sent successfully!")
            
        except Exception as e:
            logger.error(f"Failed to send email notification: {str(e)}")
    
    def run_complete_workflow(self):
        """Run both workflows, log summary, and send email notification"""
        try:
            self.execution_stats = {
                'start_time': datetime.now(),
                'end_time': None,
                'emails_checked': 0,
                'attachments_found': 0,
                'attachments_saved': 0,
                'attachments_skipped': 0,
                'attachments_failed': 0,
                'files_found': 0,
                'files_processed': 0,
                'files_skipped': 0,
                'files_failed': 0,
                'duplicates_removed': 0,
                'errors': []
            }
            
            logger.info("\n" + "="*80)
            logger.info("STARTING COMPLETE BIGBASKET AUTOMATION WORKFLOW")
            logger.info("="*80 + "\n")
            
            if not self.authenticate():
                logger.error("Authentication failed. Aborting workflow.")
                return False
            
            gmail_result = self.process_gmail_workflow()
            excel_result = self.process_excel_workflow()
            
            self.execution_stats['end_time'] = datetime.now()
            
            logger.info("\n" + "="*80)
            logger.info("WORKFLOW EXECUTION SUMMARY")
            logger.info("="*80)
            logger.info(f"Start Time: {self.execution_stats['start_time'].strftime('%Y-%m-%d %H:%M:%S')}")
            logger.info(f"End Time: {self.execution_stats['end_time'].strftime('%Y-%m-%d %H:%M:%S')}")
            logger.info(f"Duration: {self.execution_stats['end_time'] - self.execution_stats['start_time']}")
            logger.info(f"Days Back Parameter: {self.gmail_config['days_back']}")
            logger.info("--- GMAIL TO DRIVE ---")
            logger.info(f"Emails Checked: {self.execution_stats['emails_checked']}")
            logger.info(f"Attachments Found: {self.execution_stats['attachments_found']}")
            logger.info(f"Attachments Saved: {self.execution_stats['attachments_saved']}")
            logger.info(f"Attachments Skipped: {self.execution_stats['attachments_skipped']}")
            logger.info(f"Attachments Failed: {self.execution_stats['attachments_failed']}")
            logger.info("--- DRIVE TO SHEETS ---")
            logger.info(f"Files Found: {self.execution_stats['files_found']}")
            logger.info(f"Files Processed: {self.execution_stats['files_processed']}")
            logger.info(f"Files Skipped: {self.execution_stats['files_skipped']}")
            logger.info(f"Files Failed: {self.execution_stats['files_failed']}")
            logger.info(f"Duplicates Removed: {self.execution_stats['duplicates_removed']}")
            logger.info(f"Total Errors: {len(self.execution_stats['errors'])}")
            if self.execution_stats['errors']:
                logger.info("Error Details:")
                for error in self.execution_stats['errors']:
                    logger.info(f"  - {error}")
            logger.info("="*80 + "\n")
            
            self._log_execution_summary()
            self._send_email_notification()
            
            logger.info("Complete workflow finished successfully!\n")
            return True
            
        except Exception as e:
            logger.error(f"Workflow execution failed: {str(e)}")
            if self.execution_stats['start_time']:
                self.execution_stats['end_time'] = datetime.now()
                self.execution_stats['errors'].append(f"Critical error: {str(e)}")
                try:
                    self._log_execution_summary()
                    self._send_email_notification()
                except:
                    pass
            return False

def main():
    """Main function to set up and run the scheduler"""
    import argparse
    
    parser = argparse.ArgumentParser(description='BigBasket Automation Scheduler')
    parser.add_argument('--once', action='store_true', help='Run once and exit (for GitHub Actions)')
    parser.add_argument('--interval', type=int, default=3, help='Interval in hours (default: 3, for local use)')
    args = parser.parse_args()
    
    logger.info("="*80)
    logger.info("BIGBASKET AUTOMATION SCHEDULER STARTED")
    logger.info("="*80)
    
    # Auto-detect GitHub Actions environment
    if os.environ.get('GITHUB_ACTIONS') == 'true':
        args.once = True
        logger.info("Running in GitHub Actions mode (once)")
    else:
        logger.info(f"Running in local mode (every {args.interval} hours)")
    
    logger.info("Workflows: Gmail → Drive → Sheets")
    logger.info("Email Notifications: keyur@thebakersdozen.in and self")
    logger.info("="*80 + "\n")
    
    scheduler = BigBasketScheduler(run_once=args.once)
    
    if args.once:
        logger.info("Running single workflow execution...")
        success = scheduler.run_complete_workflow()
        if success:
            logger.info("Workflow completed successfully!")
        else:
            logger.error("Workflow completed with errors!")
    else:
        schedule.every(args.interval).hours.do(scheduler.run_complete_workflow)
        
        logger.info("Running initial workflow execution...")
        scheduler.run_complete_workflow()
        
        logger.info("\nScheduler is now running. Press Ctrl+C to stop.\n")
        
        try:
            while True:
                schedule.run_pending()
                time.sleep(60)
        except KeyboardInterrupt:
            logger.info("\n" + "="*80)
            logger.info("SCHEDULER STOPPED BY USER")
            logger.info("="*80)

if __name__ == "__main__":
    main()
