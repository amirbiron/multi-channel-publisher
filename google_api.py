"""
google_api.py — פונקציות עזר ל-Google Sheets ו-Google Drive
"""

import io
import logging
import threading
from typing import Optional

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

from config import (
    GOOGLE_SCOPES,
    SPREADSHEET_ID,
    SHEET_NAME,
    get_google_sa_info,
)

logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════
#  Google Clients — thread-local (httplib2 is NOT thread-safe)
# ═══════════════════════════════════════════════════════════════

_local = threading.local()


def _get_credentials():
    sa_info = get_google_sa_info()
    return service_account.Credentials.from_service_account_info(
        sa_info, scopes=GOOGLE_SCOPES
    )


def get_sheets_service():
    svc = getattr(_local, "sheets_service", None)
    if svc is None:
        creds = _get_credentials()
        svc = build("sheets", "v4", credentials=creds)
        _local.sheets_service = svc
    return svc


def get_drive_service():
    svc = getattr(_local, "drive_service", None)
    if svc is None:
        creds = _get_credentials()
        svc = build("drive", "v3", credentials=creds)
        _local.drive_service = svc
    return svc


# ═══════════════════════════════════════════════════════════════
#  Sheets — קריאה ועדכון
# ═══════════════════════════════════════════════════════════════

def sheets_read_all_rows() -> tuple[list[str], list[list[str]]]:
    """
    קורא את כל הטבלה.
    מחזיר (header, rows) — כאשר rows הוא רשימה של רשימות (שורות).
    """
    svc = get_sheets_service()
    rng = f"{SHEET_NAME}!A:Z"

    resp = (
        svc.spreadsheets()
        .values()
        .get(spreadsheetId=SPREADSHEET_ID, range=rng)
        .execute()
    )

    values = resp.get("values", [])
    if not values:
        return [], []

    header = values[0]
    rows = values[1:]
    return header, rows


def sheets_update_cell(row_number: int, col_letter: str, value: str):
    """
    מעדכן תא בודד בטבלה.
    row_number: מספר שורה (1-based, כולל header).
    col_letter: אות העמודה (A, B, C...).
    """
    svc = get_sheets_service()
    rng = f"{SHEET_NAME}!{col_letter}{row_number}"

    svc.spreadsheets().values().update(
        spreadsheetId=SPREADSHEET_ID,
        range=rng,
        valueInputOption="RAW",
        body={"values": [[value]]},
    ).execute()

    logger.debug(f"Updated {rng} = {value!r}")


def sheets_update_cells(row_number: int, updates: dict[str, str], header: list[str]):
    """
    מעדכן מספר תאים באותה שורה בקריאת batch אחת.
    updates: {column_name: value}
    """
    svc = get_sheets_service()
    data = []

    for col_name, value in updates.items():
        col_letter = col_letter_from_header(header, col_name)
        rng = f"{SHEET_NAME}!{col_letter}{row_number}"
        data.append({"range": rng, "values": [[value]]})

    if data:
        svc.spreadsheets().values().batchUpdate(
            spreadsheetId=SPREADSHEET_ID,
            body={"valueInputOption": "RAW", "data": data},
        ).execute()

        logger.debug(f"Batch updated row {row_number}: {list(updates.keys())}")


def sheets_read_row(row_number: int) -> list[str]:
    """
    קורא שורה בודדת מהטבלה (1-based, כולל header).
    מחזיר רשימת ערכים (או רשימה ריקה אם אין נתונים).
    """
    svc = get_sheets_service()
    rng = f"{SHEET_NAME}!A{row_number}:Z{row_number}"

    resp = (
        svc.spreadsheets()
        .values()
        .get(spreadsheetId=SPREADSHEET_ID, range=rng)
        .execute()
    )

    values = resp.get("values", [])
    return values[0] if values else []


def col_letter_from_header(header: list[str], col_name: str) -> str:
    """
    ממיר שם עמודה לאות (A, B, C...).
    תומך עד 26 עמודות (A-Z). אם צריך יותר — להרחיב.
    """
    try:
        idx = header.index(col_name)
    except ValueError:
        raise ValueError(f"Column '{col_name}' not found in header: {header}")

    if idx > 25:
        raise ValueError(f"Column index {idx} exceeds single-letter range (A-Z)")

    return chr(ord("A") + idx)


# ═══════════════════════════════════════════════════════════════
#  Drive — הורדת קובץ + metadata
# ═══════════════════════════════════════════════════════════════

def drive_get_file_metadata(file_id: str) -> dict:
    """
    מביא metadata של קובץ מ-Drive (שם, MIME type, גודל).
    """
    svc = get_drive_service()
    return (
        svc.files()
        .get(fileId=file_id, fields="id,name,mimeType,size", supportsAllDrives=True)
        .execute()
    )


def drive_get_media_metadata(file_id: str) -> dict:
    """
    מביא metadata מורחב של קובץ מדיה מ-Drive — כולל מימדי תמונה/וידאו.

    imageMediaMetadata: width, height, rotation
    videoMediaMetadata: width, height, durationMillis
    """
    svc = get_drive_service()
    return (
        svc.files()
        .get(
            fileId=file_id,
            fields="id,name,mimeType,size,imageMediaMetadata,videoMediaMetadata",
            supportsAllDrives=True,
        )
        .execute()
    )


def drive_download_bytes(file_id: str) -> bytes:
    """
    מוריד את תוכן הקובץ כ-bytes מ-Drive (alt=media).
    """
    svc = get_drive_service()
    request = svc.files().get_media(fileId=file_id, supportsAllDrives=True)

    buffer = io.BytesIO()
    downloader = MediaIoBaseDownload(buffer, request)

    done = False
    while not done:
        status, done = downloader.next_chunk()
        if status:
            logger.debug(f"Drive download {file_id}: {int(status.progress() * 100)}%")

    logger.info(f"Downloaded {file_id} ({buffer.tell()} bytes)")
    return buffer.getvalue()


def drive_download_with_metadata(file_id: str) -> tuple[bytes, dict]:
    """
    מוריד קובץ + metadata בפעולה אחת (נוח לזיהוי סוג קובץ).
    """
    metadata = drive_get_file_metadata(file_id)
    file_bytes = drive_download_bytes(file_id)
    return file_bytes, metadata


def drive_list_folder(folder_id: str, page_token: Optional[str] = None) -> dict:
    """
    מחזיר רשימת קבצים ותיקיות בתוך תיקייה ב-Drive.
    תומך ב-pagination.
    """
    svc = get_drive_service()
    query = f"'{folder_id}' in parents and trashed = false"
    fields = "nextPageToken, files(id, name, mimeType, size, thumbnailLink, createdTime)"

    result = (
        svc.files()
        .list(
            q=query,
            fields=fields,
            pageSize=50,
            pageToken=page_token,
            orderBy="name",
            supportsAllDrives=True,
            includeItemsFromAllDrives=True,
        )
        .execute()
    )

    return {
        "files": result.get("files", []),
        "nextPageToken": result.get("nextPageToken"),
    }


def sheets_append_row(values: list[str]):
    """מוסיף שורה חדשה בסוף הטבלה."""
    svc = get_sheets_service()
    rng = f"{SHEET_NAME}!A:Z"

    svc.spreadsheets().values().append(
        spreadsheetId=SPREADSHEET_ID,
        range=rng,
        valueInputOption="RAW",
        insertDataOption="INSERT_ROWS",
        body={"values": [values]},
    ).execute()

    logger.info(f"Appended row with {len(values)} cells")


def sheets_delete_row(row_number: int):
    """
    מוחק שורה מהטבלה (1-based, כולל header).
    משתמש ב-batchUpdate עם deleteDimension.
    """
    svc = get_sheets_service()

    # קבלת sheet ID (לא spreadsheet ID)
    spreadsheet = svc.spreadsheets().get(
        spreadsheetId=SPREADSHEET_ID,
        fields="sheets.properties"
    ).execute()

    sheet_id = None
    for sheet in spreadsheet["sheets"]:
        if sheet["properties"]["title"] == SHEET_NAME:
            sheet_id = sheet["properties"]["sheetId"]
            break

    if sheet_id is None:
        raise ValueError(f"Sheet '{SHEET_NAME}' not found")

    svc.spreadsheets().batchUpdate(
        spreadsheetId=SPREADSHEET_ID,
        body={
            "requests": [{
                "deleteDimension": {
                    "range": {
                        "sheetId": sheet_id,
                        "dimension": "ROWS",
                        "startIndex": row_number - 1,  # 0-based
                        "endIndex": row_number,
                    }
                }
            }]
        },
    ).execute()

    logger.info(f"Deleted row {row_number}")
