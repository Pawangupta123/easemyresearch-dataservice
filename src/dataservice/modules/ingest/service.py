"""An upload's preview (skeleton step 7b): read the xlsx Node's presigned upload wrote, decide its
columns with Node's own rules (detect/), and hand back what Node stores as the preview."""

import zipfile
from urllib.parse import urlparse

from bson import ObjectId
from python_calamine import CalamineError

from dataservice.common.errors import ValidationError
from dataservice.modules.ingest.detect.cells import read_first_sheet, validate_headers
from dataservice.modules.ingest.detect.preview import ai_samples, preview_fields
from dataservice.modules.ingest.schemas import UploadDetected
from dataservice.storage.staging import read_staged_bytes

UPLOAD_PREFIX = "imports/"


def check_source(source: str, import_bucket: str) -> None:
    """Only Node's uploads: the import bucket, under imports/ — never any other object."""
    parsed = urlparse(source)
    key = parsed.path.lstrip("/")
    if parsed.scheme != "s3" or parsed.netloc != import_bucket or not key.startswith(UPLOAD_PREFIX) or ".." in key:
        raise ValidationError("The upload is not in the import bucket")


def detect_upload(data: bytes) -> UploadDetected:
    """The preview of one workbook's bytes; a header row Node would refuse comes back refused."""
    try:
        sheet = read_first_sheet(data)
    except (zipfile.BadZipFile, ValueError, OSError, CalamineError) as error:
        raise ValidationError("The file could not be read as an XLSX workbook") from error
    if sheet is None:
        return _refused("XLSX file must contain at least one sheet")
    header_cells = sheet.row(0) if sheet.row_count else []
    if not header_cells:
        return _refused("Header row is missing in the XLSX file")
    headers = validate_headers(header_cells)
    if not headers["ok"]:
        return UploadDetected.model_validate({"headers": headers, "fields": [], "samples": []})
    fields = preview_fields(sheet, lambda: str(ObjectId()))
    return UploadDetected.model_validate({"headers": headers, "fields": fields, "samples": ai_samples(sheet, fields)})


def detect_from_source(source: str, import_bucket: str, max_bytes: int) -> UploadDetected:
    check_source(source, import_bucket)
    data = read_staged_bytes(source)
    if len(data) > max_bytes:
        raise ValidationError(f"The upload is larger than {max_bytes // (1024 * 1024)} MB")
    return detect_upload(data)


def _refused(message: str) -> UploadDetected:
    return UploadDetected.model_validate({"headers": {"ok": False, "message": message}, "fields": [], "samples": []})
