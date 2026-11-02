"""An upload's preview over HTTP: Node's reserved dataset, the import bucket only, Node's shapes."""

import re
import time
from io import BytesIO
from pathlib import Path

import pytest
from fastapi.testclient import TestClient
from openpyxl import Workbook

from dataservice.common.errors import ValidationError
from dataservice.common.utils.dataset_token import DatasetClaims, sign
from dataservice.config import Settings, get_settings
from dataservice.main import create_app
from dataservice.modules.ingest import service

SECRET = "s" * 40
DATASET = "5eed000000000000000f4240"
FIXTURES = Path(__file__).resolve().parent.parent / "fixtures" / "import_golden"


def _xlsx(rows: list[list[object]]) -> bytes:
    workbook = Workbook()
    sheet = workbook.active
    assert sheet is not None
    for row in rows:
        sheet.append(row)
    out = BytesIO()
    workbook.save(out)
    return out.getvalue()


def test_a_workbook_comes_back_as_nodes_preview_with_object_ids() -> None:
    detected = service.detect_upload((FIXTURES / "categories.xlsx").read_bytes())
    assert detected.headers.ok
    assert [field.field_name for field in detected.fields][:2] == ["Smoker", "Sex"]
    assert all(re.fullmatch(r"[0-9a-f]{24}", field.temp_field_id) for field in detected.fields)
    assert [sample.temp_field_id for sample in detected.samples] == [field.temp_field_id for field in detected.fields]
    smoker = detected.model_dump(by_alias=True)["fields"][0]
    assert smoker["detectedType"] == "radio" and smoker["typeMetrics"]["nonEmptyCount"] == 60


def test_a_header_row_node_refuses_comes_back_refused() -> None:
    detected = service.detect_upload(_xlsx([["Age", None, "Sex"], [1, 2, "M"]]))
    assert not detected.headers.ok and detected.fields == []
    assert detected.headers.message == "Column 2 header cannot be empty"


def test_a_file_that_is_not_a_workbook_is_refused() -> None:
    with pytest.raises(ValidationError, match="could not be read"):
        service.detect_upload(b"not a zip")


@pytest.mark.parametrize(
    "source",
    [
        "s3://other-bucket/imports/u/file.xlsx",
        "s3://imports-bucket/datasets/x/table/part.parquet",
        "s3://imports-bucket/imports/../secrets.xlsx",
        "C:/local/file.xlsx",
    ],
)
def test_only_uploads_in_the_import_bucket_are_read(source: str) -> None:
    with pytest.raises(ValidationError, match="not in the import bucket"):
        service.check_source(source, "imports-bucket")


def test_the_route_takes_a_job_token_for_the_reserved_dataset() -> None:
    app = create_app()
    app.dependency_overrides[get_settings] = lambda: Settings(service_token_secret=SECRET, import_bucket="b")
    client = TestClient(app)
    body = {"source": "s3://elsewhere/imports/x.xlsx"}
    now = int(time.time())
    read = sign(SECRET, DatasetClaims("u", DATASET, "read", "all"), now)
    job = sign(SECRET, DatasetClaims("u", DATASET, "job", "all"), now)
    url = f"/v1/datasets/{DATASET}/upload:detect"
    assert client.post(url, json=body).status_code == 401
    assert client.post(url, json=body, headers={"Authorization": f"Bearer {read}"}).status_code == 403
    refused = client.post(url, json=body, headers={"Authorization": f"Bearer {job}"})
    assert refused.status_code == 400 and refused.json()["message"] == "The upload is not in the import bucket"
