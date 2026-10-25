# Walking skeleton plan (draft, 2026-10-25)

Status: **approved 2026-10-25** (allow-list only, small FE change, 10 MB cap, local Docker Mongo on 27018, never Atlas, never the .env.test DB). The critic ran; its 31 corrections are in [Critic corrections](#critic-corrections) and override the step text below where they differ.

Progress:
- Step 0: done (branches exp/skeleton and exp/dataservice-skeleton, local only).
- Step 1: done, except the contract DTOs and fixtures, which move into step 2 (they belong with the first endpoints).
  - Token: Python 47d0415, Node fb4ccb10.
  - Gateway: Node fb4ccb10.
  - A Node-signed token was verified by Python, and refused once expired.
- Step 2: done.
  - Reader: row meta, newest first, given row ids (`efb32cd`).
  - Endpoints rows:query / rows:count / cells:read (`17efab4`).
  - Fenced create + seed probe, 1k and 1M rows (`25e224a`).
  - Changes from the step text:
    - The seed writes only Delta plus `seed.json`; Node seeds Mongo (M16).
    - The manifest requires a base for now: a Data Service dataset gets its base from its first import.
    - Filters and sort are not taken yet; unknown keys are refused.
  - Moved on:
    - The exported contract and its fixtures go to the start of step 3, where Node's Zod reads them.
    - `datasetId` as an ObjectId goes to the first test of step 4 (H2).
  - Timings: STATUS §1 "Skeleton query service".

## Summary

VERDICT. Plan B is the base and Plan A supplies the grafts.

Scores out of 10, on (a) proves the risky parts / (b) smallest work / (c) no throwaway code / (d) respects the decisions:

**Plan A: 7 / 9 / 5 / 7.**
- It reads first and does the patch edit early, which is good.
- Throwaway code:
  - the scaffold x-service-token, although STATUS.md:62/232 says the scoped token comes with the first internal endpoint;
  - storage branches inside the legacy /imports services, although api.md §1.3 and plan 2.6 put new forms on /datasets and keep Node's import module for mongo forms;
  - a bootstrap publish that plan 2.1 replaces;
  - FieldSpecs read from the Delta schema instead of the revision's schema.
- A new modules/dataset would sit beside the Phase 1 home src/v2/common/datastore/, which breaks 'extend existing modules'.

**Plan B: 8 / 4 / 9 / 9.**
- Everything is production-shaped.
- But the visible grid and the edit only arrive at steps 11–12, behind 13 collections, quota reservations, YAML codegen and the full job/callback stack.

**Claims checked against the code (read-only):**
- B is right that quota truth is the live sum of Form.entryCount: payment-quota.repository.ts:49 sums it and :78/:221 overwrite used_entries with it.
- B is right that the HTTP bulk-edit passes no mutationId: core.controller.ts:290-301 sends only userId, isOwner, part and validateOnly; only an AI-supplied context.mutationId reaches records-core-bulk.service.ts:225.
- Grid default order is createdAt desc, _id desc (db-filter.helper.ts:280).
- The cursor is keyset: recordId + createdAt + sort context, v1. #decodeGridCursor (records-core-read.service.ts:564) ignores unknown keys, so adding `revision` is safe.
- createFormDocument already lets data override storage (form-write.repository.ts:146).
- The grid row mapper needs userId, createdAt AND updatedAt (row-transform.helper.ts:206-209). B left out _updated_at; A had it.
- Plan B was wrong on two points:
  - The existing import is not '10 MB through Node'. The FE uploads straight to S3 with a presigned URL (import.routes.ts:22; cap IMPORT_MAX_FILE_SIZE = 10 MB).
  - FIELD_ID already accepts 24-hex ids (snapshot.py:22 `[A-Za-z0-9_]{1,64}`).
- In the reader:
  - The row key is already `_rid` (a string; model.py ROW_ID) and there is no `_row_id`.
  - Ties always break on `_rid` ASC (read.py:134).
  - Only `_rid` plus the fields are projected (read.py:137), so meta columns and a descending row order are real gaps.
  - Snapshot.fields is required (snapshot.py:72), so field types must come from somewhere. Following the design, Node sends them in the manifest from dataset_schemas.
- The FE repo (for B's recordsMutation.api.ts claim) is not at the Desktop path. I did not search further (no home-dir search).

**FINAL PLAN = B's contracts and module homes, put into A's read-first order, cut to the skeleton:**
1. A seeded Delta dataset is visible through Node's real grid route and the real scoped token.
2. A cell edit through bulk-edit, as one Mongo transaction, then appears in the grid.
3. The guard and the route census close silent empty reads before any real form can be created.
4. Only then come the real write path (lease/operation/publish port), detect/validate/ingest behind production-shaped /datasets routes in the import module, and the FE wiring.

Things that stay out of this run are named one by one in out_of_scope. Code: Node in src/v2/common/datastore/dataservice/, and Python in new modules/datasets and modules/ingest plus clients/node_authority.py. Both on new local branches that are never pushed.

## Critic corrections

The critic checked this plan against both codebases on 2026-10-25. Each correction is listed under the step it changes. Numbers are the critic's (H = high, M = medium, L = low).

**Step 1 (contract, env)**
- H2: pin the BSON types of `dataset_patches`.
  - `datasetId` is an ObjectId; Python converts it (mongo_patches.py queries a str today).
  - Row, field and option ids are 24-hex strings.
  - `num` is a double; a date is a midnight-UTC BSON Date.
  - One shared patch-document fixture.
- M19: env names in both .env.example files.
  - Python `SERVICE_TOKEN_SECRET` = Node `DATA_SERVICE_TOKEN_SECRET`.
  - Python still needs Node's base URL (PORT defaults to 5001), the callback secret, and the import bucket and credentials (Python uses AWS_*, Node uses AWS_S3_*).
- M21: copy the contract fixtures into Node's tests/fixtures, with a sync check. Node hooks cannot read a sibling repo.
- L29: /v1/jobs carries datasetId in the body, so it needs a body-based token check, not the path one.

**Step 2 (query endpoints, seed)**
- M16: Python seeds only Delta. A Node script seeds the Mongo docs.
  - The form needs createdBy, status, deletedAt, title, fields with an ObjectId `_id`, options with fieldId, mainForm and entryCount.
  - Otherwise the row mapper drops every cell.
- M17: auth needs a seeded User, Role, an active Session, and a successful Payment with a plan. Or log in for real against the docker DB.
- M18: local Mongo URL.
  - Node: `mongodb://localhost:27018?replicaSet=rs0&directConnection=true` (no slash) plus DB_NAME. Python's db name must be the same.
  - Set a separate REDIS_DB, so BullMQ jobs do not reach the Atlas-backed dev workers.
- L26: the seed carries `_created_by`, `_created_at` and `_updated_at`. Compaction must keep them (`_updated_at` from the folded patches).
- M9 (part): ManifestDto carries `tables.followups` and the visit fields now, not a literal null.

**Step 3 (grid read)**
- H1: take the storage branch from `req.formAccess.storage`, passed through the controller context. `findFormWithStructure` does not select `storage`/`datasetId`; add them too. Test through the real repository select.
- M13: cursor.
  - Extend the decoder to carry `revision` and `afterRid`.
  - rows:query gets `afterRid` (keyset), not only an offset.
  - Offset mode is not pinned to a revision; document that.
- L24: the read token's rowScope is 'all' for every viewer. Creator scope applies only to edits.
- L25: for an unfiltered grid, total = manifest rowCount; no rows:count call.
- M15: the FE cannot see `storage`. Add it as a new response field (formMetadata, get-form and list selects) and report the FE impact.
- L31: the reader already supports filters and sort, so cutting them is a choice. Port them here if cheap.

**Step 4 (cell edit)**
- H2: the first test is the cross-repo "Node writes a patch, rows:query reads it".
- M8: `validateOnly` dry runs read `records`. Validate through cells:read without writing, or refuse.
- M9: refuse bulk-edit entries that carry a followUpNumber in the skeleton, so a visit edit never overwrites the main cell.
- M10: skip `revisionBulkCaptureService.captureMany` and do not write RevisionBatch (batches map to `dataset_changes.batchId`).
- M11: bump `Form.dataVersion` with `$inc`/`$max`, never set it to the revision number.
- M14: add `Idempotency-Key` to the CORS allowed headers before the FE sends it.
- M20: Node e2e runs on a separate opt-in vitest config and env file for the docker DB (name contains "test"), outside pre-push `test:e2e`. Never .env.test, which points at Atlas.
- M22: reusing bulk-edit validation needs entry-shaped before-values from cells:read and a form with followUps. Derived output (`applyDerived`) is refused or stripped.

**Step 5 (guards)**
- H3: form-module routes read `records` to decide what is safe, and would see a Data Service form as empty.
  - form-update: `existsByFormId`, `findUsedOptionIds`, `migrateAllowedTypeChanges`, column archives.
  - form-field-patch: `countOptionUsage`.
  - Also unique-identifier and convert-to-manual.
  - These routes change form.fields without touching dataset_schemas.
  - Base the guard list on the static inventory (node-phase1-audit.md §2, 107 call sites in 43 files), not only on a log census. It includes doc-extract commit, AI record tools, doc-build, graphs and reports, and the Python analysis service, which reads `records` directly.
- H4: add-row, columns and the per-row revision routes have no checkFormPermission, so `requireMongoStorage` would return 500 there. Guard them in the service with `findFormStorage`. toggle-hidden stays open (display-only).
- M12: specify each DataServiceGlobalCounts method and test it. `countActiveRows(since)` cannot come from `datasets.rowCount`.
- L28: FeatureNotAvailableError is 422, not 400.

**Step 6 (lease, operations, publish)**
- H5: mount raw-body capture for /api/v2/internal before `express.json` (HMAC over Python's bytes; mongoSanitize changes bodies), and exempt that path from the 100/min rate limiter.
- M23: the dataset-drain job (jobId = datasetId) needs `removeOnComplete` and `removeOnFail` set to true, or a kept job swallows the next wake-up.

**Step 7 (detect, validate, ingest)**
- H6: POST /datasets reuses `uploadImportReferenceSchema` and the upload service's checks:
  - the `imports/<userId>/` prefix;
  - MIME type;
  - HEAD size of 10 MB;
  - `AWS_S3_IMPORT_BUCKET`.
  - Pass bucket and key to Python.
- H7: check the form quota before creating the form, or create it at confirm as import-finalize does. An entry-capacity check on xlsx confirm is new behaviour, so mark it so.
- L27: say which legacy import endpoints the modal needs (preview-data, cancel, import-status).

**Plan status**
- L30: step 0 is done and STATUS §0 points at exp/skeleton (fixed by this edit).

## Steps

### 0. [both] Experiment branches, env names, one Mongo

Node: new local branch exp/dataservice-skeleton off feat/datastore-phase1 (print the branch first; the user switches branches mid-session). Python: exp/skeleton off feat/lease-fence. Neither is ever pushed. New env names only; no .env file is read. Node env.config.ts: DATA_SERVICE_ENABLED, DATA_SERVICE_URL, DATA_SERVICE_TOKEN_SECRET (min 32), DATA_SERVICE_CALLBACK_SECRET, DATA_SERVICE_TIMEOUT_MS, DATA_SERVICE_USER_IDS (opt-in allow-list, read only when ENABLED). Python Settings: the same secrets, mongodb_url/mongodb_db (read-only for dataset_patches), lake_root (local path, or s3://<testing bucket>/dataservice-probe/skeleton/). Both services use the docker replica set on 27018 with its own dev DB plus a seeded user and payment. Run: `uv run python -m granian --interface asgi dataservice.main:app --port 8100`.

**Proves:** Both services can share one Mongo with transactions, without touching Atlas (over quota) or the .env.test DB (e2e wipes it).

**Tests:** Python: test_config covers the new settings. Node: env schema unit test (missing secret → fails at boot only when DATA_SERVICE_ENABLED).

### 1. [both] Contract DTOs, scoped service token, DataServiceGateway

Contract (plan 0.7, lightweight):
- Pydantic models in Python are the source: ManifestDto {datasetId, revision, tables:{base:{version}|null, followups:null}, patches:{fromSeq,toSeq}, fields:[{fieldId, valueType:'str'|'num'|'date'|'opts'}], rowCount, dataVersion}, plus the bodies of rows:query, rows:count and cells:read.
- FastAPI's OpenAPI is exported to contracts/dataservice-internal.openapi.json, together with contracts/fixtures/*.json request/response examples.
- Node: hand-written Zod in src/v2/types/dataservice.types.ts, plus a pure vitest that parses every fixture.

Token (api.md §2; STATUS.md says it comes with the first internal endpoint):
- Node: src/v2/common/datastore/dataservice/service-token.ts signs short-lived JWTs (jsonwebtoken) {sub:userId, datasetId, scope:'read'|'write'|'job', rowScope:'all'|userId, exp≤60s}.
- Python: common/auth.py verifies them on /v1/* with PyJWT and rejects a datasetId that differs from the path. /health keeps the scaffold token.

Transport (one HTTP client, no copy):
- Extend the existing src/v2/common/services/python-api.gateway.ts (155 lines) so PythonApiGateway takes optional {baseUrl, headers()}. The defaults stay PYTHON_SERVICE_URL + x-api-key, so analysis callers do not change.
- New src/v2/common/services/dataservice.gateway.ts: class DataServiceGateway on that transport, separate service URL (D13). It sends Authorization Bearer, x-request-id and traceparent. Typed methods: queryRows, countRows, readCells. The error envelope maps to ValidationError / FeatureNotAvailableError.

**Proves:** The Node↔Python contract and production auth from the first call (plan 0.5, security 8.4). No later rewrite of headers or DTOs.

**Tests:** Python unit: expired token, wrong datasetId and wrong scope give 401/403; the fixtures validate against the Pydantic models. Node unit: signer claims and expiry; gateway with axios mocked (header present, error mapping); Zod parses every fixture; the existing python-api.gateway tests stay green (defaults unchanged).

### 2. [python] Query endpoints over the one reader + seeded dataset (1k and 1M rows)

New module src/dataservice/modules/datasets/ (router, schemas, service), mounted in main.py:
- POST /v1/datasets/{id}/rows:query {manifest, columns?, offset, limit, rowOrder:'desc'|'asc'} → {revision, rows:[{_rid, createdBy, createdAt, updatedAt, values}]}.
- POST …/rows:count {manifest} → {revision, count}.
- POST …/cells:read {manifest, targets} → {revision, cells, rows:[{_rid, state, createdBy}]} (api.md §2.1; the before-values of an edit).
All three go through storage/reader read.query / count with the existing MongoPatchSource. A new pure snapshot_from_manifest(dto, lake_root) builds Snapshot.fields from the manifest; Python reads no Mongo collection except dataset_patches.

Reader changes, each one with mutation-style tests because the reader just passed the 55-agent review:
- (model.py/read.py) project _created_by, _created_at and _updated_at (base value; a patched cell sets _updated_at = the patch `at`);
- a row-order option, `_rid DESC` vs ASC (ObjectIds are time-ordered, which matches Node's createdAt desc, _id desc and covers patch-inserted rows);
- a `rids` filter through the existing PAGE_IDS semi-join, for cells:read.
Values serialise as: dates ISO yyyy-mm-dd, opts as option-id lists, null kept.

New storage/fenced_commit.create(uri, data, context, configuration=retention_configuration(days)), with txn token and ops metadata. It is the only creator; the architecture test is extended.

Seed: probes/seed_skeleton_dataset.py (1k and 1M rows) creates the base through fenced_commit.create (token 1). It also writes the Mongo docs Node needs, using the data-model.md §2 names: forms {storage:'dataservice', datasetId}, datasets {currentRevision:1, patchSeq:0, status:'ready'}, dataset_revisions rev 1, dataset_schemas.

**Proves:** A pinned-revision read over HTTP: Delta at the manifest version merged with patches in (fromSeq, toSeq] (C13/C19/C40 carried over the wire). The meta columns Node's row shape needs. A first honest latency number on 1M rows, local and S3.

**Tests:** Unit (extend test_reader_query.py): meta columns, desc order with ties, the rids filter, snapshot_from_manifest including null base. Integration (docker Mongo): TestClient rows:query/count/cells:read with patches inside and outside the range; a request without a token or with another dataset's token is refused. test_architecture stays green. Benchmark rows next to bench_reader numbers.

### 3. [node] Collections + grid read through DataServiceRowReader (first visible end-to-end)

Models under src/v2/common/datastore/dataservice/models/, names and indexes exactly as data-model.md §2, types in src/v2/types/datastore.types.ts. Skeleton subset: datasets, dataset_revisions, dataset_schemas, dataset_patches (unique datasetId+seq), dataset_changes, dataset_idempotency. form.model.ts / form.types.ts gain datasetId (ObjectId|null).

Shape-B capability `RowReader { listRows(form, q, scope), countRows(form, scope) }` in datastore.types.ts.

DataServiceRowReader (common/datastore/dataservice/dataservice-row-reader.ts):
- resolves datasets.currentRevision, or the revision carried in the cursor;
- loads that dataset_revisions doc and its dataset_schemas → ManifestDto;
- mints a 'read' token (rowScope = today's rule);
- calls queryRows and countRows in parallel.

Pure helper toRecordDocument(row, schema) builds the RecordLike that the EXISTING mapRecordWithContext (row-transform.helper.ts:140) takes: _id=_rid, userId=createdBy, createdAt, updatedAt, data[]={fieldId, type, value|optionIds}. The mapper and the response stay untouched. common/datastore/index.ts exports rowReaderFor(form).

records-core-read.service.ts getRecordsByFormId (line 119): after #getRequiredForm, isDataServiceForm(form) → #getDataServiceGrid. It reuses buildGridContextFromForm for columns and formMetadata. Search, filters, column sort and followUpNumbers → FeatureNotAvailableError (not-ported code). The cursor keeps today's v1 envelope plus a `revision` key (the decoder ignores unknown keys), so page 2 reads the same snapshot (consistency §12). The fire-and-forget FormMeasurementLevelAiService call (it reads records) is skipped for these forms.

GlobalCounts: DataServiceGlobalCounts sums datasets.rowCount (status ready), and globalRowCounts() composes it with MongoGlobalCounts. Callers do not change.

**Proves:** Node's real POST /forms/:formId/records returns Delta rows in the same {columns, rows, pagination, formMetadata} shape, revision-pinned. The FE grid opens the seeded form. End-to-end latency Node→Python→DuckDB on 1M rows.

**Tests:** Unit (gateway mocked): toRecordDocument per type (num null, date, single/multi choice, text); a snapshot that compares a Mongo fixture of the same rows with the Data Service path (identical apart from ids); the cursor round-trips the revision; filters and search are refused; branch selection by storage; the composed GlobalCounts (extend datastore-mongo-global-counts.test.ts); model schema introspection. The existing records-core-read tests stay green. Manual: curl with a dev JWT, then the FE grid.

### 4. [node] One cell edit = one Node edit transaction (bulk-edit branch)

Route unchanged: PATCH /forms/:formId/records/bulk-edit (core.routes.ts:210) → records-core-bulk.service.ts bulkEditRecords (line 161). When isDataServiceForm(form):
- reuse prepareService.normalize (pure) and the existing per-entry value validation for each field type (no derived work);
- call a new RowWriter capability `applyBatch(ctx, form, cellPatches)`.

DataServiceRowWriter (common/datastore/dataservice/) ports prototype/node_side/edit_store.py, following consistency §14.2:
1. Read n, then cells:read for before-values, row state and creator scope (owner: any row; collaborator: own rows, as today).
2. withTransaction:
   - CAS datasets.currentRevision n→n+1 (status 'ready') with $inc patchSeq;
   - insert dataset_patches {datasetId, seq, op:'set', target:{row, field, scope:{kind:'main', followUp:0}}, value:{t,v}, opId, batchId, actor, at};
   - dataset_changes before/after;
   - dataset_revisions n+1 (kind 'edit', same tables, patches.toSeq);
   - Form.dataVersion = n+1 through FormWriteRepository;
   - staleness through the SAME three repos (analysisStaleness / StatisticalTable / SurvivalAnalysis incrementVersionForColumns);
   - RevisionBatchRepository.upsertBatch with a new optional session (mutationId = batchId);
   - dataset_idempotency.
3. After commit: activityLog.logBulkEdit as today.
4. A CAS miss re-reads, up to a configured number of retries → DATASET_BUSY.

Idempotency: an optional, additive Idempotency-Key header (api.md §1.1). Body or context mutationId is used when present. Without either, the server makes a UUID.

An edit to the form's unique-identifier field → FeatureNotAvailableError (claims are Phase 3).

Response {updatedCount, totalRequested} unchanged. 'No matching records' / 404 semantics as today.

**Proves:** The hybrid-edit design on real Node: one Mongo transaction, and the next grid read shows the new value because the reader merges seq in (fromSeq, toSeq] (consistency §14.2/§15.1). Later-wins on concurrent edits. The first measured edit latency (cells:read + transaction; plan 0.10 / C38).

**Tests:** Unit (mocked repos and gateway): patch {t,v} per field type; collaborator scope; unique-field refusal; the CAS retry loop; same key + different payload → 409 IDEMPOTENCY_MISMATCH. e2e (test replica set, gateway stubbed for cells:read; real Python only behind an opt-in env): edit → revision n+1 + patch; two concurrent edits → both land and the later one wins; a retry with the same key writes nothing. Port the integration assertions of tests/integration/test_edits.py. Cross-repo smoke: a patch written by Node is read back by rows:query at n+1.

### 5. [node] Guard every unported route + route census

Add a NOT_PORTED_FEATURE catalogue next to MONGO_ONLY_FEATURE in common/constants. Widen requireMongoStorage (storage-guard.middleware.ts) to accept either catalogue; both answer FEATURE_NOT_AVAILABLE. Apply it after checkFormPermission on every records route off the skeleton path:
- submit, records-followup, add-row;
- single-row get/view/edit/file-access;
- delete, bulk-delete, restore, deleted list;
- record-ids, check-unique, schedule, toggle-hidden, columns, export-records;
- /revisions/batches/:mutationId/restore(-preview) and the per-row /revisions/record/... routes (RevisionBatch is written in step 4, but restore reads Mongo pre-images);
- analysis / statistical-table / survival entry points (transforms are already guarded in d4278f22).
Only the grid list and bulk-edit stay open.

Then the route census (Plan A): open the seeded form in the FE, collect every /api/v2 route it hits from the Node pino logs, and classify each one as works / guarded / reads empty records. Guard whatever is still silent (form-level lists, AI tools, measurement level, sample values). This must be done before step 7 makes Data Service forms creatable.

**Proves:** A Data Service form can only show the truth or a clear error, never a silent empty read of `records`.

**Tests:** One table-driven vitest over the route list: storage 'dataservice' → 400 FEATURE_NOT_AVAILABLE, 'mongo' or missing → passes. A route on the list that is missing its guard fails the test. The census table goes into dataservice docs/STATUS.md.

### 6. [both] Port the publish path: lease, operations, publish, writer door, drain

Node, in src/v2/common/datastore/dataservice/, logic one-to-one with prototype/node_side and the same CAS filters:
- dataset-lease.repository.ts ← lease_store.py;
- data-operation.repository.ts ← operation_store.py;
- dataset-publish.service.ts ← publish_store.py, with every consistency §6 effect the skeleton needs: lease row, rebase check against small edits, revision n+1, datasets currentRevision/rowCount/status, Form.dataVersion and Form.entryCount = rowCount (today's quota truth), dataset_schemas ref, staleness, RevisionBatch, operation applied + data_jobs succeeded. Models: dataset_leases, data_operations, data_jobs.
- Writer routes from api.md §3 (lease, manifest, operations?status=pending, operation get/payload/running/committed/failed/rolled-back, publish) as an internal route group under the record module. They sit behind a new src/v2/common/middleware/dataservice-signature.middleware.ts (HMAC-SHA256 over body + timestamp, timingSafeEqual) and the authority door check (live lease token, else 409 FENCED). Payload is inline references only; the encryption/S3 hook stays a TODO in one function.
- BullMQ queue dataset-drain (jobId = datasetId) plus the operation sweeper (C5), hosted in the EXISTING import worker process.

Python:
- src/dataservice/clients/node_authority.py implements the existing LeaseAuthority and OperationAuthority protocols over §3: HMAC-signed, 409 FENCED → FenceError, with a heartbeat that renews the lease.
- POST /v1/datasets/{id}:drain → 202, then recover() + drain() from modules/writer/service.py, one in-process lock per dataset.
- Fence and reconcile accept `base: null` (revision 0).

**Proves:** Node is the only publisher on its real code: C3, C5, C6, C15, C18, C28, C29 and C32 hold on Node, not only on the reference. Ingest (step 7) becomes a normal operation, not a one-off.

**Tests:** Node unit (mocked repos): every CAS branch of the status machine, door check, signature middleware (tampered body, old timestamp, missing header). Node e2e (test replica set): publish is idempotent (C15); a stale token is refused at the door; publish vs a concurrent acquire (C18). Cross-repo: Python's test_lease / test_operations / test_publish / test_drain run once more under a `node_http` marker against a Node test server. That is how the port is proven, so prototype/node_side can later be deleted.

### 7. [both] detect + :validate + ingest (D12) behind /datasets routes in the import module

Python, new module src/dataservice/modules/ingest/:
- detect job: reads the xlsx from S3 (python-calamine or openpyxl; no pyarrow.compute because of Windows App Control). It stages the file once to Parquet. Per column it runs DuckDB GROUP BY value, then ONE classifier per distinct value → a value map, plus types text/number/single-choice/date and options. The output is in today's ImportPreview fields + corrections shape (importPreview.model.ts).
- POST /v1/datasets/{id}:validate {uploadId, fields} → {issues, fields, draft}. Field ids and option ids are new ObjectId hex.
- Writer job `ingest`, registered in modules/writer/service.py JOBS beside append_rows. It casts the staged Parquet by joining on the value map with the draft's corrections, generates _rid ObjectIds in file order with _created_by/_created_at/_updated_at, and creates the base with fenced_commit.create. It is idempotent on opId and returns JobResult {rowsAdded, changedFieldIds}.
- POST /v1/jobs {jobId, type, datasetId, compute:'local', input, callbackUrl} → 202. The job runs in a worker thread of the query-api. Progress and terminal events go to Node's callback.
- Golden-vector subset first (plan 2.3): a Node vitest runs import-type-*.helper.ts, import-column-stats.helper.ts and import-options-canonical.helper.ts over fixture columns (number, text, single-choice, date, plus their issue messages) and writes tests/fixtures/import-golden/*.json, copied into the dataservice repo.

Node: a dataservice sub-route group inside the EXISTING src/v2/modules/import. The legacy import services stay untouched (D11).
- POST /datasets {fileKey from the existing /storage/presign-upload, formName}. One withTransaction: FormWriteRepository.createFormDocument({storage:'dataservice', datasetId}), datasets {status:'ingesting'}, dataset_revisions 0 {base:null}. Then data_jobs detect + POST /v1/jobs. Form quota is checked as import confirm does today; allowed only for DATA_SERVICE_USER_IDS.
- GET /datasets/:datasetId/preview: same shape as GET /imports/:id/preview.
- POST /datasets/:datasetId/confirm (today's confirm body), in order:
  1. auth and Zod;
  2. RecordsCoreQuotaService entry capacity check (as today);
  3. :validate; on issues → 400 with today's text and nothing persisted;
  4. one transaction: Form fields/options = the draft as is; dataset_schemas; data_operations `ingest` (payload refs only); data_jobs linked by opId;
  5. enqueue dataset-drain;
  6. answer {jobId, datasetId, formId}.
- GET /data-jobs/:jobId.
- POST /api/v2/internal/data-jobs/:jobId/events (HMAC; progress and status only; dedupe on attempt + seq; never applies effects).
- Every write route gets auth, checkFormPermission where a form exists, and an activity log entry.

**Proves:** Steps 1–2 of the user's path: detection and validation live in Python for new forms (D12), checked against Node's own rules. The first revision is published only by Node's publish transaction. Preview equals committed values, because the cast uses the detect value map.

**Tests:** Python unit: the classifier passes the golden subset; validate issue texts match Node's; cast per type and corrections; re-ingest with the same opId writes no second commit. Python integration: detect → validate → ingest → committed → Node fake publish called once; a crash before committed → rolled back and retried (C3/C20). Node unit (mocked gateway and repos): confirm order (quota before :validate, nothing persisted on issues), callback dedupe and ordering. e2e: create → preview → confirm writes form + schema + operation in one transaction.

### 8. [both] FE wiring (user commits FE) + end-to-end smoke + timings

FE (easemyresearch_frontend_v2: edit and typecheck only, the user commits):
- DirectImportModal gets a Data Service mode behind the user's flag. It uses the same presigned upload, POST /datasets, the preview step reused, confirm, polling GET /data-jobs/:jobId, then /app/records/:formId.
- Bulk-edit sends Idempotency-Key.
- Actions guarded in step 5 are hidden when form.storage === 'dataservice'.
- The grid needs no change.

Smoke script (Node scripts/ds-skeleton-smoke.ts, driving HTTP with a dev JWT): presign + PUT → POST /datasets → preview → confirm → poll job → grid pages 1–2 through the cursor → bulk-edit one cell → grid → assert the value. Exit code is non-zero on any mismatch. It records p50/p95 per step.

Run everything twice: Delta on local disk, then the testing S3 bucket under dataservice-probe/skeleton/ (cleaned up afterwards). Ask the user for a real sample xlsx (STATUS §0 step 3).

Update STATUS.md §0 'Resume here' and the backend docs copy together.

**Proves:** The walking skeleton itself, on the real Node and the real Data Service: create → import → browse → edit → see. It also gives the experiment's verdict numbers (ingest time, grid cold/warm, edit latency) and the evidence-ranked list of the remaining Phase 1 sites.

**Tests:** The smoke script is the test. FE: typecheck plus the existing grid tests. The census and timings tables go into STATUS.md next to the bench_reader numbers.

### 9. [both] Stretch: one aggregate read

Python: POST /v1/datasets/{id}/fields:stats {manifest, fields, kind:'mcq'|'number'}, with DuckDB aggregation through the one reader. Node: a FieldStats capability. The record analysis stats endpoint (record/analysis/analysis.repository.ts:44,83, an audit bypass) gets a storage branch, and the step-5 guard comes off that one route. The analysis service is not wired (Phase 5, D13). The count is already proven by rows:count.

**Proves:** An analysis-style aggregate at a pinned revision sees patched values (C40/C43 shape), and neither Node nor the analysis service loads rows.

**Tests:** Python: mcq counts and number stats before and after a patch. Node: stats response identical in shape for the same data on a Mongo form (snapshot).

## Decisions (recommendations)

- Base and order: Plan B's production contracts, run in Plan A's read-first order: seeded grid → edit → guard + census → publish path → ingest → FE. Recommend yes. You see real rows and edit latency before the big write-path port, and nothing on the read side is thrown away.
- First ingest: (A) port the lease, operation and publish path now (plan 2.1; step 6) and run ingest as a normal operation; or (B) Plan A's 'bootstrap publish' inside a FINAL_IMPORT branch. Recommend A. The bootstrap would be rewritten later. Choose B only if the experiment must show an import before step 6.
- Import entry: production /datasets + /data-jobs routes as a sub-group inside the EXISTING modules/import (api.md §1.3), not storage branches inside the legacy /imports services (Plan A). The legacy import stays untouched (D11), but the FE needs a small Data Service mode in DirectImportModal (you commit it). Recommend yes.
- Upload: reuse the existing /storage/presign-upload single PUT (it already goes straight to S3, 10 MB cap) instead of building api.md §1.2 multipart and dataset_uploads now. Recommend yes. Multipart comes with large files (plan 2.8/4.5).
- Quota: keep today's truth (sum of Form.entryCount, payment-quota.repository.ts:49/78). Check entry capacity at confirm as import does today, and set Form.entryCount = rowCount in the publish transaction. Recommend deferring entry_reservations and Payment.reserved_entries (C7, concurrent imports) to the next step after the skeleton, because only one opted-in user imports.
- Ingest cast runs inside the drain under a renewed lease (deviates from consistency §4.5). Recommend accepting it for the first ingest of a brand-new dataset only, because nothing else can write to it. Append imports must prepare outside the lease.
- Contract: Pydantic is the source; FastAPI exports OpenAPI to contracts/ together with shared JSON fixtures; Node uses hand-written Zod plus a fixtures contract test (plan 0.7, lightweight). This replaces B's YAML-first codegen. Recommend yes.
- Service token: build the scoped JWT now (api.md §2; STATUS says it comes with the first internal endpoint) instead of the scaffold x-service-token. Recommend yes.
- HTTP client: extend the shared PythonApiGateway with an optional baseUrl/headers (defaults unchanged), not a second copy. The service stays separate (D13). Recommend yes.
- Edit corners cut: an edit to the unique-identifier field is refused (claims are Phase 3); no dataset_events outbox. Batch undo stays guarded even though RevisionBatch rows are written. Recommend yes.
- Bulk-edit idempotency: an optional, additive Idempotency-Key header, with mutationId when present and a server UUID as fallback. Recommend yes. Please also confirm the FE grid saves a cell through PATCH bulk-edit: the FE repo was not at the Desktop path, and I did not search further.
- Who gets Data Service forms: DATA_SERVICE_ENABLED plus an allow-list DATA_SERVICE_USER_IDS. Recommend yes.
- Mongo for the run: the local docker replica set (27018) for both services, with a dedicated dev DB. Not Atlas (over quota) and never the .env.test DB. You set the env.
- xlsx parser: python-calamine (fast) or openpyxl (streaming). Recommend calamine for read speed; it avoids pyarrow.compute because of Windows App Control.
- Branch names: Node exp/dataservice-skeleton off feat/datastore-phase1, dataservice exp/skeleton off feat/lease-fence. Both never pushed. Confirm.

## Out of scope

- The other Phase 1 sites, and the MongoStore delegation of the Mongo grid (including the unbound method at core.helper.ts:276).
- Grid search, filters, column sort, follow-up columns, record-ids / column paste, check-unique, export, the trash list (all FEATURE_NOT_AVAILABLE).
- Submit, add-row, follow-up submit, single-row edit, delete/restore, unique-identifier claims, the dataset_events outbox.
- Batch undo / restore-preview and per-row history (undo_store port). RevisionBatch and dataset_changes are written, but restore stays guarded.
- Quota reservations (entry_reservations, Payment.reserved_entries), field freeze, group commit, held operations, payload encryption / S3 offload.
- Compaction, vacuum, patch retention, pinnedUntil pins (compaction_store port).
- api.md §1.2 multipart uploads and dataset_uploads; CSV / .sav / .rds / Parquet; files over 10 MB; append imports into a Data Service form.
- The full golden-vector set (Phase 2.3 exit); AI refine of detection; derived fields at read (3.8); schema operations.
- Analyses through the existing Python analysis service (rows:stream, Phase 5); AI record tools; doc-build counts.
- Lambda / Fargate / AWS infra, OTel dashboards (compute:'local' only).
- Deleting prototype/node_side (only after Node passes the same scenario suite).
- Pushing either experiment branch; committing FE code (the user commits FE).

## Risks

- Silent wrong data. Any FE screen route left unguarded reads the empty `records` collection. Mitigations: the step-5 table-driven guard test plus the route census, both before step 7 makes forms creatable.
- Reader changes (meta columns, `_rid DESC`, the rids filter) touch code that just passed a 55-agent review and 12,000-case fuzzing. They need mutation-style tests and an old-vs-new rerun.
- Display parity in toRecordDocument (date32 vs ISO midnight UTC, opts vs optionIds/value, number null vs 0, '' vs trimmed text, updatedAt for patched rows). The grid looks subtly wrong unless it is snapshot-tested against a Mongo fixture.
- Grid order is `_rid DESC`, which matches createdAt desc only while _rid ObjectIds are made in file order at ingest, and insert patches use Node ObjectIds.
- The edit reads before-values over HTTP before the Mongo transaction (time-of-check vs time-of-use). Harmless for one user; a stale `before` matters later for undo.
- Bulk-edit's existing per-entry validation was written for Mongo records (files, derived, table scopes). Reusing it without existing data may reshape input differently, so cover it with per-type unit tests.
- Porting lease/operation/publish is the largest step. A port bug shows up as a stuck dataset, so the Python scenario tests must run against Node over HTTP (node_http marker), not only against prototype/node_side.
- Quota: no reservations in the skeleton. Two concurrent confirms by the same user could both pass the capacity check (C7), which is acceptable only under the single-user allow-list.
- Fence and reconcile with revision 0 (`base: null`) need explicit handling, or the first drain fails or creates an unfenced table.
- Contract drift between Pydantic and hand-written Zod. Both sides must test against the same contracts/fixtures.
- The 10 MB xlsx cap means import cannot show scale. The speed verdict comes from the 1M-row seed through Node's grid route (step 2/3), local and S3.
- Environment blockers: Windows App Control (always `uv run python -m`, no pyarrow.compute); DuckDB extensions preinstalled; the S3 testing bucket needs CORS for the browser PUT (already used by today's import presign, so likely fine); Atlas is over quota, so use the docker replica set.
