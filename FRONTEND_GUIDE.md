# 🖥️ Frontend Integration Guide — Real-Time Anomaly Detection System

> A complete reference for building a frontend dashboard for the Real-Time ML Streaming Pipeline.
> This document covers every API, WebSocket channel, data model, and UI component you need.

---

## 📋 Table of Contents

1. [Project Workflow Overview](#1-project-workflow-overview)
2. [System Data Flow](#2-system-data-flow)
3. [Authentication Flow](#3-authentication-flow)
4. [API Endpoints Reference](#4-api-endpoints-reference)
5. [WebSocket Real-Time Channels](#5-websocket-real-time-channels)
6. [Data Models & Response Shapes](#6-data-models--response-shapes)
7. [UI Components Needed](#7-ui-components-needed)
8. [Recommended Pages & Routes](#8-recommended-pages--routes)
9. [Role-Based UI Access Control](#9-role-based-ui-access-control)
10. [Error Handling Reference](#10-error-handling-reference)
11. [Frontend Tech Stack Recommendations](#11-frontend-tech-stack-recommendations)

---

## 1. Project Workflow Overview

The Real-Time Anomaly Detection System is a **streaming ML pipeline** that:

1. **Ingests events** via REST API or Kafka → Published to `raw-events` topic
2. **Scores events** via ML inference worker → IsolationForest model assigns anomaly score
3. **Stores results** in TimescaleDB time-series database
4. **Triggers alerts** when score exceeds threshold (default: 0.70)
5. **Notifies teams** via Slack / Email / PagerDuty
6. **Visualizes** everything via REST API + live WebSocket push
7. **Retrains models** on a schedule or when drift is detected

```
  User → Frontend → POST /api/v1/events
                          ↓
                    Kafka raw-events
                          ↓
                   ML Inference Worker
                          ↓
                  Kafka scored-events
                          ↓
               TimescaleDB  +  Alert Engine
                          ↓
              Frontend ← WebSocket /ws/events
                          ↓
                   Grafana Dashboards
```

---

## 2. System Data Flow

### Event Lifecycle (Step-by-Step)

```
Step 1: POST /api/v1/events
        Body: { source_id, event_type, features: { duration, src_bytes, ... } }
        Returns: { event_id: "uuid", status: "queued" }
                 ↓
Step 2: Kafka raw-events topic
        Partitions: 6 | Retention: 24h
                 ↓
Step 3: ML Inference Worker consumes event
        - IsolationForest model scores it
        - anomaly_score ∈ [0.0, 1.0]
        - is_anomaly = (score >= ANOMALY_SCORE_THRESHOLD)
                 ↓
Step 4: Kafka scored-events topic
        Payload: { event_id, source_id, anomaly_score, is_anomaly, model_version }
                 ↓
Step 5a: TimescaleDB Writer stores result
         Table: anomaly.anomaly_events (hypertable partitioned by time)
                 ↓
Step 5b: Alert Agent checks threshold
         If is_anomaly → creates Alert row in DB
         Severity mapping:
           score 0.70-0.80 → LOW
           score 0.80-0.90 → MEDIUM
           score 0.90-0.95 → HIGH
           score 0.95-1.00 → CRITICAL
                 ↓
Step 6a: WebSocket push → /ws/events channel
         Frontend receives live event data
                 ↓
Step 6b: WebSocket push → /ws/alerts channel
         Frontend receives new alert notifications
                 ↓
Step 6c: Notification Agent → Slack/Email/PagerDuty
```

### Alert Lifecycle

```
ACTIVE → ACKNOWLEDGED → RESOLVED
   ↑           ↑             ↑
Created    Analyst sets    Analyst sets
by system  analyst_id +   resolution reason +
           note           resolved_at timestamp
```

---

## 3. Authentication Flow

### Base URL
```
http://localhost:8000
```

### Step 1: Login

```http
POST /api/v1/auth/token
Content-Type: application/x-www-form-urlencoded

username=admin@example.com&password=admin123
```

**Response:**
```json
{
  "access_token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...",
  "refresh_token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...",
  "token_type": "bearer",
  "role": "admin"
}
```

### Step 2: Use Token

```http
GET /api/v1/anomalies
Authorization: Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...
```

### Step 3: Refresh Token

```http
POST /api/v1/auth/refresh
Content-Type: application/json

{
  "refresh_token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9..."
}
```

### Step 4: Logout

```http
POST /api/v1/auth/revoke
Authorization: Bearer <current_access_token>
```

### Test Credentials

| Email | Password | Role |
|-------|----------|------|
| `admin@example.com` | `admin123` | admin |
| `analyst@example.com` | `analyst123` | analyst |
| `viewer@example.com` | `viewer123` | viewer |

---

## 4. API Endpoints Reference

### 🔐 Auth — `/api/v1/auth`

| Method | Path | Auth | Body/Params | Returns |
|--------|------|------|-------------|---------|
| `POST` | `/token` | None | form: `username`, `password` | `Token` |
| `POST` | `/refresh` | None | `{ refresh_token }` | `Token` |
| `POST` | `/revoke` | Bearer | — | `{ detail }` |

---

### 📥 Events — `/api/v1/events`

#### POST `/api/v1/events` — Ingest Single Event

```json
// Request body
{
  "source_id": "sensor-001",
  "event_type": "network_flow",
  "features": {
    "duration": 120,
    "protocol_type": 1,
    "src_bytes": 1024,
    "dst_bytes": 512,
    "land": 0,
    "wrong_fragment": 0,
    "urgent": 0
  }
}

// Response 202
{
  "event_id": "550e8400-e29b-41d4-a716-446655440000",
  "status": "queued"
}
```

#### POST `/api/v1/events/batch` — Ingest Batch

```json
// Request body
{
  "events": [
    { "source_id": "sensor-001", "event_type": "network_flow", "features": { ... } },
    { "source_id": "sensor-002", "event_type": "network_flow", "features": { ... } }
  ]
}

// Response 202
{
  "event_ids": ["uuid1", "uuid2"],
  "status": "queued",
  "count": 2
}
```

#### GET `/api/v1/events` — List Events

Query params:
- `limit` (default 50, max 500)
- `offset` (default 0)
- `only_anomalies` (boolean, default false)
- `source_id` (string filter)
- `start` (ISO-8601 datetime)
- `end` (ISO-8601 datetime)

```json
// Response 200 — array of AnomalyEventOut
[
  {
    "event_id": "uuid",
    "event_time": "2024-01-01T12:00:00Z",
    "source_id": "sensor-001",
    "feature_vector": [120, 1, 1024, 512, 0, 0, 0],
    "anomaly_score": 0.82,
    "is_anomaly": true,
    "model_version": "v1.2.0-prod",
    "processed_at": "2024-01-01T12:00:00.045Z"
  }
]
```

#### PATCH `/api/v1/events/{event_id}/label` — Apply Feedback Label

```json
// Request body (Admin only)
{
  "label": "FP",           // TP | FP | TN | FN
  "analyst_id": "analyst@example.com",
  "note": "False positive - scheduled scan"
}
```

---

### 🚨 Anomalies — `/api/v1/anomalies`

#### GET `/api/v1/anomalies` — List Anomalies

Query params:
- `min_score` (float 0.0-1.0)
- `severity` (LOW | MEDIUM | HIGH | CRITICAL)
- `source_id` (string)
- `start`, `end` (ISO-8601)
- `limit`, `offset`

#### GET `/api/v1/anomalies/stats` — Aggregated Stats

Query params:
- `bucket` (e.g., `5m`, `1h`, `1d`)
- `start`, `end`

```json
// Response
{
  "window_minutes": 60,
  "total_events": 12048,
  "anomaly_count": 342,
  "anomaly_rate_pct": 2.84,
  "avg_score": 0.763,
  "max_score": 0.981
}
```

#### GET `/api/v1/anomalies/heatmap` — Heatmap Data

Query params:
- `resolution` (5m | 1h | 1d)
- `start`, `end`

```json
// Response
{
  "resolution": "1h",
  "start": "2024-01-01T00:00:00Z",
  "end": "2024-01-02T00:00:00Z",
  "data": [
    {
      "bucket": "2024-01-01T12:00:00Z",
      "source_id": "sensor-001",
      "event_count": 120,
      "anomaly_count": 8,
      "avg_score": 0.74,
      "max_score": 0.93
    }
  ]
}
```

---

### 🔔 Alerts — `/api/v1/alerts`

#### GET `/api/v1/alerts` — List Alerts

Query params:
- `status` (ACTIVE | ACKNOWLEDGED | RESOLVED)
- `severity` (LOW | MEDIUM | HIGH | CRITICAL)
- `limit`, `offset`

```json
// AlertOut shape
{
  "alert_id": "uuid",
  "source_id": "sensor-001",
  "anomaly_score": 0.95,
  "severity": "CRITICAL",
  "status": "ACTIVE",
  "created_at": "2024-01-01T12:00:00Z",
  "acknowledged_at": null,
  "resolved_at": null,
  "analyst_id": null,
  "note": null,
  "resolution": null
}
```

#### GET `/api/v1/alerts/{alert_id}` — Alert with Linked Events

```json
// Response
{
  "alert": { ...AlertOut },
  "linked_events": [ ...AnomalyEventOut[] ]
}
```

#### PATCH `/api/v1/alerts/{alert_id}/acknowledge`

```json
// Request (Analyst+)
{
  "analyst_id": "analyst@example.com",
  "note": "Investigating unusual traffic from sensor-001"
}
```

#### PATCH `/api/v1/alerts/{alert_id}/resolve`

```json
// Request (Analyst+)
{
  "analyst_id": "analyst@example.com",
  "resolution": "Confirmed false positive - scheduled maintenance window"
}
```

#### POST `/api/v1/alerts/silence`

```json
// Request (Analyst+)
{
  "source_id": "sensor-001",
  "duration_minutes": 60,
  "reason": "Maintenance window scheduled"
}

// Response 201
{
  "silence_id": "uuid",
  "source_id": "sensor-001",
  "duration_minutes": 60,
  "reason": "Maintenance window scheduled",
  "created_at": "2024-01-01T12:00:00Z",
  "expires_at": "2024-01-01T13:00:00Z"
}
```

---

### 🤖 Model Management — `/api/v1/model`

#### GET `/api/v1/model/status`

```json
{
  "model_version": "v1.2.0-prod",
  "load_time": "2024-01-01T03:00:00Z",
  "avg_inference_latency_ms": 8.42,
  "total_inferences": 125032
}
```

#### GET `/api/v1/model/versions`

```json
[
  {
    "model_version": "v1.2.0-prod",
    "accuracy": 0.984,
    "f1_score": 0.962,
    "registered_at": "2024-01-01T03:00:00Z",
    "active": true
  },
  {
    "model_version": "v1.1.0-legacy",
    "accuracy": 0.971,
    "f1_score": 0.945,
    "registered_at": "2024-01-01T03:00:00Z",
    "active": false
  }
]
```

#### POST `/api/v1/model/retrain` (Admin only)

```json
// Request
{
  "reason": "Manual trigger - performance degradation detected",
  "force": false
}

// Response 202
{
  "job_id": "uuid",
  "status": "PENDING",
  "reason": "Manual trigger - performance degradation detected",
  "force": false,
  "created_at": "2024-01-01T12:00:00Z"
}
```

#### GET `/api/v1/model/retrain/{job_id}` — Poll Job Status

```json
{
  "job_id": "uuid",
  "status": "RUNNING",   // PENDING | RUNNING | COMPLETED | FAILED
  "reason": "...",
  "force": false,
  "created_at": "...",
  "completed_at": null
}
```

#### POST `/api/v1/model/rollback` (Admin only)

```json
// Request
{
  "version": "v1.1.0-legacy",
  "reason": "Current model accuracy degraded"
}

// Response
{
  "status": "rollback_dispatched",
  "detail": "Switch command to v1.1.0-legacy published to cluster."
}
```

#### GET `/api/v1/model/metrics`

Query: `?version=v1.2.0-prod`

```json
{
  "model_version": "v1.2.0-prod",
  "precision": 0.982,
  "recall": 0.947,
  "f1_score": 0.964,
  "auc_roc": 0.991,
  "evaluation_date": "2024-01-01T03:00:00Z"
}
```

---

## 5. WebSocket Real-Time Channels

### Base URL
```
ws://localhost:8000/api/v1/ws
```

### Channel 1: `/ws/events` — Live Scored Events

```javascript
const ws = new WebSocket('ws://localhost:8000/api/v1/ws/events');

ws.onmessage = (event) => {
  const data = JSON.parse(event.data);
  // data shape:
  // {
  //   event_id: "uuid",
  //   score: 0.82,
  //   is_anomaly: true,
  //   source_id: "sensor-001",
  //   timestamp: "2024-01-01T12:00:00.123Z"
  // }
};

// Keepalive
setInterval(() => ws.send('ping'), 30000);
```

### Channel 2: `/ws/alerts` — Real-Time Alert Feed

```javascript
const ws = new WebSocket('ws://localhost:8000/api/v1/ws/alerts');

ws.onmessage = (event) => {
  const data = JSON.parse(event.data);
  // data shape:
  // {
  //   alert_id: "uuid",
  //   severity: "CRITICAL",
  //   source_id: "sensor-001",
  //   score: 0.97,
  //   timestamp: "2024-01-01T12:00:00Z"
  // }
};
```

### Channel 3: `/ws/metrics` — System Performance Telemetry (every 5s)

```javascript
const ws = new WebSocket('ws://localhost:8000/api/v1/ws/metrics');

ws.onmessage = (event) => {
  const data = JSON.parse(event.data);
  // data shape:
  // {
  //   metric: "system_performance",
  //   value: {
  //     events_per_second: 98.5,
  //     anomaly_rate_percent: 3.2,
  //     consumer_lag: 4
  //   },
  //   timestamp: "2024-01-01T12:00:00Z"
  // }
};
```

---

## 6. Data Models & Response Shapes

### Token

```typescript
interface Token {
  access_token: string;
  refresh_token: string;
  token_type: "bearer";
  role: "admin" | "analyst" | "viewer";
}
```

### AnomalyEvent

```typescript
interface AnomalyEvent {
  event_id: string;           // UUID
  event_time: string;         // ISO-8601
  source_id: string;          // Origin sensor/device ID
  feature_vector: number[];   // Array of numeric feature values
  anomaly_score: number;      // 0.0 – 1.0
  is_anomaly: boolean;
  model_version: string;      // e.g., "v1.2.0-prod"
  processed_at: string;       // ISO-8601
}
```

### Alert

```typescript
interface Alert {
  alert_id: string;           // UUID
  source_id: string;
  anomaly_score: number;
  severity: "LOW" | "MEDIUM" | "HIGH" | "CRITICAL";
  status: "ACTIVE" | "ACKNOWLEDGED" | "RESOLVED";
  created_at: string;
  acknowledged_at: string | null;
  resolved_at: string | null;
  analyst_id: string | null;
  note: string | null;
  resolution: string | null;
}
```

### AnomalyStats

```typescript
interface AnomalyStats {
  window_minutes: number;
  total_events: number;
  anomaly_count: number;
  anomaly_rate_pct: number;   // percentage e.g., 2.84
  avg_score: number;
  max_score: number;
}
```

### HeatmapItem

```typescript
interface HeatmapItem {
  bucket: string;             // ISO-8601 time bucket
  source_id: string;
  event_count: number;
  anomaly_count: number;
  avg_score: number;
  max_score: number;
}

interface HeatmapOut {
  resolution: string;         // "5m" | "1h" | "1d"
  start: string;
  end: string;
  data: HeatmapItem[];
}
```

### ModelStatus

```typescript
interface ModelStatus {
  model_version: string;
  load_time: string;
  avg_inference_latency_ms: number;
  total_inferences: number;
}

interface ModelVersion {
  model_version: string;
  accuracy: number;
  f1_score: number;
  registered_at: string;
  active: boolean;
}

interface ModelMetrics {
  model_version: string;
  precision: number;
  recall: number;
  f1_score: number;
  auc_roc: number;
  evaluation_date: string;
}

interface RetrainJob {
  job_id: string;
  status: "PENDING" | "RUNNING" | "COMPLETED" | "FAILED";
  reason: string;
  force: boolean;
  created_at: string;
  completed_at: string | null;
}
```

### AlertSilence

```typescript
interface AlertSilenceOut {
  silence_id: string;
  source_id: string;
  duration_minutes: number;
  reason: string;
  created_at: string;
  expires_at: string;
}
```

### IngestEvent Request

```typescript
interface IngestEventIn {
  source_id: string;
  event_type: string;             // e.g., "network_flow"
  features: Record<string, number>;  // Key-value feature map
}
```

---

## 7. UI Components Needed

### Core Layout Components

| Component | Description |
|-----------|-------------|
| `Navbar` | Top nav with user role, logout button, notification badge |
| `Sidebar` | Navigation links to all pages |
| `AuthGuard` | Redirect to login if no valid JWT |
| `RoleGuard` | Hide/show UI elements based on user role |
| `LoadingSpinner` | Global loading state indicator |
| `ErrorBanner` | Reusable error alert component |
| `EmptyState` | No-data placeholder |

### Authentication Components

| Component | API Used | Description |
|-----------|----------|-------------|
| `LoginForm` | `POST /auth/token` | Email + password form with role display |
| `TokenRefresher` | `POST /auth/refresh` | Auto-refresh hook when token expires |

### Dashboard / Overview Components

| Component | API Used | Description |
|-----------|----------|-------------|
| `LiveMetricsBar` | WS `/ws/metrics` | Real-time events/sec, anomaly rate %, consumer lag |
| `StatsCards` | `GET /anomalies/stats` | Total events, anomaly count, rate, avg score |
| `AnomalyRateChart` | WS `/ws/events` + REST stats | Line chart of anomaly rate over time |
| `ActiveAlertsWidget` | `GET /alerts?status=ACTIVE` | Count of active alerts by severity |
| `ModelStatusCard` | `GET /model/status` | Active model version, latency, inference count |

### Events Components

| Component | API Used | Description |
|-----------|----------|-------------|
| `EventsTable` | `GET /events` | Paginated table with filters (time, source, anomaly-only) |
| `EventRow` | — | Row with score badge, severity indicator, source ID |
| `EventDetailPanel` | `GET /events/{id}` | Drawer/modal with full event details + feature vector |
| `EventIngestForm` | `POST /events` | Form to manually inject test events |
| `BatchIngestModal` | `POST /events/batch` | JSON batch event uploader |
| `LiveEventsFeed` | WS `/ws/events` | Auto-scrolling real-time events feed |
| `LabelEventModal` | `PATCH /events/{id}/label` | Admin: apply TP/FP/TN/FN ground-truth label |

### Anomalies Components

| Component | API Used | Description |
|-----------|----------|-------------|
| `AnomaliesTable` | `GET /anomalies` | Filtered table with severity badges + pagination |
| `AnomalyDetailModal` | `GET /anomalies/{id}` | Full anomaly context view |
| `AnomalyHeatmap` | `GET /anomalies/heatmap` | Source × time grid heatmap chart |
| `SeverityFilter` | — | LOW / MEDIUM / HIGH / CRITICAL button filter group |
| `ScoreHistogram` | — | Distribution chart of anomaly scores |

### Alerts Components

| Component | API Used | Description |
|-----------|----------|-------------|
| `AlertsPanel` | `GET /alerts` | Tabbed: ACTIVE / ACKNOWLEDGED / RESOLVED |
| `AlertCard` | — | Card with severity color, source ID, time, status |
| `AlertDetailModal` | `GET /alerts/{id}` | Alert details + linked anomaly events |
| `AlertActionBar` | `PATCH /alerts/{id}/acknowledge` | Acknowledge / Resolve buttons with form |
| `SilenceAlertModal` | `POST /alerts/silence` | Duration + reason form to silence source |
| `LiveAlertFeed` | WS `/ws/alerts` | Real-time alert toast notifications |
| `AlertBadge` | `GET /alerts?status=ACTIVE` | Badge in navbar showing active alert count |

### Model Management Components

| Component | API Used | Description |
|-----------|----------|-------------|
| `ModelStatusCard` | `GET /model/status` | Version, latency, total inferences |
| `ModelVersionsTable` | `GET /model/versions` | List of versions with accuracy, F1, active indicator |
| `ModelMetricsChart` | `GET /model/metrics` | Precision / Recall / F1 / AUC-ROC bar chart |
| `RetrainButton` | `POST /model/retrain` | Admin: trigger retraining with reason field |
| `RetrainJobStatus` | `GET /model/retrain/{job_id}` | Polling progress bar: PENDING → RUNNING → COMPLETED |
| `ModelRollbackModal` | `POST /model/rollback` | Select version + reason to rollback |

### System / Infrastructure Components

| Component | API Used | Description |
|-----------|----------|-------------|
| `KafkaTopicsTable` | `GET /kafka/topics` | Topic list with partition counts |
| `SystemHealthGrid` | External service ping | Service health status grid |
| `PerformanceMetricsPanel` | WS `/ws/metrics` | Live charts: eps, anomaly rate, consumer lag |

---

## 8. Recommended Pages & Routes

### Page Map

```
/                       → Dashboard (Overview)
/login                  → Login Page
/events                 → Events Feed
/events/live            → Live Events Stream
/events/ingest          → Manual Event Injection (Admin)
/anomalies              → Anomalies Table
/anomalies/heatmap      → Heatmap View
/alerts                 → Alerts Management
/alerts/:id             → Alert Detail
/model                  → Model Status & Versions
/model/retrain          → Retrain Management (Admin)
/system                 → System Health & Infrastructure
/settings               → User Settings
```

---

### Page: Dashboard (`/`)

**Purpose:** High-level overview of the entire pipeline status.

| Section | Data Source | Refresh |
|---------|------------|---------|
| Top KPI cards (total events, anomaly count, active alerts, model version) | `GET /anomalies/stats` | 30s |
| Live metrics bar (events/sec, anomaly rate, consumer lag) | WS `/ws/metrics` | Real-time |
| Recent anomalies table (last 10) | `GET /anomalies?limit=10` | 30s |
| Active alerts panel | `GET /alerts?status=ACTIVE&limit=5` | 30s |
| Model status card | `GET /model/status` | 60s |
| Alert count by severity (pie/donut chart) | `GET /alerts` | 60s |

---

### Page: Events Feed (`/events`)

**Purpose:** Browse and search all scored events.

| Feature | API |
|---------|-----|
| Paginated events table | `GET /events?limit=50&offset=N` |
| Filter by anomaly-only | `GET /events?only_anomalies=true` |
| Filter by source ID | `GET /events?source_id=sensor-001` |
| Filter by time range | `GET /events?start=...&end=...` |
| View event detail | `GET /events/{event_id}` |
| Apply label (Admin) | `PATCH /events/{event_id}/label` |
| Live stream tab | WS `/ws/events` |

---

### Page: Anomalies (`/anomalies`)

**Purpose:** Focused view of confirmed anomalies only.

| Feature | API |
|---------|-----|
| Anomalies table with severity filter | `GET /anomalies?severity=CRITICAL` |
| Stats summary bar | `GET /anomalies/stats?bucket=1h` |
| Heatmap view | `GET /anomalies/heatmap?resolution=1h` |
| Single anomaly detail | `GET /anomalies/{id}` |

---

### Page: Alerts Management (`/alerts`)

**Purpose:** Alert triage workflow for analysts.

| Feature | API |
|---------|-----|
| Status tabs: ACTIVE / ACKNOWLEDGED / RESOLVED | `GET /alerts?status=ACTIVE` |
| Severity filter | `GET /alerts?severity=HIGH` |
| Alert detail + linked events | `GET /alerts/{alert_id}` |
| Acknowledge alert | `PATCH /alerts/{id}/acknowledge` |
| Resolve alert | `PATCH /alerts/{id}/resolve` |
| Silence source | `POST /alerts/silence` |
| Live alert notifications | WS `/ws/alerts` |

---

### Page: Model Management (`/model`)

**Purpose:** Monitor and control the ML model lifecycle.

| Feature | API |
|---------|-----|
| Active model status | `GET /model/status` |
| Model versions table | `GET /model/versions` |
| Model metrics (precision, recall, F1, AUC-ROC) | `GET /model/metrics` |
| Trigger retraining (Admin) | `POST /model/retrain` |
| Poll retraining job | `GET /model/retrain/{job_id}` |
| Model rollback (Admin) | `POST /model/rollback` |

---

### Page: System Health (`/system`)

**Purpose:** Infrastructure and pipeline observability.

| Section | Data |
|---------|------|
| Service health grid | Ping each service URL |
| Live performance metrics | WS `/ws/metrics` |
| Kafka topic info | `GET /kafka/topics` |
| Links to Grafana, MLflow, Kafka UI | Static links |

---

## 9. Role-Based UI Access Control

### Role Hierarchy
```
admin > analyst > viewer
```

### Component Visibility Matrix

| Feature / Component | viewer | analyst | admin |
|--------------------|--------|---------|-------|
| View events table | ✅ | ✅ | ✅ |
| View anomalies | ✅ | ✅ | ✅ |
| View alerts | ✅ | ✅ | ✅ |
| View model status | ✅ | ✅ | ✅ |
| Ingest events | ✅ | ✅ | ✅ |
| Acknowledge alerts | ❌ | ✅ | ✅ |
| Resolve alerts | ❌ | ✅ | ✅ |
| Silence alerts | ❌ | ✅ | ✅ |
| Apply event labels | ❌ | ❌ | ✅ |
| Trigger retraining | ❌ | ❌ | ✅ |
| Model rollback | ❌ | ❌ | ✅ |

### JWT Role Detection

```javascript
// Decode the JWT payload to get role
function getUserRole(token) {
  const payload = JSON.parse(atob(token.split('.')[1]));
  return payload.role; // "admin" | "analyst" | "viewer"
}
```

---

## 10. Error Handling Reference

### HTTP Status Codes

| Code | Meaning | Frontend Action |
|------|---------|----------------|
| `200` | Success | Render data |
| `202` | Accepted (async) | Show "queued" status |
| `201` | Created | Show success message |
| `400` | Bad Request | Show validation error |
| `401` | Unauthorized | Redirect to `/login` |
| `403` | Forbidden | Show "insufficient permissions" |
| `404` | Not Found | Show empty state |
| `422` | Validation Error | Show field-level errors |
| `500` | Server Error | Show generic error banner |

### Common Error Response Shape

```json
{
  "detail": "Could not validate credentials or token expired"
}
```

Or for validation errors:
```json
{
  "detail": [
    {
      "loc": ["body", "features", "duration"],
      "msg": "value is not a valid float",
      "type": "type_error.float"
    }
  ]
}
```

### WebSocket Reconnection Strategy

```javascript
class ResilientWebSocket {
  constructor(url, onMessage) {
    this.url = url;
    this.onMessage = onMessage;
    this.retryDelay = 1000;
    this.connect();
  }

  connect() {
    this.ws = new WebSocket(this.url);
    this.ws.onmessage = (e) => this.onMessage(JSON.parse(e.data));
    this.ws.onclose = () => {
      setTimeout(() => this.connect(), this.retryDelay);
      this.retryDelay = Math.min(this.retryDelay * 2, 30000); // exponential backoff
    };
    this.ws.onopen = () => { this.retryDelay = 1000; }; // reset on success
  }
}
```

---

## 11. Frontend Tech Stack Recommendations

### Recommended Stack

| Layer | Technology | Reason |
|-------|-----------|--------|
| **Framework** | React 18 or Next.js 14 | Component model, SSR option |
| **Language** | TypeScript | Type-safe API integration |
| **State Management** | Zustand or Redux Toolkit | Global auth + alert state |
| **Data Fetching** | React Query (TanStack Query) | Caching, polling, pagination |
| **WebSocket** | Native WebSocket + custom hook | Full control, reconnection logic |
| **Charts** | Recharts or Chart.js | Line, bar, heatmap, pie charts |
| **UI Components** | shadcn/ui or Mantine | Modern, accessible components |
| **Styling** | Tailwind CSS | Rapid consistent styling |
| **Date Handling** | date-fns or dayjs | ISO-8601 parsing |
| **Forms** | React Hook Form + Zod | Validation + type inference |
| **HTTP Client** | Axios or Fetch | JWT interceptor support |

### Axios Interceptor for JWT

```javascript
const api = axios.create({ baseURL: 'http://localhost:8000/api/v1' });

api.interceptors.request.use((config) => {
  const token = localStorage.getItem('access_token');
  if (token) config.headers.Authorization = `Bearer ${token}`;
  return config;
});

api.interceptors.response.use(
  (res) => res,
  async (error) => {
    if (error.response?.status === 401) {
      // Try token refresh
      const refresh = localStorage.getItem('refresh_token');
      const res = await axios.post('/api/v1/auth/refresh', { refresh_token: refresh });
      localStorage.setItem('access_token', res.data.access_token);
      // Retry original request
      error.config.headers.Authorization = `Bearer ${res.data.access_token}`;
      return axios(error.config);
    }
    return Promise.reject(error);
  }
);
```

### React Query Setup

```javascript
// Poll anomaly stats every 30 seconds
const { data: stats } = useQuery({
  queryKey: ['anomaly-stats'],
  queryFn: () => api.get('/anomalies/stats').then(r => r.data),
  refetchInterval: 30_000,
});

// Paginated events
const { data, fetchNextPage } = useInfiniteQuery({
  queryKey: ['events', filters],
  queryFn: ({ pageParam = 0 }) =>
    api.get('/events', { params: { offset: pageParam, limit: 50, ...filters } }).then(r => r.data),
  getNextPageParam: (_, pages) => pages.length * 50,
});
```

### CORS Configuration

The backend allows CORS from these origins by default:
```
http://localhost:3000
http://localhost:8080
```

To add your custom frontend origin, set in `.env`:
```
CORS_ORIGINS=http://localhost:5173,http://yourapp.com
```

---

## Quick Reference Card

### Critical Endpoints for MVP Dashboard

```
POST  /api/v1/auth/token              → Login
GET   /api/v1/anomalies/stats         → Dashboard KPIs
GET   /api/v1/anomalies?limit=20      → Recent anomalies
GET   /api/v1/alerts?status=ACTIVE    → Active alerts count
GET   /api/v1/model/status            → Model info
WS    /api/v1/ws/metrics              → Live performance
WS    /api/v1/ws/events               → Live events stream
WS    /api/v1/ws/alerts               → Live alerts
```

### Severity Color Coding

| Severity | Score | Suggested Color |
|----------|-------|----------------|
| `LOW` | 0.70–0.80 | 🟡 Yellow `#EAB308` |
| `MEDIUM` | 0.80–0.90 | 🟠 Orange `#F97316` |
| `HIGH` | 0.90–0.95 | 🔴 Red `#EF4444` |
| `CRITICAL` | 0.95–1.00 | 🔴 Deep Red `#7F1D1D` |

### Alert Status Color Coding

| Status | Suggested Color |
|--------|----------------|
| `ACTIVE` | 🔴 Red `#EF4444` |
| `ACKNOWLEDGED` | 🟡 Amber `#F59E0B` |
| `RESOLVED` | 🟢 Green `#22C55E` |
