# =============================================================
# bootstrap.ps1 — Windows PowerShell First-Run Initializer
# Real-Time Anomaly Detection System
#
# Usage (from anomaly-detection-system\ directory):
#   .\scripts\bootstrap.ps1
#
# Requires: Docker Desktop, docker compose v2
# =============================================================

$ErrorActionPreference = "Continue"
$ProjectName = "anomaly-detection"

function Write-Step  { param($msg) Write-Host "[bootstrap] $msg" -ForegroundColor Cyan }
function Write-Ok    { param($msg) Write-Host "[OK] $msg" -ForegroundColor Green }
function Write-Warn  { param($msg) Write-Host "[!]  $msg" -ForegroundColor Yellow }
function Write-Fail  { param($msg) Write-Host "[X]  $msg" -ForegroundColor Red }

# --- Load .env --------------------------------------------------
Write-Step "Loading .env..."
if (-not (Test-Path ".env")) {
    Write-Fail ".env not found. Run: Copy-Item .env.example .env  then fill in passwords."
    exit 1
}
Get-Content ".env" | ForEach-Object {
    if ($_ -match "^\s*([^#][^=]+)=(.*)$") {
        $key   = $Matches[1].Trim()
        $value = $Matches[2].Trim()
        [System.Environment]::SetEnvironmentVariable($key, $value, "Process")
    }
}
Write-Ok ".env loaded."

# --- Step 0: Start services if needed ---------------------------
Write-Step "Step 0: Ensuring Docker services are running..."
$running = docker compose --project-name $ProjectName ps --services --filter "status=running" 2>$null
if ($running -notmatch "kafka") {
    Write-Warn "Kafka not running. Starting all services..."
    docker compose --project-name $ProjectName up -d
    Write-Step "Waiting 30s for initial startup..."
    Start-Sleep -Seconds 30
}
Write-Ok "Docker services started."

# --- Step a: Wait for Kafka -------------------------------------
Write-Step "Step a: Waiting for Kafka (up to 120s)..."
$maxWait = 120
$elapsed = 0
$kafkaReady = $false

while ($elapsed -lt $maxWait) {
    $result = docker compose --project-name $ProjectName exec -T kafka `
        kafka-topics --bootstrap-server localhost:9092 --list 2>$null
    if ($LASTEXITCODE -eq 0) {
        $kafkaReady = $true
        break
    }
    Write-Host "." -NoNewline
    Start-Sleep -Seconds 5
    $elapsed += 5
}
Write-Host ""
if ($kafkaReady) { Write-Ok "Kafka is ready." }
else { Write-Warn "Kafka may not be fully ready — continuing anyway." }

# --- Wait for Schema Registry -----------------------------------
Write-Step "Waiting for Schema Registry (up to 60s)..."
$schemaUrl = "http://localhost:8081/subjects"
$elapsed = 0
$schemaReady = $false

while ($elapsed -lt 60) {
    try {
        $resp = Invoke-WebRequest -Uri $schemaUrl -UseBasicParsing -TimeoutSec 3 -ErrorAction Stop
        if ($resp.StatusCode -eq 200) { $schemaReady = $true; break }
    } catch { }
    Write-Host "." -NoNewline
    Start-Sleep -Seconds 5
    $elapsed += 5
}
Write-Host ""
if ($schemaReady) { Write-Ok "Schema Registry is ready." }
else { Write-Warn "Schema Registry not ready — continuing." }

# --- Step b: Create Kafka topics --------------------------------
Write-Step "Step b: Creating Kafka topics..."
$topics = @(
    @{name="raw-events";    partitions=6; retention="86400000"},
    @{name="scored-events"; partitions=6; retention="259200000"},
    @{name="alerts";        partitions=3; retention="604800000"},
    @{name="model-updates"; partitions=1; retention="2592000000"}
)

foreach ($topic in $topics) {
    docker compose --project-name $ProjectName exec -T kafka `
        kafka-topics --bootstrap-server localhost:9092 `
        --create --if-not-exists `
        --topic $topic.name `
        --partitions $topic.partitions `
        --replication-factor 1 `
        --config "retention.ms=$($topic.retention)" 2>$null | Out-Null
    Write-Ok "Topic '$($topic.name)' ensured."
}
Write-Ok "Kafka topics created."

# --- Step c: Register schemas -----------------------------------
Write-Step "Step c: Registering Avro schemas..."
$schemasDir = "infra\kafka\schemas"
if (Test-Path $schemasDir) {
    $schemaFiles = @(
        @{subject="raw-events-value";    file="raw_event.avsc"},
        @{subject="scored-events-value"; file="scored_event.avsc"},
        @{subject="alerts-value";        file="alert_event.avsc"}
    )
    foreach ($s in $schemaFiles) {
        $path = Join-Path $schemasDir $s.file
        if (Test-Path $path) {
            $schemaContent = Get-Content $path -Raw
            $payload = @{ schema = $schemaContent } | ConvertTo-Json -Compress
            try {
                $resp = Invoke-RestMethod -Uri "http://localhost:8081/subjects/$($s.subject)/versions" `
                    -Method POST -ContentType "application/vnd.schemaregistry.v1+json" `
                    -Body $payload -ErrorAction Stop
                Write-Ok "Schema '$($s.subject)' registered (id=$($resp.id))."
            } catch {
                Write-Warn "Schema '$($s.subject)': $($_.Exception.Message)"
            }
        } else {
            Write-Warn "Schema file not found: $path"
        }
    }
} else {
    Write-Warn "Schema directory not found. Skipping schema registration."
}

# --- Step d: Verify TimescaleDB ---------------------------------
Write-Step "Step d: Verifying TimescaleDB initialization..."
$tsUser = if ($env:TIMESCALE_USER) { $env:TIMESCALE_USER } else { "anomaly_admin" }
$tsDb   = if ($env:TIMESCALE_DB)   { $env:TIMESCALE_DB   } else { "anomaly_db"    }
$result = docker compose --project-name $ProjectName exec -T timescaledb `
    psql -U $tsUser -d $tsDb -t -c "SELECT COUNT(*) FROM timescaledb_information.hypertables;" 2>$null
$htCount = ($result -replace '\s','')
if ($htCount -match '^\d+$' -and [int]$htCount -gt 0) {
    Write-Ok "TimescaleDB hypertables initialized ($htCount found)."
} else {
    Write-Warn "Hypertables not found or TimescaleDB still initializing. Init scripts will run automatically on first start."
}

# --- Step e: Check CICIDS2017 dataset ---------------------------
Write-Step "Step e: Checking CICIDS2017 dataset..."
$dataFile = "ml\data\raw\cicids2017.csv"
New-Item -ItemType Directory -Force -Path "ml\data\raw" | Out-Null

if ((Test-Path $dataFile) -and (Get-Item $dataFile).Length -gt 100000) {
    Write-Ok "CICIDS2017 dataset already cached."
} else {
    Write-Step "Downloading CICIDS2017 dataset via ML worker container..."
    docker compose --project-name $ProjectName run --rm `
        -v "${PWD}/ml:/app/ml" `
        ml-inference-worker `
        python /app/ml/data/download_cicids.py 2>$null
    if ((Test-Path $dataFile) -and (Get-Item $dataFile).Length -gt 100000) {
        Write-Ok "Dataset downloaded."
    } else {
        Write-Warn "Dataset download failed or incomplete. Training will use synthetic data."
    }
}

# --- Step f: Wait for MLflow + MinIO then train -----------------
Write-Step "Step f: Waiting for MLflow (up to 120s)..."
$elapsed = 0
$mlflowReady = $false
while ($elapsed -lt 120) {
    try {
        $resp = Invoke-WebRequest -Uri "http://localhost:5000/health" -UseBasicParsing -TimeoutSec 3 -ErrorAction Stop
        if ($resp.StatusCode -eq 200) { $mlflowReady = $true; break }
    } catch { }
    Write-Host "." -NoNewline
    Start-Sleep -Seconds 5
    $elapsed += 5
}
Write-Host ""
if ($mlflowReady) { Write-Ok "MLflow is ready." }
else { Write-Warn "MLflow not responding — training will still run." }

Write-Step "Creating MinIO buckets..."
docker compose --project-name $ProjectName exec -T minio `
    mc alias set local http://localhost:9000 `
    "$($env:MINIO_ACCESS_KEY)" "$($env:MINIO_SECRET_KEY)" 2>$null | Out-Null
$bucketModels   = if ($env:MINIO_BUCKET_MODELS)   { $env:MINIO_BUCKET_MODELS }   else { "ml-models" }
$bucketDatasets = if ($env:MINIO_BUCKET_DATASETS) { $env:MINIO_BUCKET_DATASETS } else { "training-datasets" }
docker compose --project-name $ProjectName exec -T minio `
    mc mb --ignore-existing "local/$bucketModels" 2>$null | Out-Null
docker compose --project-name $ProjectName exec -T minio `
    mc mb --ignore-existing "local/$bucketDatasets" 2>$null | Out-Null
Write-Ok "MinIO buckets ready."

Write-Step "Running ML training pipeline..."
docker compose --project-name $ProjectName run --rm `
    -e MLFLOW_TRACKING_URI=http://mlflow:5000 `
    -e MLFLOW_S3_ENDPOINT_URL=http://minio:9000 `
    -e "AWS_ACCESS_KEY_ID=$($env:MINIO_ACCESS_KEY)" `
    -e "AWS_SECRET_ACCESS_KEY=$($env:MINIO_SECRET_KEY)" `
    -e "MINIO_BUCKET_MODELS=$bucketModels" `
    -v "${PWD}/ml:/app/ml" `
    ml-inference-worker `
    python /app/ml/train.py
if ($LASTEXITCODE -eq 0) { Write-Ok "ML training complete." }
else { Write-Warn "Training exited with code $LASTEXITCODE. Check logs: docker compose logs ml-inference-worker" }

# --- Step g: Verify MLflow registration -------------------------
Write-Step "Step g: Verifying MLflow run registration..."
try {
    $runs = Invoke-RestMethod -Uri "http://localhost:5000/api/2.0/mlflow/runs/search" `
        -Method POST -ContentType "application/json" `
        -Body '{"experiment_ids":[],"max_results":1}' -ErrorAction Stop
    $runId = $runs.runs[0].info.run_id
    Write-Ok "Model registered in MLflow. Latest run: $runId"
} catch {
    Write-Warn "Could not verify MLflow run. Check http://localhost:5000 manually."
}

# --- Done -------------------------------------------------------
Write-Host ""
Write-Host "═══════════════════════════════════════════════════════" -ForegroundColor Green
Write-Host "  Bootstrap complete! System ready." -ForegroundColor Green
Write-Host "═══════════════════════════════════════════════════════" -ForegroundColor Green
Write-Host ""
Write-Host "  Service URLs:" -ForegroundColor Cyan
Write-Host "  Grafana      → http://localhost:3000  (admin / `$GRAFANA_ADMIN_PASSWORD)"
Write-Host "  FastAPI Docs → http://localhost:8000/docs"
Write-Host "  Kafka UI     → http://localhost:8080"
Write-Host "  MLflow       → http://localhost:5000"
Write-Host "  MinIO        → http://localhost:9001"
Write-Host ""
Write-Host "  Next: .\scripts\smoke_test.ps1" -ForegroundColor Cyan
Write-Host ""
