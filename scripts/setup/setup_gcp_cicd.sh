#!/usr/bin/env bash
# GCP Workload Identity Federation + GitHub Actions デプロイ用 SA のセットアップ
set -euo pipefail

# ── 色付き出力 ──────────────────────────────────────────────────
GREEN='\033[0;32m'; YELLOW='\033[1;33m'; RED='\033[0;31m'; RESET='\033[0m'
info()    { echo -e "${GREEN}[INFO]${RESET} $*"; }
warn()    { echo -e "${YELLOW}[WARN]${RESET} $*"; }
error()   { echo -e "${RED}[ERROR]${RESET} $*" >&2; exit 1; }
section() { echo -e "\n${GREEN}=== $* ===${RESET}"; }

# ── 前提チェック ────────────────────────────────────────────────
command -v gcloud >/dev/null || error "gcloud が見つかりません。Cloud SDK をインストールしてください。"
command -v gh     >/dev/null || warn  "gh (GitHub CLI) が見つかりません。Secrets は手動登録が必要です。"

# ── 設定値の入力 ────────────────────────────────────────────────
section "設定値の確認"

# GCP プロジェクト ID
DEFAULT_PROJECT=$(gcloud config get-value project 2>/dev/null || echo "")
if [[ -n "$DEFAULT_PROJECT" ]]; then
    read -rp "GCP プロジェクト ID [${DEFAULT_PROJECT}]: " PROJECT_ID
    PROJECT_ID="${PROJECT_ID:-$DEFAULT_PROJECT}"
else
    read -rp "GCP プロジェクト ID: " PROJECT_ID
fi
[[ -z "$PROJECT_ID" ]] && error "プロジェクト ID が未入力です。"

# GitHub リポジトリ
read -rp "GitHub リポジトリ (例: Nova-jp/market-analytics-ver1): " GITHUB_REPO
[[ -z "$GITHUB_REPO" ]] && error "GitHub リポジトリが未入力です。"
GITHUB_ORG="${GITHUB_REPO%%/*}"

# 固定値（変更が必要な場合は書き換えてください）
SA_NAME="github-actions-deployer"
POOL_ID="github-actions-pool"
PROVIDER_ID="github-provider"
REGION="asia-northeast1"
SERVICE="market-analytics"

echo ""
echo "以下の設定でセットアップを実行します:"
echo "  GCP プロジェクト ID : $PROJECT_ID"
echo "  GitHub リポジトリ   : $GITHUB_REPO"
echo "  Service Account    : ${SA_NAME}@${PROJECT_ID}.iam.gserviceaccount.com"
echo "  WIF Pool           : $POOL_ID"
echo "  WIF Provider       : $PROVIDER_ID"
echo ""
read -rp "続けますか？ [y/N]: " CONFIRM
[[ "$CONFIRM" != "y" && "$CONFIRM" != "Y" ]] && { info "中止しました。"; exit 0; }

# ── Step 1: API 有効化 ──────────────────────────────────────────
section "Step 1: API 有効化"
gcloud services enable \
    iam.googleapis.com \
    iamcredentials.googleapis.com \
    cloudresourcemanager.googleapis.com \
    run.googleapis.com \
    artifactregistry.googleapis.com \
    --project "$PROJECT_ID"
info "API を有効化しました。"

# ── Step 2: Service Account 作成 ───────────────────────────────
section "Step 2: Service Account 作成"
SA_EMAIL="${SA_NAME}@${PROJECT_ID}.iam.gserviceaccount.com"

if gcloud iam service-accounts describe "$SA_EMAIL" --project "$PROJECT_ID" &>/dev/null; then
    warn "Service Account ${SA_EMAIL} はすでに存在します。スキップします。"
else
    gcloud iam service-accounts create "$SA_NAME" \
        --display-name "GitHub Actions Deployer" \
        --project "$PROJECT_ID"
    info "Service Account を作成しました: ${SA_EMAIL}"
fi

# ── Step 3: IAM 権限付与 ────────────────────────────────────────
section "Step 3: IAM 権限付与"

gcloud projects add-iam-policy-binding "$PROJECT_ID" \
    --member "serviceAccount:${SA_EMAIL}" \
    --role "roles/artifactregistry.writer" \
    --condition=None \
    --quiet
info "roles/artifactregistry.writer を付与しました。"

gcloud projects add-iam-policy-binding "$PROJECT_ID" \
    --member "serviceAccount:${SA_EMAIL}" \
    --role "roles/run.developer" \
    --condition=None \
    --quiet
info "roles/run.developer を付与しました。"

# Cloud Run が使う Compute Engine デフォルト SA への act-as 権限
COMPUTE_SA=$(gcloud iam service-accounts list \
    --filter="email:compute@developer.gserviceaccount.com" \
    --format="value(email)" \
    --project "$PROJECT_ID")

if [[ -n "$COMPUTE_SA" ]]; then
    gcloud iam service-accounts add-iam-policy-binding "$COMPUTE_SA" \
        --member "serviceAccount:${SA_EMAIL}" \
        --role "roles/iam.serviceAccountUser" \
        --project "$PROJECT_ID" \
        --quiet
    info "Compute Engine SA への serviceAccountUser を付与しました。"
else
    warn "Compute Engine デフォルト SA が見つかりませんでした。Cloud Run の act-as 権限を手動で確認してください。"
fi

# ── Step 4: Workload Identity Pool 作成 ────────────────────────
section "Step 4: Workload Identity Pool 作成"

if gcloud iam workload-identity-pools describe "$POOL_ID" \
        --location global --project "$PROJECT_ID" &>/dev/null; then
    warn "WIF Pool ${POOL_ID} はすでに存在します。スキップします。"
else
    gcloud iam workload-identity-pools create "$POOL_ID" \
        --location global \
        --display-name "GitHub Actions Pool" \
        --project "$PROJECT_ID"
    info "WIF Pool を作成しました。反映を待っています..."
    sleep 10
fi

POOL_RESOURCE=$(gcloud iam workload-identity-pools describe "$POOL_ID" \
    --location global --project "$PROJECT_ID" --format "value(name)")

# ── Step 5: WIF Provider 作成 ──────────────────────────────────
section "Step 5: Workload Identity Provider 作成"

if gcloud iam workload-identity-pools providers describe "$PROVIDER_ID" \
        --location global \
        --workload-identity-pool "$POOL_ID" \
        --project "$PROJECT_ID" &>/dev/null; then
    warn "WIF Provider ${PROVIDER_ID} はすでに存在します。スキップします。"
else
    gcloud iam workload-identity-pools providers create-oidc "$PROVIDER_ID" \
        --location global \
        --workload-identity-pool "$POOL_ID" \
        --issuer-uri "https://token.actions.githubusercontent.com" \
        --attribute-mapping \
            "google.subject=assertion.sub,attribute.repository=assertion.repository" \
        --attribute-condition "assertion.repository == '${GITHUB_REPO}'" \
        --project "$PROJECT_ID"
    info "WIF Provider を作成しました: ${PROVIDER_ID}"
fi

PROVIDER_RESOURCE=$(gcloud iam workload-identity-pools providers describe "$PROVIDER_ID" \
    --location global \
    --workload-identity-pool "$POOL_ID" \
    --project "$PROJECT_ID" \
    --format "value(name)")

# ── Step 6: SA への WIF バインディング ─────────────────────────
section "Step 6: Service Account への WIF バインディング"

gcloud iam service-accounts add-iam-policy-binding "$SA_EMAIL" \
    --role "roles/iam.workloadIdentityUser" \
    --member "principalSet://iam.googleapis.com/${POOL_RESOURCE}/attribute.repository/${GITHUB_REPO}" \
    --project "$PROJECT_ID" \
    --quiet
info "WIF バインディングを設定しました。"

# ── Step 7: GitHub Secrets 登録 ────────────────────────────────
section "Step 7: GitHub Secrets 登録"

echo ""
echo "以下の値を GitHub Secrets に登録します:"
echo "  GCP_PROJECT_ID      = $PROJECT_ID"
echo "  WIF_PROVIDER        = $PROVIDER_RESOURCE"
echo "  WIF_SERVICE_ACCOUNT = $SA_EMAIL"
echo ""

if command -v gh &>/dev/null; then
    if gh auth status &>/dev/null; then
        gh secret set GCP_PROJECT_ID       --body "$PROJECT_ID"       --repo "$GITHUB_REPO"
        gh secret set WIF_PROVIDER         --body "$PROVIDER_RESOURCE" --repo "$GITHUB_REPO"
        gh secret set WIF_SERVICE_ACCOUNT  --body "$SA_EMAIL"          --repo "$GITHUB_REPO"
        info "GitHub Secrets を登録しました。"
    else
        warn "gh の認証が必要です。'gh auth login' を実行してから再度 Secrets を登録してください。"
        warn "または上記の値をリポジトリの Settings → Secrets → Actions から手動登録してください。"
    fi
else
    warn "gh コマンドが見つかりません。以下を手動で登録してください:"
    warn "https://github.com/${GITHUB_REPO}/settings/secrets/actions"
fi

# ── 完了 ────────────────────────────────────────────────────────
section "セットアップ完了"
echo ""
echo "次のステップ:"
echo "  1. GitHub Secrets が登録されていることを確認"
echo "     https://github.com/${GITHUB_REPO}/settings/secrets/actions"
echo ""
echo "  2. main ブランチに push して GitHub Actions のデプロイを確認"
echo "     https://github.com/${GITHUB_REPO}/actions"
echo ""
echo "  3. デプロイ後のサービス URL を確認"
echo "     gcloud run services describe ${SERVICE} --region ${REGION} --project ${PROJECT_ID} --format 'value(status.url)'"
