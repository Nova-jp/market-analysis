#!/bin/bash
# Cloud Scheduler Setup Script for Market Analytics
# This script sets up the required cron jobs on Google Cloud Scheduler.

# Configuration
SERVICE_NAME="market-analytics"
REGION="asia-northeast1"
PROJECT_ID=${GOOGLE_CLOUD_PROJECT:-"turnkey-diode-472203-q6"}

echo "🚀 Setting up Cloud Scheduler for project: $PROJECT_ID"

# 1. Get Cloud Run Service URL
echo "🔍 Retrieving Cloud Run Service URL..."
SERVICE_URL=$(gcloud run services describe $SERVICE_NAME --region $REGION --project $PROJECT_ID --format 'value(status.url)')

if [ -z "$SERVICE_URL" ]; then
    echo "❌ Error: Could not find Cloud Run service URL."
    echo "Please ensure the service '$SERVICE_NAME' is deployed in region '$REGION'."
    exit 1
fi

echo "✅ Found Service URL: $SERVICE_URL"

# Helper function to create or update job
create_job() {
    local JOB_NAME=$1
    local SCHEDULE=$2
    local ENDPOINT=$3
    local DESCRIPTION=$4

    echo "---------------------------------------------------"
    echo "Configuring job: $JOB_NAME"
    echo "  Schedule: $SCHEDULE"
    echo "  Endpoint: $ENDPOINT"

    # Check if job exists
    gcloud scheduler jobs describe $JOB_NAME --location $REGION --project $PROJECT_ID >/dev/null 2>&1
    
    if [ $? -eq 0 ]; then
        echo "  🔄 Job exists, updating..."
        gcloud scheduler jobs update http $JOB_NAME \
            --schedule="$SCHEDULE" \
            --uri="$SERVICE_URL$ENDPOINT" \
            --time-zone="Asia/Tokyo" \
            --http-method=POST \
            --headers="X-CloudScheduler=true,User-Agent=Google-Cloud-Scheduler" \
            --location="$REGION" \
            --project="$PROJECT_ID" \
            --description="$DESCRIPTION"
    else
        echo "  ✨ Job does not exist, creating..."
        gcloud scheduler jobs create http $JOB_NAME \
            --schedule="$SCHEDULE" \
            --uri="$SERVICE_URL$ENDPOINT" \
            --time-zone="Asia/Tokyo" \
            --http-method=POST \
            --headers="X-CloudScheduler=true,User-Agent=Google-Cloud-Scheduler" \
            --location="$REGION" \
            --project="$PROJECT_ID" \
            --description="$DESCRIPTION"
    fi
}

# 2. Setup Jobs

# Macro Economic Data Collection (Stock, FX, US Yields, CPI) - 07:00
create_job "daily-macro-collection" "0 7 * * *" "/api/scheduler/macro-daily-collection" "Daily Macro Economic Data (Stock/FX/Yields) collection"

# Daily Data Collection (JSDA) - 18:00
create_job "daily-bond-collection" "0 18 * * *" "/api/scheduler/daily-collection" "Daily JSDA bond market data collection"

# IRS Data Collection - 21:00
create_job "daily-irs-collection" "0 21 * * *" "/api/scheduler/irs-daily-collection" "Daily IRS (Swap) data collection"

# ASW Calculation - 21:30 (After IRS data is ready)
create_job "daily-asw-calculation" "30 21 * * *" "/api/scheduler/asw-daily-calculation" "Daily Asset Swap Spread calculation"

# Auction Data Collection - 12:36 (Weekdays)
create_job "daily-auction-collection" "36 12 * * 1-5" "/api/scheduler/auction-collection" "Daily MOF auction result collection"

# Calendar Refresh - 06:00 on 1st of month
create_job "monthly-calendar-refresh" "0 6 1 * *" "/api/scheduler/calendar-refresh" "Monthly MOF auction calendar refresh"

# International Transactions Collection (Polling every Thursday)
# MOF usually updates around 08:50. We poll multiple times to ensure data is captured.
create_job "weekly-intl-trans-0845" "45 8 * * 4" "/api/scheduler/international-transactions-collection" "Weekly MOF International Transactions (08:45)"
create_job "weekly-intl-trans-0915" "15 9 * * 4" "/api/scheduler/international-transactions-collection" "Weekly MOF International Transactions (09:15)"
create_job "weekly-intl-trans-0945" "45 9 * * 4" "/api/scheduler/international-transactions-collection" "Weekly MOF International Transactions (09:45)"
create_job "weekly-intl-trans-1015" "15 10 * * 4" "/api/scheduler/international-transactions-collection" "Weekly MOF International Transactions (10:15)"
create_job "weekly-intl-trans-1215" "15 12 * * 4" "/api/scheduler/international-transactions-collection" "Weekly MOF International Transactions (12:15)"

echo "---------------------------------------------------"
echo "🎉 Cloud Scheduler setup complete!"
