# deploy.ps1 - Windows PowerShell equivalent of deploy.sh
# Usage: $env:ALERT_EMAIL="you@example.com"; .\deploy.ps1 csv-pipeline
param(
    [string]$StackName = "csv-pipeline"
)

# Validate ALERT_EMAIL
if (-not $env:ALERT_EMAIL) {
    Write-Error "Set ALERT_EMAIL environment variable first:`n  `$env:ALERT_EMAIL = 'you@example.com'"
    exit 1
}

Write-Host "`n==> Building SAM application..." -ForegroundColor Cyan
sam build
if ($LASTEXITCODE -ne 0) { Write-Error "sam build failed"; exit 1 }

Write-Host "`n==> Deploying stack '$StackName' to AWS..." -ForegroundColor Cyan
sam deploy `
    --stack-name $StackName `
    --resolve-s3 `
    --capabilities CAPABILITY_IAM `
    --no-fail-on-empty-changeset `
    --parameter-overrides ProjectName=$StackName AlertEmail=$env:ALERT_EMAIL
if ($LASTEXITCODE -ne 0) { Write-Error "sam deploy failed"; exit 1 }

Write-Host "`n==> Fetching S3 bucket name from stack outputs..." -ForegroundColor Cyan
$BUCKET = aws cloudformation describe-stacks `
    --stack-name $StackName `
    --query "Stacks[0].Outputs[?OutputKey=='DataBucketName'].OutputValue" `
    --output text
if ($LASTEXITCODE -ne 0) { Write-Error "Could not fetch bucket name"; exit 1 }
Write-Host "    Data bucket: $BUCKET" -ForegroundColor Green

Write-Host "`n==> Uploading Glue script to s3://$BUCKET/scripts/..." -ForegroundColor Cyan
aws s3 cp glue\csv_to_postgres.py "s3://$BUCKET/scripts/csv_to_postgres.py"
if ($LASTEXITCODE -ne 0) { Write-Error "Glue script upload failed"; exit 1 }

Write-Host "`n========================================" -ForegroundColor Green
Write-Host " Deployment complete!" -ForegroundColor Green
Write-Host "========================================" -ForegroundColor Green
Write-Host " Bucket : $BUCKET"
Write-Host ""
Write-Host " NEXT: Confirm the SNS subscription email sent to $env:ALERT_EMAIL"
Write-Host "       Then run a test:"
Write-Host "   aws s3 cp tests\data\orders_20261106.csv s3://$BUCKET/incoming/"
Write-Host ""
Write-Host " To save the bucket name for later tests:"
Write-Host "   `$env:B = '$BUCKET'"

