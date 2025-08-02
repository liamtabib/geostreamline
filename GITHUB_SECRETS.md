# GitHub Secrets Configuration

This document outlines the required GitHub repository secrets for the Maps Data Pipeline workflow.

## Required Secrets

### Authentication & Project Configuration
- **GCP_SECRETS**: JSON service account key for Google Cloud Platform authentication
- **GCP_PROJECT_ID**: Your Google Cloud Project ID (e.g., `pipeline-466508`)

### Storage Configuration  
- **GCS_BUCKET**: Google Cloud Storage bucket name (e.g., `pipeline_bucket_liamtab_dev`)

### API Keys
- **GOOGLE_MAPS_API_KEY**: Google Maps Area Insights API key for data ingestion

## How to Set Secrets

1. Go to your repository on GitHub
2. Navigate to Settings → Secrets and variables → Actions
3. Click "New repository secret"
4. Add each secret with the exact name and value

## Secret Usage in Workflow

These secrets are mapped to environment variables in the GitHub Actions workflow:

```yaml
env:
  GCP_PROJECT_ID: ${{ secrets.GCP_PROJECT_ID }}
  GCS_BUCKET_NAME: ${{ secrets.GCS_BUCKET }}
  GOOGLE_MAPS_API_KEY: ${{ secrets.GOOGLE_MAPS_API_KEY }}
  GOOGLE_APPLICATION_CREDENTIALS: ${{ steps.auth.outputs.credentials_file_path }}
```

## Security Notes

- Service account JSON should have minimal required permissions
- API keys should be restricted to specific APIs and IP ranges when possible
- Secrets are automatically masked in workflow logs
- Review and rotate secrets periodically

## Troubleshooting

If the workflow fails with authentication errors:
1. Verify all secrets are set correctly
2. Check service account permissions in GCP Console
3. Ensure API keys are active and have proper quotas
4. Validate JSON formatting for GCP_SECRETS