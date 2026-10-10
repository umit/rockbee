#!/usr/bin/env bash
set -euo pipefail

# LocalStack ready.d hook; these are local emulator credentials only.
export AWS_DEFAULT_REGION="${AWS_DEFAULT_REGION:-us-east-1}"
export AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID:-test}"
export AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY:-test}"

bucket=rockbee-test
if ! awslocal s3api head-bucket --bucket "$bucket" >/dev/null 2>&1; then
    if [[ "$AWS_DEFAULT_REGION" == us-east-1 ]]; then
        awslocal s3api create-bucket --bucket "$bucket"
    else
        awslocal s3api create-bucket --bucket "$bucket" \
            --create-bucket-configuration "LocationConstraint=$AWS_DEFAULT_REGION"
    fi
fi

awslocal s3api head-bucket --bucket "$bucket"
printf 'LocalStack S3 bucket ready: %s\n' "$bucket"
