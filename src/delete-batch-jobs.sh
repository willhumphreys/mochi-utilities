#!/bin/bash

# Set variables
JOB_QUEUE="fargateSpotTrades"  # Replace with your job queue name if different
REGION="eu-central-1"           # Replace with your AWS region
AWS_PROFILE="mochi-admin"       # Add this line for your profile

echo "Starting to terminate all AWS Batch jobs in queue: $JOB_QUEUE"

# Get list of all active jobs (SUBMITTED, PENDING, RUNNABLE, STARTING, RUNNING)
job_statuses=("SUBMITTED" "PENDING" "RUNNABLE" "STARTING" "RUNNING")

for status in "${job_statuses[@]}"; do
  echo "Fetching $status jobs..."

  job_ids=$(aws batch list-jobs \
    --job-queue $JOB_QUEUE \
    --job-status $status \
    --region $REGION \
    --profile $AWS_PROFILE \
    --query "jobSummaryList[].jobId" \
    --output text)

  # If no jobs with this status, continue to next status
  if [ -z "$job_ids" ]; then
    echo "No $status jobs found."
    continue
  fi

  # Count how many jobs were found
  job_count=$(echo "$job_ids" | wc -w)
  echo "Found $job_count $status jobs. Terminating..."

  # Terminate each job - note the added --profile parameter
  for job_id in $job_ids; do
    echo "Terminating job: $job_id"
    aws batch terminate-job \
      --job-id $job_id \
      --reason "Manual termination via cleanup script" \
      --region $REGION \
      --profile $AWS_PROFILE

    # Small pause to avoid API throttling
    sleep 0.5
  done

  echo "All $status jobs have been terminated."
done

echo "Job termination process complete!"