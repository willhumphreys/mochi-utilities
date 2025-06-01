#!/bin/bash

# Set variables
JOB_QUEUE="fargateSpotTrades"  # Replace with your job queue name if different
REGION="eu-central-1"           # Replace with your AWS region
AWS_PROFILE="mochi-admin"       # Add this line for your profile
NAME_PREFIX="Trades"            # Jobs that start with this name will be terminated

echo "Starting to terminate AWS Batch jobs in queue: $JOB_QUEUE with name prefix: $NAME_PREFIX"

# Get list of all active jobs (SUBMITTED, PENDING, RUNNABLE, STARTING, RUNNING)
job_statuses=("SUBMITTED" "PENDING" "RUNNABLE" "STARTING" "RUNNING")

for status in "${job_statuses[@]}"; do
  echo "Fetching $status jobs..."

  # Get jobs with their IDs and names
  job_list=$(aws batch list-jobs \
    --job-queue $JOB_QUEUE \
    --job-status $status \
    --region $REGION \
    --profile $AWS_PROFILE \
    --query "jobSummaryList[].{jobId:jobId,jobName:jobName}" \
    --output json)

  # If no jobs with this status, continue to next status
  if [ "$job_list" == "[]" ]; then
    echo "No $status jobs found."
    continue
  fi

  # Count and filter jobs that start with the specified prefix
  matching_jobs=()
  while read -r job_id job_name; do

    matching_jobs+=("$job_id")
    echo "Found matching job: $job_name (ID: $job_id)"

  done < <(echo "$job_list" | jq -r '.[] | .jobId + " " + .jobName')

  job_count=${#matching_jobs[@]}

  if [ $job_count -eq 0 ]; then
    echo "No $status jobs with name prefix '$NAME_PREFIX' found."
    continue
  fi

  echo "Found $job_count $status jobs with name prefix '$NAME_PREFIX'. Terminating..."

  # Terminate each matching job in the background
  for job_id in "${matching_jobs[@]}"; do
    echo "Terminating job: $job_id"
    aws batch terminate-job \
      --job-id $job_id \
      --reason "Manual termination via cleanup script" \
      --region $REGION \
      --profile $AWS_PROFILE &

    # Small pause to avoid overwhelming the system with too many background processes
    sleep 0.1
  done

  echo "All matching $status jobs termination commands have been issued."
done

# Wait for all background processes to complete
wait

echo "Job termination process complete!"