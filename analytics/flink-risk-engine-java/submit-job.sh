#!/bin/bash
set -e

# CHANGED (P1 resolution): this container no longer runs the job's main()
# directly. It builds/holds the JAR and SUBMITS it to the real
# flink-jobmanager/flink-taskmanager cluster via the `flink` CLI (bundled
# in this image, same as jobmanager/taskmanager use), instead of falling
# back to Flink's embedded LocalStreamEnvironment inside its own process.
# Once submitted with -d (detached), the job runs on the cluster
# independently of this container's lifecycle -- this container's job is
# done, and it exits. Expect `docker compose ps` to show this container as
# Exited (0) shortly after a successful run, same pattern as kafka-init /
# connect-init -- that's correct, not a crash.

JOBMANAGER_HOST="${FLINK_JOBMANAGER_HOST:-flink-jobmanager}"
JOBMANAGER_PORT="${FLINK_JOBMANAGER_PORT:-8081}"
JOB_NAME="banking-fraud-risk-engine-v2"   # must match env.execute(...) in RiskScoringJob.java
TARGET="${JOBMANAGER_HOST}:${JOBMANAGER_PORT}"

echo "Waiting for JobManager at ${TARGET} to accept CLI commands..."
# Deliberately using `flink list` itself as the readiness probe rather than
# curl -- curl's presence inside the official Flink image isn't something
# to assume (same reasoning as the docker-compose.yml healthchecks
# elsewhere in this project), whereas the flink CLI is guaranteed present
# in this exact image.
until flink list -m "${TARGET}" >/dev/null 2>&1; do
  echo "  ...JobManager not ready yet, retrying in 5s"
  sleep 5
done

echo "Checking for an already-running instance of '${JOB_NAME}'..."
if flink list -m "${TARGET}" --running 2>/dev/null | grep -q "${JOB_NAME}"; then
  echo "'${JOB_NAME}' is already running on the cluster -- not resubmitting."
  echo "(A plain container restart without an explicit cancel would"
  echo "otherwise launch a duplicate job instance competing on the same"
  echo "Kafka consumer group, fraud-engine-v2 -- this check prevents that."
  echo "To deploy a CODE CHANGE, cancel the existing job first --"
  echo "  flink cancel <job-id> -m ${TARGET}"
  echo "or via the Web UI at http://localhost:8082 -- then re-run this"
  echo "container so the new JAR gets submitted.)"
  exit 0
fi

echo "Submitting ${JOB_NAME} to ${TARGET}..."
flink run -m "${TARGET}" -d /app/app.jar
echo "Submission complete -- job now runs on flink-taskmanager, independent of this container."