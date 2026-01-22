#!/bin/bash

# Run the Multi-Code Location Dagster Demo

# Set DAGSTER_HOME to store daemon metadata locally
export DAGSTER_HOME="$(pwd)/.dagster_home"
mkdir -p "$DAGSTER_HOME"

echo "Starting Multi-Code Location Dagster Demo..."
echo ""
echo "This will start the Dagster UI with all 5 code locations:"
echo "  - fintech_alpha (Snowflake + dbt + Databricks + Azure)"
echo "  - insurance_beta (Streaming + Azure DW + Fraud Detection)"
echo "  - healthcare_gamma (On-prem EHR + HIPAA Compliance)"
echo "  - shared (Common utilities)"
echo "  - shared_analytics (🔗 Cross-company analytics & lineage)"
echo ""
echo "🔗 NEW: Check out the global asset lineage graph to see"
echo "   cross-code location dependencies!"
echo ""
echo "🤖 Starting both webserver AND daemon for full automation support"
echo ""
echo "Navigate to: http://localhost:3000"
echo ""

# Cleanup function to kill both processes on exit
cleanup() {
    echo ""
    echo "Shutting down Dagster webserver and daemon..."
    if [ ! -z "$DAEMON_PID" ]; then
        kill $DAEMON_PID 2>/dev/null
    fi
    if [ ! -z "$WEBSERVER_PID" ]; then
        kill $WEBSERVER_PID 2>/dev/null
    fi
    exit 0
}

# Set up trap to catch Ctrl+C and clean up
trap cleanup INT TERM

# Start the daemon in the background
echo "Starting Dagster daemon (for schedules & automation conditions)..."
uv run dagster-daemon run -w workspace.yaml &
DAEMON_PID=$!

# Give the daemon a moment to start
sleep 2

# Start the webserver in the foreground
echo "Starting Dagster webserver..."
uv run dagster-webserver -w workspace.yaml &
WEBSERVER_PID=$!

# Wait for both processes
wait
