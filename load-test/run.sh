#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# Load-test runner for baltazar-gateway
#
# Usage:
#   ./load-test/run.sh local                    # local stack, web UI on :8089
#   ./load-test/run.sh local --headless -u 200 -r 20 --run-time 5m
#   ./load-test/run.sh remote test              # against test env (prompts for token)
#   ./load-test/run.sh remote prod              # against prod env (prompts for token)
#
# Environment variables (remote mode):
#   AUTH_TOKEN   — Bearer <jwt>  (or set interactively below)
#   ENC_KEY      — Base64 24-byte 3DES key
#   GAME_IDS     — comma-separated game IDs to use (default: 1,2,3,4,5)
# ---------------------------------------------------------------------------

set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR/.."

# Pick the active Docker context so docker compose inherits it correctly.
# 'docker context show' returns the current context name (e.g. "colima", "default").
DOCKER_CONTEXT="$(docker context show 2>/dev/null || echo default)"
DC="docker --context $DOCKER_CONTEXT compose"

MODE="${1:-local}"
shift || true

# ---------------------------------------------------------------------------
case "$MODE" in

  local)
    echo "==> Starting local stack (Redis + mock-backend + gateway)..."
    $DC -f "$SCRIPT_DIR/docker-compose.local.yml" up -d --build

    echo "==> Waiting for gateway to be healthy..."
    for i in $(seq 1 24); do
      if curl -sf http://localhost:8080/actuator/health > /dev/null 2>&1; then
        echo "    Gateway is up."
        break
      fi
      echo "    [$i/24] Not ready yet, waiting 5s..."
      sleep 5
    done

    echo ""
    echo "==> Starting Locust (web UI at http://localhost:8089)"
    echo "    WireMock admin UI: http://localhost:9090/__admin/"
    echo "    Press Ctrl-C to stop. Stack will keep running."
    echo ""
    LOAD_ENV=local \
    ENC_KEY="${ENC_KEY:-YWJjZGVmZ2hpamtsbW5vcHFyc3R1dnd4}" \
    GAME_IDS="${GAME_IDS:-1,2,3,4,5}" \
      locust -f load-test/locustfile.py \
        --host ws://localhost:8080 \
        --web-port 8089 \
        "$@"
    ;;

  remote)
    ENV_NAME="${1:-test}"
    shift || true

    # Resolve host
    case "$ENV_NAME" in
      test)  HOST="${GATEWAY_TEST_HOST:?Set GATEWAY_TEST_HOST=wss://your-test-domain}" ;;
      prod)  HOST="${GATEWAY_PROD_HOST:?Set GATEWAY_PROD_HOST=wss://your-prod-domain}" ;;
      *)     HOST="$ENV_NAME" ;;   # treat as literal URL
    esac

    # Auth token
    if [ -z "${AUTH_TOKEN:-}" ]; then
      read -rsp "Enter Bearer token (hidden): " AUTH_TOKEN
      echo ""
      AUTH_TOKEN="Bearer $AUTH_TOKEN"
    fi

    # Encryption key
    if [ -z "${ENC_KEY:-}" ]; then
      read -rsp "Enter Base64 3DES key (hidden): " ENC_KEY
      echo ""
    fi

    echo ""
    echo "==> Starting Locust against $HOST (web UI at http://localhost:8089)"
    echo ""
    LOAD_ENV=remote \
    AUTH_TOKEN="$AUTH_TOKEN" \
    ENC_KEY="$ENC_KEY" \
    GAME_IDS="${GAME_IDS:-1,2,3,4,5}" \
      locust -f load-test/locustfile.py \
        --host "$HOST" \
        --web-port 8089 \
        "$@"
    ;;

  stop)
    echo "==> Stopping local stack..."
    $DC -f "$SCRIPT_DIR/docker-compose.local.yml" down
    ;;

  *)
    echo "Usage: $0 {local|remote [test|prod|<url>]|stop} [locust args...]"
    exit 1
    ;;
esac
