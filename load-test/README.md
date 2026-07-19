# Load Testing for baltazar-gateway

Load testing setup for the baltazar-gateway using Locust and WireMock.

## Architecture

- **Gateway**: The actual baltazar-gateway service
- **Mock Backend**: WireMock server simulating the backend API
- **Redis**: Redis pub/sub for real-time messaging
- **Locust**: Load testing tool running on your host machine

## WireMock Mappings

**Important**: The load test **reuses** the same WireMock mappings as integration tests.

- Source: `src/test/resources/wiremock/`
- No duplication - Dockerfile.mock copies directly from test resources
- Any changes to integration test mocks automatically apply to load tests

## Prerequisites

```bash
# Python 3.10+
pip install -r load-test/requirements.txt

# Docker & Docker Compose
docker --version
docker compose version
```

## Running Load Tests

### Local Mode (Recommended)

Tests against a local stack (Redis + mock backend + gateway):

```bash
# Start local stack and Locust web UI
./load-test/run.sh local

# Or headless mode with specific parameters
./load-test/run.sh local --headless -u 200 -r 20 --run-time 5m
```

Then open http://localhost:8089 to configure and start the load test.

**Services:**
- Locust UI: http://localhost:8089
- Gateway: http://localhost:8080
- WireMock Admin: http://localhost:9090/__admin/

### Remote Mode

Tests against a remote environment (test/prod):

```bash
# Against test environment
GATEWAY_TEST_HOST=wss://test.example.com ./load-test/run.sh remote test

# Against prod environment
GATEWAY_PROD_HOST=wss://prod.example.com ./load-test/run.sh remote prod
```

You'll be prompted for:
- Bearer token (JWT)
- Encryption key (Base64 24-byte 3DES key)

### Stop Local Stack

```bash
./load-test/run.sh stop
```

## Load Test Scenarios

The load test simulates real game sessions with weighted tasks:

- **MOVE_CONFIRM** (weight: 10) - Player makes a move
- **FETCH_DATA** (weight: 5) - Player fetches game state
- **GET_HINT** (weight: 3) - Player requests a hint
- **START_TIMER** (weight: 2) - Start move timer
- **RESET_TIME** (weight: 2) - Reset timer
- **SEND_EMOJI** (weight: 1) - Send emoji
- **SEND_CHAT** (weight: 1) - Send chat message

Each virtual user:
1. Opens WebSocket connection to `/ws`
2. Sends raw STOMP CONNECT frame
3. Subscribes to `/topic/public/{gameId}`
4. Continuously sends random game commands
5. Receives server pushes in background thread

## Protocol

**Raw STOMP over WebSocket** (no SockJS):

```
Client → Gateway:  CONNECT\n...\0
Gateway → Client:  CONNECTED\n...\0
Client → Gateway:  SEND\n...\0
Gateway → Client:  MESSAGE\n...\0
```

## Configuration

Environment variables:

- `LOAD_ENV` - `local` or `remote` (default: `local`)
- `AUTH_TOKEN` - Bearer token for authentication
- `ENC_KEY` - Base64 24-byte 3DES encryption key
- `GAME_IDS` - Comma-separated game IDs (default: `1,2,3,4,5`)

## Metrics

Locust reports:

- `ws_connect` - WebSocket connection + STOMP handshake latency
- `ws_subscribe` - SUBSCRIBE frame send latency
- `cmd_MOVE_CONFIRM`, `cmd_FETCH_DATA`, etc. - Individual command latencies
- Total RPS (requests per second)
- Failure rate
- Response time percentiles (50th, 95th, 99th)

## Troubleshooting

**Gateway not healthy:**
```bash
docker compose -f load-test/docker-compose.local.yml logs gateway
```

**WireMock not responding:**
```bash
docker compose -f load-test/docker-compose.local.yml logs mock-backend
curl http://localhost:9090/__admin/health
```

**Check WireMock mappings:**
```bash
curl http://localhost:9090/__admin/mappings
```

**Rebuild everything:**
```bash
docker compose -f load-test/docker-compose.local.yml down
docker compose -f load-test/docker-compose.local.yml build --no-cache
./load-test/run.sh local
```
