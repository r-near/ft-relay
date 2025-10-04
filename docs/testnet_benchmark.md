# Testnet 60k Transfer Benchmark Playbook

This guide explains how to rerun the 60,000 transfer benchmark from `tests/integrated_benchmark.rs::test_bounty_requirement_60k` against **testnet** instead of the local sandbox. It follows the same batching and throughput targets: ≥60,000 logical FT transfers, ≥100 requests/second sustained, and completion within 10 minutes.

The playbook is split into four phases:

1. **Provision** – create accounts, fund them, and deploy the FT contract.
2. **Prepare** – register receivers and add function-call access keys.
3. **Execute** – launch the relay and drive 60k HTTP requests.
4. **Verify** – confirm balances on-chain and capture metrics.

---

## 0. Prerequisites

- `near` CLI (JS) ≥3.0.0. Install via `npm install -g near-cli`.
- `cargo` with Rust stable (for building `ft-relay`).
- `jq`, `awk`, and `python3` (used by helper snippets below).
- Python package `aiohttp` (for the load generator). Install with `python3 -m pip install aiohttp`.
- Testnet root account funded from <https://wallet.testnet.near.org> or <https://near-faucet.io/>. You need enough NEAR to create subaccounts and cover storage. Budget **~20 NEAR** to be safe (≈2.5 NEAR storage + ≈5 NEAR gas headroom + buffer).
- Access to this repository with `resources/fungible_token.wasm` checked in.

Throughout the guide we use these environment variables. Adjust names to fit your naming scheme and export them before starting:

```bash
export ROOT_ACCOUNT="yourroot.testnet"
export OWNER_ACCOUNT="ft-relay.${ROOT_ACCOUNT}"
export TOKEN_ACCOUNT="$OWNER_ACCOUNT"          # contract deployed to owner
export RECEIVER_PREFIX="recv"                  # subaccount prefix
export RECEIVER_COUNT=20
export RPC_URL="https://rpc.testnet.near.org"
```

> Tip: wrap the exports in a shell script (for example `scripts/export-benchmark-env.sh`) so you can source it in later sessions.

---

## 1. Provision – Create and Fund Accounts

1. **Create the owner account** (only if it does not exist yet):
   ```bash
   near create-account "$OWNER_ACCOUNT" --masterAccount "$ROOT_ACCOUNT" --initialBalance "15"
   ```
   - This gives the owner enough float for storage deposits, FT transfers, and gas.
2. **Verify funding** (should be ≥10 NEAR available):
   ```bash
   near state "$OWNER_ACCOUNT"
   ```

3. **Create receiver accounts** (20 is the default from the integration test). Example helper:
   ```bash
   seq -f "%02g" 0 $((RECEIVER_COUNT-1)) | while read idx; do
     ACCOUNT="${RECEIVER_PREFIX}${idx}.${OWNER_ACCOUNT}"
     near create-account "$ACCOUNT" --masterAccount "$OWNER_ACCOUNT" --initialBalance "2"
   done
   ```
   - Allocate ~2 NEAR per receiver so you can reuse the accounts later. After the run they will still own the tokens.

---

## 2. Prepare – Deploy Contract, Register Receivers, Add Keys

### 2.1 Deploy and Initialize the FT Contract

1. **Deploy**
   ```bash
   near deploy "$TOKEN_ACCOUNT" --wasmFile resources/fungible_token.wasm --initFunction "new" \
     --initArgs '{"owner_id":"'"$OWNER_ACCOUNT"'","total_supply":"1000000000000000000000000000","metadata":{"spec":"ft-1.0.0","name":"Benchmark Token","symbol":"BENCH","decimals":18}}'
   ```
   - This matches the hard-coded initial supply in the integration test (1 billion tokens).

2. **Optional sanity check** – fetch `ft_metadata`:
   ```bash
   near view "$TOKEN_ACCOUNT" ft_metadata
   ```

### 2.2 Register Receivers for Storage

Each receiver must pay the storage deposit once. The integration test uses 0.125 NEAR.

```bash
STORAGE_DEPOSIT="125000000000000000000000"   # 0.125 NEAR in yocto
seq -f "%02g" 0 $((RECEIVER_COUNT-1)) | while read idx; do
  ACCOUNT="${RECEIVER_PREFIX}${idx}.${OWNER_ACCOUNT}"
  near call "$TOKEN_ACCOUNT" storage_deposit '{"account_id":"'"$ACCOUNT"'"}' \
    --accountId "$OWNER_ACCOUNT" --amount 0.125
done
```

### 2.3 Prepare Function-Call Keys for the Relay

The relay expects function-call keys tied to the signer account (`ACCOUNT_ID`). Replicate the pool used in the test (owner key + 2 extra keys = 3 keys total).

1. **Generate local keys** (do not reuse your full-access keys):
   ```bash
   for i in 0 1 2; do
     near generate-key "relay-signer-$i"
   done
   ```
   - Keys are saved under `~/.near-credentials/testnet/relay-signer-$i.json`.

2. **Add each public key to the owner account as a function-call key**:
   ```bash
   for i in 0 1 2; do
     CREDS_FILE="$HOME/.near-credentials/testnet/relay-signer-$i.json"
     PUBKEY=$(jq -r '.public_key' "$CREDS_FILE")
     near add-key "$OWNER_ACCOUNT" "$PUBKEY" \
       --contract-id "$TOKEN_ACCOUNT" \
       --method-names ft_transfer,ft_transfer_call \
       --allowance 10
   done
   ```
   - Allowance `10` ≈ 10 NEAR of prepaid gas per key, more than enough for 60k transfers.

3. **Collect the secret keys for the `.env` file**:
   ```bash
   jq -r '.private_key' ~/.near-credentials/testnet/relay-signer-{0,1,2}.json | paste -sd, -
   ```
   Save the comma-separated string; the relay will read it from `PRIVATE_KEYS`.

---

## 3. Execute – Run the Relay and Fire 60k Requests

### 3.1 Configure the Relay

Create `.env.benchmark` in the repo root:

```
ACCOUNT_ID=ft-relay.yourroot.testnet
PRIVATE_KEYS=ed25519:...,ed25519:...,ed25519:...
RPC_URL=https://rpc.testnet.near.org
BIND_ADDR=127.0.0.1:18082
BATCH_SIZE=90
BATCH_LINGER_MS=20
MAX_INFLIGHT_BATCHES=500
RUST_LOG=info,ft_relay=info,near_api=warn
```

> Keep `ACCOUNT_ID` and `PRIVATE_KEYS` aligned; the first key should belong to `ACCOUNT_ID` and match what you added above.

### 3.2 Launch the Relay

```bash
source .env.benchmark
cargo run --release -- --token "$TOKEN_ACCOUNT"
```

Once the log prints `listening on http://127.0.0.1:18082`, the relay is live.

### 3.3 Drive the Load (60,000 HTTP Requests)

You can use the lightweight Python helper below (requires `aiohttp`). Save it as `scripts/fire_60k.py`:

```python
#!/usr/bin/env python3
import argparse
import asyncio
import json
import time

import aiohttp

CONCURRENT_WORKERS = 200
TOTAL_REQUESTS = 60000
RECEIVER_COUNT = 20

async def worker(idx, session, receivers, results):
    success = 0
    for i, receiver in receivers:
        payload = {
            "receiver_id": receiver,
            "amount": "1000000000000000000"
        }
        try:
            async with session.post("/v1/transfer", json=payload) as resp:
                if resp.status == 200:
                    success += 1
        except Exception:
            pass

        if idx == 0 and i % 10000 == 0 and i > 0:
            elapsed = time.perf_counter() - results["start"]
            print(f"  progress: {i} requests in {elapsed:.1f}s")

    results["success"][idx] = success

async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--rpc", default="http://127.0.0.1:18082")
    parser.add_argument("--receiver-prefix", required=True)
    parser.add_argument("--owner", required=True)
    args = parser.parse_args()

    receivers = [
        (i, f"{args.receiver_prefix}{i:02d}.{args.owner}")
        for i in range(RECEIVER_COUNT)
    ]

    batch = TOTAL_REQUESTS // CONCURRENT_WORKERS
    tasks = []
    results = {"success": [0] * CONCURRENT_WORKERS, "start": time.perf_counter()}

    conn = aiohttp.TCPConnector(limit=CONCURRENT_WORKERS)
    async with aiohttp.ClientSession(args.rpc, connector=conn) as session:
        for worker_id in range(CONCURRENT_WORKERS):
            start = worker_id * batch
            end = TOTAL_REQUESTS if worker_id == CONCURRENT_WORKERS - 1 else start + batch
            slice_receivers = [
                (i, receivers[i % RECEIVER_COUNT][1])
                for i in range(start, end)
            ]
            tasks.append(asyncio.create_task(worker(worker_id, session, slice_receivers, results)))
        await asyncio.gather(*tasks)

    total_success = sum(results["success"])
    elapsed = time.perf_counter() - results["start"]
    print(f"sent {TOTAL_REQUESTS} requests in {elapsed:.2f}s")
    print(f"accepted {total_success} ({total_success / TOTAL_REQUESTS:.2%})")

if __name__ == "__main__":
    asyncio.run(main())
```

Run it:

```bash
python3 scripts/fire_60k.py --receiver-prefix "$RECEIVER_PREFIX" --owner "$OWNER_ACCOUNT" --rpc http://127.0.0.1:18082
```

- Concurrency (200 workers) and per-request payload match the integration test. Adjust `CONCURRENT_WORKERS` or `TOTAL_REQUESTS` inside the script if needed.
- Watch the relay logs; you should see batches of up to 90 `ft_transfer` actions being submitted.

Record the wall-clock duration and throughput (`TOTAL_REQUESTS / elapsed_seconds`). The target is ≥100 req/s.

---

## 4. Verify – On-Chain Balances & Reporting

### 4.1 Poll Receiver Balances

Use this Python helper (standard library only) to replicate the polling loop from the test. It queries the RPC directly, polls every 3 seconds, and stops after 60 iterations (~3 minutes) or once the expected total is reached.

```bash
python3 - <<'PY'
import base64
import json
import os
import time
import urllib.request

OWNER = os.environ["OWNER_ACCOUNT"]
TOKEN = os.environ["TOKEN_ACCOUNT"]
PREFIX = os.environ["RECEIVER_PREFIX"]
COUNT = int(os.environ.get("RECEIVER_COUNT", 20))
RPC_URL = os.environ.get("RPC_URL", "https://rpc.testnet.near.org")
TOTAL_REQUESTS = 60000
EXPECTED_TOTAL = TOTAL_REQUESTS * 10**18

def view_balance(account_id: str) -> int:
    args = base64.b64encode(json.dumps({"account_id": account_id}).encode()).decode()
    payload = {
        "jsonrpc": "2.0",
        "id": "ft-balance",
        "method": "query",
        "params": {
            "request_type": "call_function",
            "account_id": TOKEN,
            "method_name": "ft_balance_of",
            "args_base64": args
        }
    }
    req = urllib.request.Request(
        RPC_URL,
        data=json.dumps(payload).encode(),
        headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(req) as resp:
        body = json.loads(resp.read())
    raw = body["result"]["result"]
    data = base64.b64decode(bytes(raw))
    return int(data.decode())

poll_interval = 3
for poll in range(1, 61):
    total = 0
    for idx in range(COUNT):
        receiver = f"{PREFIX}{idx:02d}.{OWNER}"
        total += view_balance(receiver)
    pct = total / EXPECTED_TOTAL * 100
    print(f"poll {poll:02d}: total={total / 10**18:.0f} tokens ({pct:.2f}% done)")
    if total >= EXPECTED_TOTAL:
        print("target reached")
        break
    time.sleep(poll_interval)
else:
    print("max polls reached; final total shown above")
PY
```

- Expect the total balance to reach `60_000` tokens (if each transfer was 1 token) within a couple of polls.
- A success rate ≥80% mirrors the assertion in the integration test.

### 4.2 Collect Evidence

Capture:
- Relay logs showing throughput and transaction IDs.
- Python load-generator output (duration, acceptance rate).
- Balance poll results confirming final totals.

Optional: use `near tx-status <hash>` for a few sample transaction hashes printed by the relay to inspect gas usage and receipts.

---

## 5. Cleanup (Optional)

- Leave the FT contract state intact if you want to re-run later. The receivers now hold the tokens.
- To recycle the environment, either burn tokens via `ft_burn` or redeploy the contract with a fresh `new` call.
- Remove the temporary signer credentials if you no longer need them: `rm ~/.near-credentials/testnet/relay-signer-*.json` and revoke the keys via `near delete-key`.

---

## 6. Troubleshooting

- **HTTP 503 from relay** – queue is saturated; raise `MAX_INFLIGHT_BATCHES` or retry later.
- **Nonce or invalid signature errors** – ensure the secret keys in `.env` match the keys you added and that no other process is using them.
- **Storage errors** – double-check that each receiver called `storage_deposit` before load.
- **Slow finality** – polling window can be extended by increasing `max_polls` or spacing out batches.

With this checklist you should be able to reproduce the bounty-scale benchmark against testnet with the same semantics as the sandbox integration test.
