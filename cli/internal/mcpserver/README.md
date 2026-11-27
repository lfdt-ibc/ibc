<!-- SPDX-License-Identifier: Apache-2.0 -->

# IBC MCP server

`ibc relayer run` serves IBC CLI operations as
[Model Context Protocol](https://modelcontextprotocol.io) tools when `ibc.yml` sets
`mcp.listenAddr`, so an AI agent can send IFT tokens and follow the packets through that relayer.
The server runs in the relayer's process, on its own port, and calls the relayer directly. It is
an API over what that relayer already does. It does not mint and it does not cap amounts: mint
stays on `ibc tx ift mint`, and a send is whatever that client's signer can already submit. A
send uses the signer already configured on that client in `ibc.yml`.

```yaml
mcp:
  listenAddr: 127.0.0.1:3001
  auditLog: ~/.ibc/mcp-audit.jsonl # optional; empty uses mcp-audit.jsonl in the IBC home
  tokenFile: ~/.ibc/mcp-token # optional; empty uses mcp-token in the IBC home
  tokens: # optional; IFT tokens ibc_get_context lists, with symbol and decimals
    - chainId: "41001"
      ift: "0x5FbDB2315678afecb367f032d93F642f64180aa3"
```

## Tools

| Tool | What it does | Backed by |
| --- | --- | --- |
| `ibc_get_context` | This relayer's chains, connections (client IDs and signers), configured IFT tokens with symbol and decimals, whether packet queries work, and whether this client can send (`can_send`) | `ibc.yml`, IFT contract, `Packets` |
| `ibc_ift_balance` | IFT balance of an address or signer alias, with symbol and decimals | IFT contract |
| `ibc_ift_send` | Send IFT over a client with that client's signer. Waits until the user accepts or declines, then returns once the tx is broadcast; mining (up to 5 minutes) and the relay request continue in the background. `from` defaults to the client's signer. `packet_timeout_seconds` is the packet lifetime (default 900) | IFT `iftTransfer`, `Relay` |
| `ibc_list_sends` | This run's latest sends (up to 100), newest first, with tx, status (`submitting`, `broadcast`, `mined`, `failed`) and next step | memory |
| `ibc_relay_tx` | Ask the relayer to deliver a tx's packets on a chain it connects; idempotent | `Relay` |
| `ibc_get_packets` | One page of packet statuses with send/recv/ack/timeout hashes | `Packets` |
| `ibc_wait_for_packets` | Wait until a tx's packets are final (or received), with progress notifications. Returns early when another call is needed: a `not_selected` packet, or a send of this server that failed or needs `ibc_relay_tx`. Default 45 seconds, maximum 1800 | `Packets`, polled |

`ift` may be omitted on a chain with exactly one token in `mcp.tokens`.

Amounts are decimal strings in the token's base unit. `ibc_ift_balance` also returns
`balance_display`, and each send `amount_display`, in whole tokens with the symbol (for example
`1.5 USDC`), so the agent can check its unit conversion.

Results of `ibc_ift_send`, `ibc_list_sends`, `ibc_relay_tx` and `ibc_wait_for_packets` carry
`next`: `{"tool", "arguments", "reason"}`. The agent calls `tool` with `arguments`; no `tool`
means nothing is left to call.

The resource template `ibc://packets/{chain_id}/{tx_hash}` returns every packet of that transaction as JSON. `ibc_get_packets` returns one page.

Failures come back as error tool results whose text is JSON:
`{"code", "message", "retryable", "tx"?}`. Each `message` ends with what the agent should do
next. A write that reached the chain always carries its `tx`.

| Code | Retryable | Meaning | What to do |
| --- | --- | --- | --- |
| `INVALID_ARGUMENT` | No | A bad argument | Fix the argument |
| `POLICY_DENIED` | No | Client, signer or chain not on this relayer; client without elicitation; approval that does not match, expired or was already used | Follow the message. A mismatch or an expiry was not spent: call again. A used approval may already be broadcast: do not send again |
| `USER_DECLINED` | No | The user declined. Nothing was sent | Retry only if the user asks |
| `CONFIG_ERROR` | No | `ibc.yml`, a backend or the audit log is misconfigured | Tell the operator |
| `CHAIN_RPC_ERROR` | Yes | The chain did not answer. Nothing was broadcast | Retry |
| `TX_REVERTED` | No | With `tx`: mined and reverted, no tokens moved. Without `tx`: reverts in simulation (for example, the signer's balance is too low) and nothing was broadcast | Check the balance. Retrying fails until the cause changes |
| `TX_NOT_CONFIRMED` | No | Broadcast but not mined in time. It may still mine | Follow that hash with `ibc_relay_tx` or `ibc_get_packets`. Do not sign again |
| `RELAYER_UNAVAILABLE` | Yes | The relayer did not answer, the server is stopping, or a wait was cancelled | Retry. After a cancelled wait, call `ibc_wait_for_packets` again, not `ibc_ift_send` |
| `RELAYER_ERROR` | No | The relayer failed the request | Follow the message |
| `PACKETS_NOT_FOUND` | Yes | A relay found no packets in the tx yet, for example before the node serves a just-mined receipt | Retry `ibc_relay_tx` |

A send that fails after `ibc_ift_send` returned shows its error in `ibc_list_sends`, and
`ibc_wait_for_packets` returns it while the transfer has no packets. `TX_NOT_CONFIRMED` is the
exception: that tx may mine later, so `next` asks for `ibc_relay_tx` instead. Once
`ibc_relay_tx` or `ibc_wait_for_packets` sees its packets, the send is listed as `mined` with a
warning, and the audit log gets a `mined` line under its `id`.

A wait's `next` keeps its `until`, so a follow-up wait has the same goal.

If a `TX_NOT_CONFIRMED` transfer is still pending after the relayer's pending-transaction
timeout, the relayer re-signs it at the same nonce with higher fees, so a different hash can be
the one that is mined. The server keeps track: the audit log gets a `replaced` line under the
send's `id` with the new hash, a replay of the spent approval returns the new hash, and
`ibc_get_packets`, `ibc_relay_tx`, `ibc_wait_for_packets` and the packets resource accept any
hash of that transfer and use the one that carries its packets, with a warning that names it. The
relayer also logs `Stalled confirmed transaction replaced with higher fees` with both hashes. If
the transfer would now revert, it is not re-signed: the relayer's next transaction takes that
nonce, so the transfer never mines, and the relayer logs `Stalled confirmed transaction now
reverts`.

## Running it

```text
$ ibc relayer run
level=INFO msg="MCP server listening" module=mcp url=http://127.0.0.1:3001 tokenFile=/home/ibc/.ibc/mcp-token
level=INFO msg="MCP audit" module=mcp tool=ibc_ift_send outcome=mined chain=41001 code="" amount=100 client=base-0 tx=0xabc
level=INFO msg=Readiness module=bootstrap readiness="{Event:ready ChainsConnected:[41001 41002] HTTP:0.0.0.0:3000}"
```

Those lines are the process log, written to stderr. A container runtime collects that stream (`docker logs`, `kubectl logs`). Startup, a failure to start, shutdown, and each audit event are on it. Panics recovered by the HTTP server and SDK session logs are on it as well. The audit file stays in the IBC home and is not part of that stream.

Every line the MCP server writes carries `module=mcp` (`"module":"mcp"` with `--log-json`), so
filter the relayer's log on it:

```bash
ibc relayer run 2>&1 | grep 'module=mcp'
ibc relayer run --log-json 2>&1 | jq -c 'select(.module == "mcp")'
```

Lines that the relayer itself writes while serving an MCP call, such as its transaction
submitter re-signing a stalled send, carry the relayer's module instead.

`--mcp-listen 127.0.0.1:3001` overrides `mcp.listenAddr` for that process.

Every request must send `Authorization: Bearer <token>`, where the token is the contents of
`mcp.tokenFile` (default `mcp-token` in the IBC home). A missing file is created at startup with a
random token and mode `0600`, and the same token is used on every restart. To rotate it, delete
the file and restart. A request without the token gets HTTP 401.

Point the MCP client at that URL with the token:

```bash
# Claude Code
claude mcp add --transport http ibc http://127.0.0.1:3001 \
  --header "Authorization: Bearer $(cat ~/.ibc/mcp-token)"
```

The MCP server starts and stops with the relayer. A taken port stops the relayer from starting.
Without `mcp.listenAddr` and without `--mcp-listen`, the relayer runs exactly as before. The
address must be loopback (`127.0.0.1`, `::1`, or `localhost`) as well, because the endpoint can
sign with the relayer's keys.

## What a caller may do

Only a caller with the bearer token can call a tool. Loopback alone is not enough: other local
users, and other containers in the same pod, share it. The approval below is answered by the
connected client, so it shows the user each transfer but does not stop a caller that holds the
token.

A send is allowed when `(chain_id, client_id)` is one end of a relayer connection and `from`, if
given, is the signer configured on that end. Any IFT on that chain can be sent, to any receiver. The user
approves the exact transfer through MCP elicitation before anything is signed. The call does not
return until the user accepts or declines. A client without elicitation is refused with
`POLICY_DENIED`, and `ibc_get_context` reports `can_send: false` for it. The approval is bound to the chain, token, signer, receiver, amount, client,
timeout and relay mode by a single-use HMAC token, so it cannot be replayed or applied to a
different write. The token is redeemed before broadcast. A mismatch or an expiry was not spent;
call the tool again. A token that was already used means the transfer may already be broadcast.
It is reported as used for 24 hours after it expires. After that, or for a token issued before
the relayer restarted, the server cannot tell whether it was used, and the error says to ask the
user.
If the error includes `tx`, call `ibc_get_packets` with that hash as `source_tx_hash` and do not
call `ibc_ift_send` again. If no hash was returned, ask the user and do not call `ibc_ift_send`.
If a send call is cancelled or the client times out, find the send with `ibc_list_sends` and do
not call `ibc_ift_send` again.
The signer is the
one the relayer already submits with for that client, including a remote signer.

`ibc_relay_tx` does not sign. It is allowed when `chain_id` is on a relayer connection, because
the relayer spends its own gas to deliver. Reads stay open on every chain in `ibc.yml`.

Every `ibc_ift_send` and `ibc_relay_tx` call is appended to the audit log before it is submitted,
and a second line records how it ended (`mined`, `not_confirmed`, `reverted`, `relayed`, or
`failed`). A send the relayer later re-signs gets a `replaced` line with the new hash. A
`not_confirmed` send whose packets are later seen gets a `mined` line. The relay that `ibc_ift_send` requests after mining belongs to that send: it has no
line of its own, and the send's `mined` line does not record whether the relay request
succeeded. The send's warnings in `ibc_list_sends` carry that. A call refused before submission is one
line: `denied` for a policy refusal, `declined` when the user declines, and `failed` for an
invalid argument or a configuration error. The same line is written to the
process log with the tool, outcome, chain, and code. If the first line cannot be synced,
the call does not proceed. Reads are not logged. The file is owner-readable only and remains
the durable record.

## Resource impact

Without `mcp.listenAddr` and without `--mcp-listen`, the server is not created and costs nothing
at run time. The figures below were measured with 1M packets in the relayer's database.

**Memory.** An idle server holds about 0.4 MiB of heap and one goroutine. Each connected
session adds about 56 KiB. A session that stays idle for 30 minutes is closed. Retained state
is small:

- `ibc_list_sends`: at most 100 records
- spent approvals: kept for 24 hours after they expire
- token metadata: one entry per token read
- re-signed hashes: one group per re-signed send

The SDK caps request bodies.

**CPU.**

- Each send runs one background goroutine for up to 5 minutes while the tx mines, then
  requests the relay.
- `ibc_get_context` reads each configured token's symbol and decimals once, then caches them.
- `ibc_wait_for_packets` runs one packet query every 5 seconds per open wait, for up to 30
  minutes. At most 16 waits poll at once. Further waits queue without querying until a slot
  frees. A wait that gets no slot within its `timeout_seconds` returns `timed_out` with `next`
  set to wait again. With a source chain and tx hash, the query uses the
  `(source_chain_id, source_tx_hash, packet_sequence_number)` index. It takes about 0.06 ms on
  Postgres and about 0.02 ms on SQLite, with 1M packets. On SQLite this holds without
  `ANALYZE` statistics, because the index also gives the sequence order the query asks for.
  At the cap, polling costs well under 1% of a core.

**Storage.** The server writes no database rows. Packets of a send are indexed by the relayer as
any other packet. The audit log grows by about 0.75 KiB per send (an intent line and an outcome
line) and about 0.45 KiB per relay request. The relayer does not rotate it and keeps it open
for the whole run. Rotate it with copy-and-truncate (`copytruncate` in logrotate), or move it
while the relayer is stopped. Each audit line is also written to the process log. The token
file is a few dozen bytes.

## Layout

- `server.go`: server setup, middleware and tool registration
- `context.go`: `ibc_get_context`
- `balance.go`: `ibc_ift_balance`, token metadata and amount formatting
- `iftsend.go`: `ibc_ift_send`, the send that continues after it returns, and its relay request
- `authorize.go`: the checks a send must pass (token, amount, client, signer, receiver), then
  the request for the user's approval
- `sends.go`: this run's sends and `ibc_list_sends`
- `packets.go`: `ibc_relay_tx`, `ibc_get_packets` and the packets resource
- `audit.go`: append-only JSONL audit log
- `wait.go`: `ibc_wait_for_packets` and progress
- `confirm.go`: the user's approval of a send and its single-use tokens
- `config.go`: amount parsing
- `relayer.go`: adapter over the relayer's `RelayerApiService` handler, called in-process
- `http.go`: streamable HTTP listener
- `token.go`: bearer token file and the check on every request
- `backend.go`: the chain and relayer interfaces. The EVM backend and the startup hook for
  `ibc relayer run` live in `cmd/ibc/mcp.go`. A send goes through the relayer's transaction
  submitter for that chain and signer. `ibc tx ift` keeps its own sign-and-mine path.
