# AGS PBX recording receiver

Collects call recordings from AGS resort PBXs (OpenVox IPPBX / Asterisk) onto one server.
Processing (transcription, AI) comes later in the AGS application; this only receives and stores.

```
resort PBX  /var/spool/asterisk/monitor/YYYY/MM/DD/*.wav
   └─ pbx-uploader/pbx_uploader.py   (Python 2.7, runs on the PBX, outbound HTTPS only)
        └─ POST https://<receiver>/v1/pbx/recordings
             └─ server/server.mjs    (Node 20+, no dependencies)
                  └─ DATA_DIR/recordings/<source>/YYYY/MM/DD/<id>.wav + DATA_DIR/index.jsonl
```

## Server

Env (see `server/.env.example`):

| var | meaning |
|---|---|
| `PBX_TOKENS` | `source-name:token,…` one long random token per PBX; the name tags its recordings |
| `ADMIN_TOKEN` | token for listing / downloading |
| `DATA_DIR` | persistent volume (default `/data` in Docker) |
| `PORT` | default 3000 |

Generate tokens: `node -e "console.log(require('crypto').randomBytes(32).toString('hex'))"`

Deploy: build `server/Dockerfile` (context `server/`), mount a persistent volume at `/data`,
put it behind HTTPS on a domain. Must be HTTPS: the PBX sends its token in every request.

API:

- `POST /v1/pbx/recordings`: body = raw audio, `Authorization: Bearer <PBX token>`,
  `Content-Type: audio/wav`, call details in `X-Pbx-*` headers. 201 stored, 409 already have it,
  401 bad token, 413 too large, 415 not audio, 422 checksum mismatch.
- `GET /v1/pbx/recordings?source=&since=<ISO>&limit=`: newest first, admin token.
- `GET /v1/pbx/recordings/<id>/audio`: the file, admin token.
- `GET /health`

Each stored record: `id, source, receivedAt, file, size, sha256, pbxPath, kind (exten|in|out|q|g),
from, to, startedAt (PBX local time, IST), uniqueid, durationSecs`.

## Uploader (on the PBX)

Copy `pbx-uploader/pbx_uploader.py` and `uploader.conf` (from `uploader.conf.example`) to
`/mnt/data/ags-uploader/` on the PBX (`/` is a small flash partition). Then:

```bash
cd /mnt/data/ags-uploader
python pbx_uploader.py --check     # lists what it would send, sends nothing
python pbx_uploader.py --once      # one real sweep
python pbx_uploader.py --daemon    # every INTERVAL seconds, forever
```

It skips calls still being written (2 min) and calls under 5 s, remembers what was sent in
`sent.txt`, retries failures next sweep, and never deletes anything on the PBX.
Start-on-boot setup depends on the PBX's init system: see the install notes once confirmed.
