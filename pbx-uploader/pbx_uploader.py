#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
AGS PBX recording uploader.

Runs ON the customer's Asterisk PBX (OpenVox IPPBX / Issabel, CentOS 7, Python 2.7.5)
and pushes every finished call recording to the AGS PBX receiver (../server).
Outbound HTTPS only: nothing is opened on the resort's router.

  python pbx_uploader.py --once      one sweep, then exit (cron / manual test)
  python pbx_uploader.py --daemon    sweep every INTERVAL seconds, forever
  python pbx_uploader.py --check     print what would be sent, send nothing

Config: uploader.conf next to this file (KEY=VALUE lines), see uploader.conf.example.
Standard library only, works on Python 2.7.5 and 3.x.
"""
from __future__ import print_function

import fcntl
import hashlib
import os
import re
import sys
import time
import wave

try:  # Python 2
    import urllib2 as _req
    from urllib2 import HTTPError, URLError
except ImportError:  # Python 3
    import urllib.request as _req
    from urllib.error import HTTPError, URLError

import ssl

VERSION = '1.0.0'
HERE = os.path.dirname(os.path.abspath(__file__))

DEFAULTS = {
    'ENDPOINT': '',          # https://<receiver-domain>/v1/pbx/recordings
    'TOKEN': '',
    'MONITOR_DIR': '/var/spool/asterisk/monitor',
    'STATE_DIR': HERE,
    'INTERVAL': '120',      # seconds between sweeps in --daemon mode
    'SETTLE_SECS': '120',   # skip files modified this recently: the call may still be recording
    'MIN_SECS': '5',        # skip recordings shorter than this (missed / instantly dropped calls)
    'MAX_AGE_DAYS': '30',   # never send recordings older than this (first run on an old PBX)
    'MAX_BYTES': str(25 * 1024 * 1024),
    'CA_BUNDLE': '/etc/pki/tls/certs/ca-bundle.crt',
}

AUDIO_EXT = ('.wav', '.WAV', '.wav49', '.gsm', '.mp3')

# exten-9-1001-20261027-082316-1793069591.1102.wav
# in-<caller>-<did>-..., out-<dialled>-<ext>-..., q-<queue>-<caller>-..., g-<group>-<caller>-...
NAME_RE = re.compile(
    r'^(?P<kind>[A-Za-z]+)-(?P<a>[^-]*)-(?P<b>[^-]*)-(?P<date>\d{8})-(?P<time>\d{6})-(?P<uid>[0-9.]+)\.'
)


def load_config():
    cfg = dict(DEFAULTS)
    path = os.environ.get('SD_UPLOADER_CONF', os.path.join(HERE, 'uploader.conf'))
    if os.path.exists(path):
        with open(path) as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith('#') or '=' not in line:
                    continue
                k, v = line.split('=', 1)
                cfg[k.strip()] = v.strip().strip('"').strip("'")
    return cfg


def log(msg):
    print('%s %s' % (time.strftime('%Y-%m-%d %H:%M:%S'), msg))
    sys.stdout.flush()


class State(object):
    """Append-only ledger of files the server has confirmed. One relative path per line."""

    def __init__(self, state_dir):
        self.path = os.path.join(state_dir, 'sent.txt')
        self.done = set()
        if os.path.exists(self.path):
            with open(self.path) as f:
                self.done = set(l.rstrip('\n') for l in f if l.strip())

    def mark(self, rel):
        self.done.add(rel)
        with open(self.path, 'a') as f:
            f.write(rel + '\n')


def parse_name(name):
    m = NAME_RE.match(name)
    if not m:
        return {}
    d = m.groupdict()
    return {
        'kind': d['kind'].lower(),
        'from': d['a'],
        'to': d['b'],
        # PBX local time (IST), "YYYY-MM-DD HH:MM:SS"
        'started': '%s-%s-%s %s:%s:%s' % (d['date'][:4], d['date'][4:6], d['date'][6:],
                                           d['time'][:2], d['time'][2:4], d['time'][4:]),
        'uniqueid': d['uid'],
    }


def wav_seconds(path):
    try:
        w = wave.open(path, 'rb')
        try:
            rate = w.getframerate()
            return int(round(w.getnframes() / float(rate))) if rate else None
        finally:
            w.close()
    except Exception:
        return None  # not a PCM wav (gsm / mp3): let the server work it out


def sha256(path):
    h = hashlib.sha256()
    with open(path, 'rb') as f:
        for chunk in iter(lambda: f.read(65536), b''):
            h.update(chunk)
    return h.hexdigest()


def ssl_context(cfg):
    # RHEL/CentOS 7's Python 2.7.5 carries the PEP 466 backport (SNI + verification);
    # Cloudflare refuses clients without SNI, so this must be the real context.
    if not hasattr(ssl, 'create_default_context'):
        return None
    ca = cfg['CA_BUNDLE']
    return ssl.create_default_context(cafile=ca if os.path.exists(ca) else None)


def find_candidates(cfg, state):
    root = cfg['MONITOR_DIR']
    now = time.time()
    settle = int(cfg['SETTLE_SECS'])
    oldest = now - int(cfg['MAX_AGE_DAYS']) * 86400
    out = []
    for dirpath, _dirs, files in os.walk(root):
        for name in files:
            if not name.endswith(AUDIO_EXT):
                continue
            full = os.path.join(dirpath, name)
            rel = os.path.relpath(full, root)
            if rel in state.done:
                continue
            try:
                st = os.stat(full)
            except OSError:
                continue
            if st.st_mtime > now - settle or st.st_mtime < oldest or st.st_size == 0:
                continue
            out.append((st.st_mtime, full, rel, st.st_size))
    out.sort()
    return out


def upload(cfg, ctx, full, rel, size, meta):
    with open(full, 'rb') as f:
        body = f.read()
    headers = {
        'Authorization': 'Bearer ' + cfg['TOKEN'],
        # Cloudflare 403s the default "Python-urllib" agent.
        'User-Agent': 'AGS-PBX-Uploader/' + VERSION,
        'Content-Type': 'audio/mpeg' if rel.endswith('.mp3') else 'audio/wav',
        'X-Pbx-Path': rel.replace(os.sep, '/'),
        'X-Pbx-Sha256': meta['sha256'],
    }
    for k in ('kind', 'from', 'to', 'started', 'uniqueid'):
        if meta.get(k):
            headers['X-Pbx-' + k.capitalize()] = meta[k]
    if meta.get('duration') is not None:
        headers['X-Pbx-Duration'] = str(meta['duration'])
    req = _req.Request(cfg['ENDPOINT'], data=body, headers=headers)
    kwargs = {'timeout': 120}
    if ctx is not None:
        kwargs['context'] = ctx
    try:
        resp = _req.urlopen(req, **kwargs)
        return resp.getcode()
    except HTTPError as e:
        return e.code


def sweep(cfg, state, dry_run=False):
    ctx = ssl_context(cfg)
    min_secs = int(cfg['MIN_SECS'])
    max_bytes = int(cfg['MAX_BYTES'])
    sent = skipped = failed = 0
    for _mtime, full, rel, size in find_candidates(cfg, state):
        meta = parse_name(os.path.basename(full))
        meta['duration'] = wav_seconds(full)
        if meta['duration'] is not None and meta['duration'] < min_secs:
            if not dry_run:
                state.mark(rel)
            skipped += 1
            continue
        if size > max_bytes:
            log('skip (too large, %d bytes): %s' % (size, rel))
            if not dry_run:
                state.mark(rel)
            skipped += 1
            continue
        if dry_run:
            log('would send %s %s' % (rel, meta))
            continue
        meta['sha256'] = sha256(full)
        try:
            code = upload(cfg, ctx, full, rel, size, meta)
        except (URLError, IOError, OSError) as e:
            log('network error, will retry next sweep: %s' % (e,))
            failed += 1
            break  # internet is down: stop this sweep, keep order
        if code in (200, 201, 409):  # 409 = server already has it
            state.mark(rel)
            sent += 1
        elif code in (401, 403):
            log('server refused the token (HTTP %d). Check TOKEN in uploader.conf.' % code)
            failed += 1
            break
        elif code in (413, 415, 422):
            log('server rejected %s (HTTP %d), not retrying' % (rel, code))
            state.mark(rel)
            skipped += 1
        else:
            log('HTTP %d for %s, will retry' % (code, rel))
            failed += 1
    if sent or failed or skipped:
        log('sweep done: sent=%d skipped=%d failed=%d' % (sent, skipped, failed))


def main(argv):
    cfg = load_config()
    mode = argv[1] if len(argv) > 1 else '--once'
    if mode not in ('--once', '--daemon', '--check'):
        print(__doc__)
        return 2
    if not (cfg['TOKEN'] and cfg['ENDPOINT']) and mode != '--check':
        log('TOKEN or ENDPOINT is empty: set both in %s' % os.path.join(HERE, 'uploader.conf'))
        return 1
    if not os.path.isdir(cfg['STATE_DIR']):
        os.makedirs(cfg['STATE_DIR'])
    # One instance at a time (cron + daemon, or two cron runs, must not double-send).
    lock = open(os.path.join(cfg['STATE_DIR'], 'uploader.lock'), 'w')
    try:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except IOError:
        log('another uploader is already running')
        return 0
    state = State(cfg['STATE_DIR'])
    if mode == '--check':
        sweep(cfg, state, dry_run=True)
        return 0
    if mode == '--once':
        sweep(cfg, state)
        return 0
    log('uploader %s started, watching %s every %ss' % (VERSION, cfg['MONITOR_DIR'], cfg['INTERVAL']))
    while True:
        try:
            sweep(cfg, state)
        except Exception as e:  # never let one bad file kill the daemon
            log('sweep error: %r' % (e,))
        time.sleep(int(cfg['INTERVAL']))


if __name__ == '__main__':
    sys.exit(main(sys.argv))
