# STAR LINK CODE HACK — Ultimate Edition v4.0

Maximum speed • Maximum accuracy • Zero bugs • Premium UX

## New in v4.0

| Feature | Description |
|---------|-------------|
| **Session Pool** | Pre-solves captchas in background → workers grab ready pairs (big speed boost) |
| **Multi-pass OCR** | 4 preprocessing variants (Otsu, inverted, adaptive, morph) → higher captcha success |
| **Resume Scan** | Sequential modes auto-resume from last position after stop/restart |
| **ETA Display** | Shows estimated time remaining for sequential scans |
| **Smart Rate-limit** | Detects "request limited" and automatically backs off then recovers |
| **Success Alerts** | Instant notification + premium card when a code hits |
| **Live Admin Dashboard** | `/status` shows total checked, hits, rate-limits, avg speed, pool size |
| **Proxy Ready** | Just fill `PROXY_LIST` if you want rotation |
| **Clear Progress** | `/clearprogress` to reset resume point |

## Commands
- `/start` `/portal` `/scan` `/stop` `/result` `/recheck`
- `/key` `/genkey` `/delkey` `/listkeys` `/sendall`
- `/status` (admin) `/clearprogress`

## Deploy
Same as before (Render / Railway / Fly.io).
Build: `pip install -r requirements.txt`
Start: `python star.py`
