# STAR LINK CODE HACK — Bot

Production-grade RuiJie captive-portal voucher scanner bot.

## Deploy

### Render (recommended for beginners)
1. Push this folder to GitHub
2. Create new Web Service on render.com
3. Build: `pip install -r requirements.txt`
4. Start: `python star.py`

### Railway
1. Push to GitHub
2. New Project → Deploy from GitHub
3. Auto-detected via `Procfile`

### Fly.io
```bash
fly launch
fly deploy