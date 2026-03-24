# Deployment Guide

This repository is prepared for source-based deployment on Windows.

## Requirements

- Python 3.10+
- Node.js 18+
- Access to your configured LLM provider

Optional:

- Local embedding service such as Ollama
- QVeris keys for stronger A-share overnight scanning

## Quick Start

```powershell
git clone <your-github-repo-url>
cd TradingAgents-CN

python -m venv venv
venv\Scripts\python.exe -m pip install -U pip
venv\Scripts\python.exe -m pip install -r requirements.txt

cd dashboard-ui
npm install
cd ..
```

Set the required environment variables for your LLM provider, then start:

```powershell
start_dashboard.bat
```

For a more detailed Chinese deployment guide, see `README_部署说明.md`.
