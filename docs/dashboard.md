# TradingAgents Dashboard MVP

## What ships

- FastAPI backend in `dashboard_api/`
- React + Vite frontend in `dashboard-ui/`
- SQLite task storage in `dashboard_data/dashboard.db`
- File-based artifacts in `dashboard_data/tasks/<task_id>/`

## Local development

### One-click launcher

You can start the packaged dashboard with a double-clickable Windows launcher:

```bat
start_dashboard.bat
```

The launcher will:

- sync TradingAgents-related user environment variables into the process
- install frontend dependencies when `dashboard-ui/node_modules` is missing
- rebuild `dashboard-ui/dist` when the source files are newer than the last build
- start the FastAPI backend on `http://127.0.0.1:8000`
- open the dashboard in your default browser

Backend logs are written to:

- `dashboard_data/logs/dashboard_backend.stdout.log`
- `dashboard_data/logs/dashboard_backend.stderr.log`

To stop the background backend started by the launcher:

```bat
stop_dashboard.bat
```

### 1. Start the backend

```powershell
venv\Scripts\python.exe -m dashboard_api.app
```

The API will be available at `http://127.0.0.1:8000`.

### 2. Start the frontend

```powershell
cd dashboard-ui
npm install
npm run dev
```

The dashboard will be available at `http://127.0.0.1:5173`.

### 3. Build the frontend for backend serving

```powershell
cd dashboard-ui
npm install
npm run build
```

After the build succeeds, the backend will serve `dashboard-ui/dist` at `http://127.0.0.1:8000`.

## Task storage

Each task creates a directory under `dashboard_data/tasks/<task_id>/`:

- `task.json`
- `reports/market.md`
- `reports/sentiment.md`
- `reports/news.md`
- `reports/fundamentals.md`
- `reports/trader_plan.md`
- `reports/investment_plan.md`
- `final_decision.md`
- `final_state.json`
- `events.log`

## Tests

```powershell
venv\Scripts\python.exe -m unittest tests.test_dashboard_api
```
