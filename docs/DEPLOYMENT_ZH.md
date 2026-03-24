# TradingAgents-CN 中文部署说明

## 1. 环境准备

- Windows
- Python 3.10+
- Node.js / npm
- 可以访问你所配置的 LLM 服务

## 2. 安装依赖

```powershell
python -m venv venv
venv\Scripts\python.exe -m pip install -U pip
venv\Scripts\python.exe -m pip install -r requirements.txt

cd dashboard-ui
npm install
cd ..
```

## 3. 配置环境变量

至少需要配置一组 LLM 参数：

```powershell
setx TRADINGAGENTS_LLM_PROVIDER "ark"
setx TRADINGAGENTS_LLM_BASE_URL "https://ark.cn-beijing.volces.com/api/coding/v3"
setx TRADINGAGENTS_LLM_API_KEY "your-key"
setx TRADINGAGENTS_QUICK_LLM "doubao-seed-2.0-lite"
setx TRADINGAGENTS_DEEP_LLM "doubao-seed-2.0-pro"
```

如果要使用本地 embedding：

```powershell
setx TRADINGAGENTS_EMBEDDING_BASE_URL "http://127.0.0.1:11434/v1"
setx TRADINGAGENTS_EMBEDDING_MODEL "bge-m3:latest"
```

如果希望 A 股隔夜扫描启用 QVeris 补充数据源：

```powershell
setx QVERIS_API_KEYS "key1,key2,key3,key4"
```

## 4. 启动项目

双击根目录下的：

- `start_dashboard.bat`

默认会打开：

- `http://127.0.0.1:8000`

## 5. 停止项目

双击：

- `stop_dashboard.bat`

## 6. 说明

- 本项目基于原始 `TradingAgents` 二次开发
- 当前更适合本地单用户部署
- 运行数据、缓存和 API key 不建议提交到 GitHub
