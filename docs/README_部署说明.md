# TradingAgents-CN 部署说明

这份说明面向第一次部署本项目的朋友，目标是用最少步骤把仪表盘跑起来。

## 1. 环境要求

- Windows 10/11
- Python 3.10 及以上
- Node.js 18 及以上
- 可访问你配置的 LLM 服务

可选：

- 本地 embedding 服务，例如 Ollama
- QVeris key，用于增强 A 股隔夜扫描的指数和实时现货补充数据

## 2. 获取代码

```powershell
git clone <你的 GitHub 仓库地址>
cd TradingAgents-CN
```

## 3. 安装依赖

```powershell
python -m venv venv
venv\Scripts\python.exe -m pip install -U pip
venv\Scripts\python.exe -m pip install -r requirements.txt

cd dashboard-ui
npm install
cd ..
```

## 4. 配置环境变量

项目启动时读取系统或用户环境变量，不会自动加载 `.env` 文件。

至少需要配置一组 LLM：

```powershell
setx TRADINGAGENTS_LLM_PROVIDER "ark"
setx TRADINGAGENTS_LLM_BASE_URL "https://ark.cn-beijing.volces.com/api/coding/v3"
setx TRADINGAGENTS_LLM_API_KEY "你的大模型 key"
setx TRADINGAGENTS_QUICK_LLM "doubao-seed-2.0-lite"
setx TRADINGAGENTS_DEEP_LLM "doubao-seed-2.0-pro"
```

如果你要启用本地 embedding：

```powershell
setx TRADINGAGENTS_EMBEDDING_BASE_URL "http://127.0.0.1:11434/v1"
setx TRADINGAGENTS_EMBEDDING_MODEL "bge-m3:latest"
```

如果你要启用 QVeris 补充数据源：

```powershell
setx QVERIS_API_KEYS "key1,key2,key3,key4"
```

环境变量设置完成后，请关闭并重新打开终端，或者直接重新双击启动器。

## 5. 启动项目

最简单的方式是双击：

- `start_dashboard.bat`

如果你希望手动启动：

```powershell
powershell -File .\scripts\start_dashboard.ps1
```

启动成功后，浏览器会打开：

- `http://127.0.0.1:8000`

## 6. 停止项目

双击：

- `stop_dashboard.bat`

或者手动运行：

```powershell
powershell -File .\scripts\stop_dashboard.ps1
```

## 7. 常见问题

### 首页提示 readiness 不通过

请优先检查：

- LLM key 是否已配置
- 本地 embedding 服务是否启动
- 网络是否能访问配置的 LLM 地址

### 隔夜推荐里提示无法获取市场快照

项目会优先使用 Akshare/Sina，并在可用时用 QVeris 做补充。
如果你希望增强 A 股隔夜扫描稳定性，建议额外配置 `QVERIS_API_KEYS`。

### 页面看起来像旧版本

请按一次 `Ctrl + F5` 强制刷新；如果仍不对，重新运行启动脚本让前端重新构建。

## 8. 不建议上传到公开仓库的内容

请不要把以下内容提交到 GitHub：

- `venv/`
- `dashboard-ui/node_modules/`
- `dashboard-ui/dist/`
- `dashboard_data/` 和其他本地运行数据目录
- API key 或本地私有配置

当前仓库已经通过 `.gitignore` 屏蔽了这些常见目录，但你在推送前仍然应该再执行一次：

```powershell
git status
```
