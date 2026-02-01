# StandX 做市商机器人 (StandX Market Maker Bot)

一个专为 **StandX 永续合约交易所 (Perpetual Exchange)** 设计的高性能、基于 WebSocket 的做市商机器人。
核心逻辑采用 Python `asyncio` 编写，确保低延迟的订单执行和实时的状态管理。

## 🚀 主要功能

- **网格策略 (Grid Strategy)**: 自动在当前市场价格上下方挂出买单和卖单，通过捕捉点差获利。
- **WebSocket 实时流**: 直接对接 StandX 的 `Market` (行情) 和 `Trading` (交易) WebSocket 频道，响应速度极快。
- **强大的认证系统**: 完美支持 Ed25519 签名认证，内置对 Base58/Base64 密钥格式的智能兼容处理。
- **延迟监控**: 提供专属工具测试服务器到交易所的连接质量。
- **安全看门狗 (Safety Watchdogs)**:
    - **系统看门狗**: 检测数据流是否冻结，防止行情中断。
    - **持仓监控 (`start_monitor.py`)**: 独立进程运行，当持仓时间过长（如 > 30秒）无法平仓时，通过 **声音 + Telegram** 报警，防止网络卡死导致的亏损。
- **Telegram 集成**: 关键报警和错误信息直接推送到您的手机。

## 📂 项目结构

- `main.py`: 交易机器人的主入口。
- `strategy/`: 包含网格交易的核心逻辑 (`StandXMarketMaker`)。
- `protocol/`: 处理底层通信 (`ws_client.py`) 和 签名认证 (`auth.py`)。
- `check_latency.py`: 用于测试服务器网络延迟的工具。
- `start_monitor.py`: 独立的安全监控报警程序。
- `debug_key.py`: 帮助您验证 API Key 格式的实用小工具。

## 🛠️ 安装指南

1.  **克隆代码库**:
    ```bash
    git clone https://github.com/ziayuan/StandXMM-WS.git
    cd StandXMM-WS
    ```

2.  **配置 Python 环境**:
    ```bash
    python3 -m venv venv
    source venv/bin/activate
    pip install -r requirements.txt
    ```

3.  **配置密钥**:
    复制配置模板：
    ```bash
    cp .env.example .env
    ```
    编辑 `.env` 文件，填入您的 StandX API Key (Token)、API Secret (私钥) 以及 Telegram 配置。

4.  **调整策略参数**:
    编辑 `config.yaml`，设置您想要交易的币种 (`symbol`)、下单数量 (`quantity`) 以及网格参数（点差 `spread`、步长 `step`）。

## 🏃‍♂️ 运行指南

### 1. 启动交易机器人
**推荐方式 (后台运行)**：
使用我们的新脚本，即使 SSH 断开，机器人也会在后台持续运行。
```bash
./run.sh
```
- 查看日志：`tail -f bot.log`
- 停止机器人：`pkill -f main.py`

### 2. 参数配置
在 `config.yaml` 中，您可以设置：
- `grid.fill_cooldown_minutes`: 成交后暂停时间（默认 10 分钟）。
- `system_watchdog`: 报警系统的超时和通知开关。

### 3. 查看状态
机器人运行时，在 Telegram 中发送 `/status` 可查看：
-当前价格与持仓
- **账户余额**（新增支持）
- 挂单详情

### 2. 运行网络测试 (推荐在服务器部署前运行)
检查您的服务器与 StandX 的连接稳定性。
```bash
python check_latency.py
```

### 3. 启动安全监控 (强烈推荐)
建议在一个独立的终端或 screen 会话中运行此程序。它相当于一个“死人开关”警报器。
```bash
python start_monitor.py
```


## ☁️ 服务器部署指南 (更新代码)

如果您已经在服务器上运行了旧版本，请按以下步骤更新：

1.  **拉取最新代码**:
    ```bash
    cd StandXMM-WS
    git pull
    ```

2.  **更新配置文件**:
    由于 `config.yaml` 已经被修改（包含新参数），您可能需要手动检查并合并配置：
    ```bash
    nano config.yaml
    # 确保添加了 fill_cooldown_minutes: 30 等新参数
    ```

3.  **重启机器人**:
    ```bash
    # 先停止旧进程
    pkill -f main.py
    
    # 启动新版（后台运行）
    ./run.sh
    ```

## ⚠️ 免责声明

本软件仅供教育和研究目的使用。做市交易涉及重大风险。
**使用本软件所产生的任何盈亏由用户自行承担。** 作者不对任何资金损失负责。
