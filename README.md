# k8s-delete-interceptor
eventhub/
├── config/
│   └── config.yaml          # 全局配置文件
├── sqlevent/                # [模块] 慢SQL事件模块
│   ├── entry.go             # 模块入口 (暴露给 main.go 调用)
│   └── internal/            # [私有] 内部实现，外部无法直接引用
│       ├── api/             # Gin Handler
│       ├── model/           # 数据模型
│       ├── processor/       # 核心处理 (Channel, Worker)
│       ├── repo/            # 数据库操作
│       ├── notifier/        # Telegram 通知
│       └── service/         # 业务逻辑串联
├── pkg/                     # [公共] 通用工具
│   └── utils/               # Hash, Time 等工具
├── go.mod
└── main.go                  # 主程序入口
