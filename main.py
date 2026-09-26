"""本地增量数据接入管线服务入口。

启动：python main.py  （或 uvicorn main:app）
配置：见 app/config.py 的环境变量说明。
"""

import uvicorn

from app.service import create_app

app = create_app()


@app.get("/")
def read_root():
    return {"service": "incremental-ingestion", "docs": "/docs"}


if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=8000)
