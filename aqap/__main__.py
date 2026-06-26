"""
AQAP 引擎 CLI 入口
"""
from aqap.core.engine import main

if __name__ == "__main__":
    import asyncio
    asyncio.run(main())
