"""
AQAP 配置热加载 — watchdog 文件变化检测

监视 config.yaml 变更，无需重启即可应用新配置。
"""

import asyncio
import logging
import os
from pathlib import Path
from typing import Any, Callable

logger = logging.getLogger("aqap.config.hotreload")


class ConfigWatcher:
    """配置文件监视器

    使用 polling 方式检测文件变更 (mtime 追踪)。
    跨平台兼容 (Windows / macOS / Linux)。
    """

    def __init__(
        self,
        config_path: Path,
        callback: Callable[[dict], Any],
        poll_interval: float = 5.0,
    ):
        self._path = config_path.resolve()
        self._callback = callback
        self._poll_interval = poll_interval
        self._last_mtime: float = 0.0
        self._running = False
        self._task: asyncio.Task | None = None

    async def start(self) -> None:
        """启动监视"""
        if not self._path.exists():
            logger.warning("[config-watch] 配置文件不存在: %s", self._path)
            return
        self._last_mtime = self._path.stat().st_mtime
        self._running = True
        self._task = asyncio.create_task(self._watch_loop())
        logger.info("[config-watch] 开始监视 %s (每 %.1fs)", self._path, self._poll_interval)

    async def stop(self) -> None:
        """停止监视"""
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass

    async def _watch_loop(self) -> None:
        """轮询 loop"""
        while self._running:
            try:
                await asyncio.sleep(self._poll_interval)
                if not self._path.exists():
                    continue
                current_mtime = self._path.stat().st_mtime
                if current_mtime > self._last_mtime:
                    logger.info("[config-watch] 检测到 config.yaml 变更, 重新加载")
                    import yaml

                    new_cfg = yaml.safe_load(self._path.read_text(encoding="utf-8"))
                    if new_cfg:
                        try:
                            await self._callback(new_cfg)
                        except Exception as e:
                            logger.error("[config-watch] 回调失败: %s", e)
                    self._last_mtime = current_mtime
            except asyncio.CancelledError:
                break
            except OSError as e:
                logger.warning("[config-watch] 读取错误: %s", e)


class EngineHotReload:
    """Engine 热加载适配器

    将 config.yaml 变更映射到 Engine 操作:
      - 插件配置变更 → 重启受影响的 Agent
      - 日志级别变更 → 无需重启 (logging.dictConfig 即时生效)
      - transport 变更 → 需要手动重启
    """

    def __init__(self, engine):
        self._engine = engine
        self._watcher: ConfigWatcher | None = None

    async def start(self, poll_interval: float = 5.0) -> None:
        config_path = Path("config.yaml")
        self._watcher = ConfigWatcher(
            config_path=config_path,
            callback=self._on_config_changed,
            poll_interval=poll_interval,
        )
        await self._watcher.start()

    async def stop(self) -> None:
        if self._watcher:
            await self._watcher.stop()

    async def _on_config_changed(self, new_config: dict) -> None:
        """配置变更回调"""
        import logging.config

        from aqap.core.log_config import setup_logging

        # 1. 日志级别变更 — 即时生效
        logging_cfg = new_config.get("logging", {})
        if logging_cfg:
            logger.info("[hot-reload] 应用日志配置...")
            debug = new_config.get("app", {}).get("debug", False)
            setup_logging(debug=debug, config=logging_cfg)

        # 2. 插件配置变更 — 重新初始化插件
        plugin_configs = new_config.get("plugins", {})
        if plugin_configs:
            logger.info("[hot-reload] 应用插件配置变更...")
            for name, cfg in plugin_configs.items():
                plugin = self._engine._supervisor._agents.get(name)
                if plugin and hasattr(plugin, "initialize"):
                    await plugin.initialize(cfg.get("config", {}))
            logger.info("[hot-reload] 插件配置已刷新")

        # 3. Agent heartbeat 参数变更 — 无需完整重启
        for agent_id, cfg in new_config.get("agents", {}).items():
            agent = self._engine.supervisor._agents.get(agent_id)
            if agent:
                new_interval = cfg.get("heartbeat_interval")
                if new_interval and new_interval != agent._heartbeat_interval:
                    agent._heartbeat_interval = new_interval
                    logger.info("[hot-reload] %s heartbeat_interval: 已更新", agent_id)

        logger.info("[hot-reload] 配置热加载完成")
