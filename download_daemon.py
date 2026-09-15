"""
Daemon进程入口 - 独立下载守护进程

功能：
1. 命令行参数解析
2. 日志初始化
3. IPC通道建立
4. Daemon核心启动
5. Watchdog看门狗监控

用法：
  python download_daemon.py \\
    --session <path> \\
    --account-id <id> \\
    --ipc-socket <path> \\
    --log-level INFO \\
    --watchdog-timeout 60
"""

import asyncio
import argparse
import logging
import os
import signal
import sys
import time
from typing import Optional

logger = logging.getLogger(__name__)


# ==================== Terminal-only logging ====================
def _env_truthy(name: str) -> bool:
    val = os.getenv(name, "")
    return val.strip().lower() in ("1", "true", "yes", "on")


def _disable_file_logging_if_requested() -> None:
    """
    Disable file logging by monkey-patching logging.FileHandler when requested.
    This prevents any log file creation and keeps logs in terminal only.
    """
    # Preserve original FileHandler for recovery.
    if not hasattr(logging, "_ORIGINAL_FILE_HANDLER"):
        logging._ORIGINAL_FILE_HANDLER = logging.FileHandler

    # If explicitly requested to keep both, restore original and skip disable.
    if _env_truthy("DOWNLOAD_LOG_BOTH") or _env_truthy("LOG_BOTH"):
        try:
            logging.FileHandler = logging._ORIGINAL_FILE_HANDLER
        except Exception:
            pass
        return

    if not (
        _env_truthy("DAEMON_LOG_DISABLE_FILES")
        or _env_truthy("DOWNLOAD_LOG_DISABLE_FILES")
        or _env_truthy("DOWNLOAD_LOG_TERMINAL_ONLY")
    ):
        return

    class _NoFileHandler(logging.Handler):
        def __init__(self, *args, **kwargs):
            super().__init__()

        def emit(self, record):
            # Terminal-only mode: drop file output.
            return

    logging.FileHandler = _NoFileHandler  # type: ignore[assignment]
    try:
        sys.stdout.write("[Init] File logging disabled by env; terminal-only mode enabled\n")
        sys.stdout.flush()
    except Exception:
        pass


_disable_file_logging_if_requested()


# ==================== Console Encoding Fix ====================
def _ensure_utf8_stdio():
    """Best-effort reconfigure stdout/stderr to UTF-8 to avoid encode errors."""
    try:
        if hasattr(sys.stdout, "reconfigure"):
            sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        if hasattr(sys.stderr, "reconfigure"):
            sys.stderr.reconfigure(encoding="utf-8", errors="replace")
    except Exception as exc:
        # Avoid raising; keep daemon running even if reconfigure fails.
        try:
            logging.getLogger(__name__).warning(
                "[Logging] Failed to reconfigure stdio encoding: %s", exc
            )
        except Exception:
            pass


# ==================== Watchdog看门狗 ====================
class DaemonWatchdog:
    """
    看门狗：防止Daemon成为僵尸进程

    机制：
    - 监控与主进程的IPC连接
    - 连接断开超过N秒则自杀
    - 自杀前优雅清理资源
    """

    def __init__(
        self,
        ipc_channel: 'IPCChannel',
        daemon_core: Optional['DaemonCore'] = None,
        timeout: int = 60
    ):
        """
        初始化看门狗

        Args:
            ipc_channel: IPC通道对象
            daemon_core: Daemon核心对象（用于优雅关闭）
            timeout: 连接断开超过此秒数则自杀，默认60秒
        """
        self.ipc_channel = ipc_channel
        self.daemon_core = daemon_core
        self.timeout = timeout
        self.last_ping = time.time()
        self.dead = False
        self.check_interval = 5  # 每5秒检查一次

    async def monitor(self):
        """
        持续监控IPC连接状态

        流程：
        1. 每5秒检查一次IPC连接
        2. 如果连接正常，更新时间戳
        3. 如果连接断开超过timeout秒，执行自杀
        """
        logger.info(
            f"[Watchdog] 启动监控 (timeout={self.timeout}s, check_interval={self.check_interval}s)"
        )

        while not self.dead:
            try:
                # 检查IPC连接状态
                is_connected = self.ipc_channel.is_connected()

                if is_connected:
                    # 连接正常，更新时间戳
                    self.last_ping = time.time()
                    logger.debug("[Watchdog] IPC连接正常")

                else:
                    # 连接断开，计算断开时长
                    disconnected_time = time.time() - self.last_ping

                    logger.warning(
                        f"[Watchdog] IPC断连 ({disconnected_time:.1f}s)"
                    )

                    if disconnected_time > self.timeout:
                        logger.critical(
                            f"[Watchdog] IPC断连超过{self.timeout}秒，"
                            f"判断主进程已崩溃，执行自杀"
                        )
                        await self._graceful_suicide()
                        return

                # 每N秒检查一次
                await asyncio.sleep(self.check_interval)

            except Exception as e:
                logger.error(f"[Watchdog] 监控异常: {e}")
                await asyncio.sleep(self.check_interval)

    async def _graceful_suicide(self):
        """
        优雅自杀：清理资源后退出

        步骤：
        1. 保存所有检查点（如果支持）
        2. 断开Daemon的所有连接
        3. 清理临时文件（session副本）
        4. 退出进程
        """
        logger.info("[Watchdog] 开始优雅自杀...")

        try:
            # 步骤1：保存检查点（如果daemon_core支持）
            if self.daemon_core and hasattr(self.daemon_core, 'save_checkpoints'):
                try:
                    logger.info("[Watchdog] 保存所有检查点...")
                    await self.daemon_core.save_checkpoints()
                except Exception as e:
                    logger.error(f"[Watchdog] 保存检查点失败: {e}")

            # 步骤2：关闭Daemon核心
            if self.daemon_core and hasattr(self.daemon_core, 'shutdown'):
                try:
                    logger.info("[Watchdog] 关闭Daemon核心...")
                    await self.daemon_core.shutdown()
                except Exception as e:
                    logger.error(f"[Watchdog] 关闭Daemon失败: {e}")

            # 步骤3：关闭IPC连接
            try:
                logger.info("[Watchdog] 关闭IPC连接...")
                await self.ipc_channel.close()
            except Exception as e:
                logger.error(f"[Watchdog] 关闭IPC失败: {e}")

        except Exception as e:
            logger.error(f"[Watchdog] 自杀前清理异常: {e}")

        finally:
            # 步骤4：退出进程（无论如何）
            #
            # [FIX-2026-09-14-WATCHDOG-SIGKILL-WINDOWS] 之前这里用
            # `os.kill(os.getpid(), signal.SIGKILL)` 强制退出进程。SIGKILL
            # 是 POSIX 专属信号，Windows 的 signal 模块根本没有这个属性，
            # 调用会直接抛出 AttributeError:
            #   module 'signal' has no attribute 'SIGKILL'
            #
            # 由于这行代码在 finally 块内、且没有被 try/except 包裹，这个
            # AttributeError 会一路抛出 _graceful_suicide()，被 monitor()
            # 外层的 `except Exception` 捕获、记录为 "[Watchdog] 监控异常"
            # 后继续循环——但 self.dead 从未被设为 True，也从未真正调用
            # os._exit/sys.exit 退出进程，导致 monitor() 的 while 循环
            # 永远不会停止：每隔 check_interval 秒就重新判定"IPC断连超时"，
            # 再次尝试 SIGKILL 自杀，再次抛出同样的 AttributeError，如此
            # 无限循环——daemon 进程实际上变成了一个永远杀不死、每隔几秒
            # 打印同一组日志的僵尸进程，必须手动在任务管理器里结束进程。
            #
            # 改用 os._exit()：这是唯一在 POSIX 和 Windows 上都保证立即终止
            # 进程、且不能被任何异常处理器拦截或被 asyncio 取消的方式
            # （不同于 sys.exit()，后者只是抛出 SystemExit，可能只取消当前
            # 协程/任务而不会真正终止进程）。这里选用退出码 1
            # 表示"非正常路径下的自我终止"。此调用之前的所有清理步骤
            # （保存 checkpoint、关闭 daemon_core、关闭 IPC）均已在上面的
            # try 块中尽力完成，os._exit() 只是确保无论如何都不会卡在
            # 一个杀不掉自己的僵尸循环里。
            logger.critical("[Watchdog] Daemon进程即将退出")
            os._exit(1)

    def stop(self):
        """停止监控"""
        self.dead = True


# ==================== 信号处理 ====================
class SignalHandler:
    """处理系统信号"""

    def __init__(self, daemon_core: Optional['DaemonCore'] = None):
        self.daemon_core = daemon_core
        self.shutdown_requested = False

    def handle_sigterm(self, signum, frame):
        """处理SIGTERM：优雅关闭"""
        logger.info("[Signal] 收到SIGTERM，准备优雅关闭...")
        self.shutdown_requested = True
        if self.daemon_core:
            self.daemon_core.request_shutdown()

    def handle_sigint(self, signum, frame):
        """处理SIGINT(Ctrl+C)：立即关闭"""
        logger.warning("[Signal] 收到SIGINT(Ctrl+C)，立即关闭...")
        self.shutdown_requested = True
        if self.daemon_core:
            self.daemon_core.request_shutdown()


# ==================== 日志配置 ====================
def setup_logging(account_id: str, log_level: str, log_file_override: str = None):
    """
    设置daemon日志

    Args:
        account_id: 账号ID（用于日志文件名）
        log_level: 日志级别（DEBUG/INFO/WARNING/ERROR）
        log_file_override: 统一日志文件路径（与主进程共享），为 None 时使用独立文件
    """
    # 创建logs目录
    os.makedirs("logs", exist_ok=True)

    # 日志文件名
    if log_file_override:
        log_file = log_file_override
    else:
        log_file = f"logs/daemon_{account_id}_{os.getpid()}.log"

    # 日志格式（含 [DAEMON] 标签，便于与主进程日志区分）
    log_format = (
        "%(asctime)s.%(msecs)03d [DAEMON] %(name)s - %(levelname)s - %(message)s"
    )
    date_format = "%Y-%m-%d %H:%M:%S"

    # 配置root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(getattr(logging, log_level))

    # 文件处理器（追加模式，与主进程共享）
    file_handler = logging.FileHandler(log_file, encoding='utf-8')
    file_handler.setLevel(getattr(logging, log_level))
    file_handler.setFormatter(
        logging.Formatter(log_format, datefmt=date_format)
    )
    root_logger.addHandler(file_handler)

    # 控制台处理器
    # [FIX-2026-02-01] Ensure stdio can encode unicode logs on Windows GBK consoles.
    # This prevents UnicodeEncodeError when log lines contain non-ASCII symbols.
    _ensure_utf8_stdio()
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(getattr(logging, log_level))
    console_handler.setFormatter(
        logging.Formatter(log_format, datefmt=date_format)
    )
    root_logger.addHandler(console_handler)

    logger.info(f"[Init] 日志已初始化 -> {log_file}")


# ==================== 命令行参数解析 ====================
def parse_arguments():
    """解析命令行参数"""
    parser = argparse.ArgumentParser(
        description="Telegram下载器Daemon进程",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例：
  python download_daemon.py \\
    --session /path/to/session_daemon \\
    --account-id acc_123_456 \\
    --ipc-socket /tmp/tg_daemon.sock \\
    --log-level INFO \\
    --watchdog-timeout 60
        """
    )

    parser.add_argument(
        '--session',
        required=True,
        help='Session文件路径（主进程副本）'
    )

    parser.add_argument(
        '--account-id',
        required=True,
        help='Telegram账号ID'
    )

    parser.add_argument(
        '--ipc-socket',
        required=True,
        help='IPC socket路径（或TCP端口）'
    )

    parser.add_argument(
        '--log-level',
        default='INFO',
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
        help='日志级别（默认：INFO）'
    )

    parser.add_argument(
        '--watchdog-timeout',
        type=int,
        default=60,
        help='看门狗超时秒数（默认：60秒）'
    )

    parser.add_argument(
        '--log-file',
        default=None,
        help='统一日志文件路径（与主进程共享）'
    )

    parser.add_argument(
        '--api-id',
        type=int,
        required=True,
        help='Telegram API ID'
    )

    parser.add_argument(
        '--api-hash',
        required=True,
        help='Telegram API Hash'
    )

    return parser.parse_args()


# ==================== 主函数 ====================
async def daemon_main(
    session: str,
    account_id: str,
    ipc_socket: str,
    api_id: int,
    api_hash: str,
    log_level: str = "INFO",
    watchdog_timeout: int = 60,
    log_file: Optional[str] = None,
):
    """
    Daemon主函数

    流程：
    1. 初始化日志
    2. 导入依赖模块
    3. 建立IPC连接
    4. 启动Daemon核心
    5. 启动Watchdog监控
    6. 处理信号
    7. 等待关闭

    [FIX-2026-09-13-FROZEN-DAEMON-ENTRYPOINT] 此前该函数内部直接调用
    parse_arguments() 读取 sys.argv，只能通过命令行 `python download_daemon.py
    --session ... --account-id ...` 的方式启动。这在 PyInstaller 打包后的
    宿主应用中会失效：sys.executable 此时指向宿主自身编译出的 exe（而非通用
    Python 解释器），该 exe 有自己的 argparse 子命令体系，把
    download_daemon.py 的路径当成第一个位置参数传进去只会触发宿主 exe 自身
    的 "invalid choice" 用法错误，daemon 进程根本不会真正启动，IPC 端口上
    也就永远没有东西在监听（对应 [WinError 1225] The remote computer refused
    the network connection）。
    现在 daemon_main() 直接接受显式参数，不再耦合于 argparse/sys.argv，
    这样宿主应用可以在 frozen 模式下通过 multiprocessing.Process 直接调用
    run_daemon_process()（见下）以编程方式启动 daemon，完全绕开
    "用 sys.executable 执行一个 .py 脚本路径" 这个在 frozen 场景下站不住脚的
    假设。命令行调用方式（main() / parse_arguments()）保持完全不变，
    仅作为在此基础上的一层薄封装。
    """
    # 初始化日志
    setup_logging(account_id, log_level, log_file_override=log_file)

    logger.info("=" * 60)
    logger.info("Daemon进程启动")
    logger.info("=" * 60)
    logger.info(f"[Init] 账号ID: {account_id}")
    logger.info(f"[Init] Session路径: {session}")
    logger.info(f"[Init] IPC路径: {ipc_socket}")
    logger.info(f"[Init] Watchdog超时: {watchdog_timeout}秒")

    try:
        # 步骤1：导入依赖模块
        logger.info("[Init] 导入依赖模块...")
        from download_ipc import IPCChannel
        from download_event_bus import EventBus
        # download_daemon_core会在下个任务创建
        try:
            from download_daemon_core import DaemonCore
        except ImportError:
            logger.error("[Init] download_daemon_core.py尚未实现，使用stub")
            # 使用stub类进行测试
            class DaemonCore:
                def __init__(self, *args, **kwargs):
                    self.shutdown_requested = False

                async def run(self):
                    logger.info("[Stub] DaemonCore运行（stub模式）")
                    while not self.shutdown_requested:
                        await asyncio.sleep(1)

                async def shutdown(self):
                    logger.info("[Stub] DaemonCore关闭")

                async def save_checkpoints(self):
                    logger.info("[Stub] 保存检查点")

                def request_shutdown(self):
                    self.shutdown_requested = True

        # 步骤2：建立IPC连接
        logger.info("[Init] 建立IPC连接...")
        # 判断是否为socket路径或TCP端口
        if ipc_socket.isdigit():
            # TCP端口
            ipc = IPCChannel(tcp_port=int(ipc_socket))
        else:
            # Unix socket路径
            ipc = IPCChannel(socket_path=ipc_socket)

        # 连接为服务端（等待主进程客户端连接）
        await ipc.connect(is_server=True)
        logger.info("[Init] IPC服务端已启动，等待主进程连接...")

        # 步骤3：初始化EventBus
        event_bus = EventBus()
        logger.info("[Init] 事件总线已初始化")

        # 步骤4：初始化Daemon核心
        logger.info("[Init] 初始化Daemon核心...")
        daemon_core = DaemonCore(
            session_path=session,
            account_id=account_id,
            ipc_channel=ipc,
            event_bus=event_bus,
            api_id=api_id,
            api_hash=api_hash
        )

        # 步骤5：初始化看门狗
        watchdog = DaemonWatchdog(
            ipc_channel=ipc,
            daemon_core=daemon_core,
            timeout=watchdog_timeout
        )

        # 步骤6：设置信号处理器
        signal_handler = SignalHandler(daemon_core)
        signal.signal(signal.SIGTERM, signal_handler.handle_sigterm)
        signal.signal(signal.SIGINT, signal_handler.handle_sigint)

        # [FIX-2026-09-14-IGNORE-BROADCAST-CTRL-BREAK] 在 Windows 上，daemon
        # 是通过 multiprocessing.Process 由宿主应用（例如 frozen 模式下的
        # KKAFIO CLI）内部启动的。默认情况下，除非子进程显式使用了
        # CREATE_NEW_PROCESS_GROUP 单独建组，否则它会加入其父进程所在的
        # 控制台进程组——这意味着，如果宿主应用的 GUI 外壳（例如 MXU）为了
        # 实现"优雅停止"而向宿主进程广播 CTRL_BREAK_EVENT，这个信号事件也会
        # 一并广播到本 daemon 进程。
        #
        # 如果不特殊处理，daemon 在没有为 SIGBREAK 注册任何处理器的情况下，
        # 会被 Windows 的默认行为直接终止——不会运行任何 Python 清理代码
        # （不会断开 TelegramClient、不会保存 checkpoint、不会走
        # DaemonCore.shutdown()），效果等同于一次没有任何预警的强制杀死，
        # 这恰恰是我们花了大量精力才修好的"优雅关闭"流程本应避免的情况。
        #
        # 正确的关闭方式应该只有一条路径：宿主进程捕获到同一个
        # CTRL_BREAK_EVENT 后，通过已有的 IPC 通道发送正式的
        # SHUTDOWN_REQUEST（宿主侧的信号处理見 kkafio_cli.py 的
        # install_graceful_stop_handler()）——这样 daemon 才能走完整套已经
        # 验证过的优雅关闭流程（取消进行中的下载、保存 checkpoint、断开
        # TelegramClient、清理 DC 连接池），而不是被同一个广播信号原地打断。
        #
        # 因此这里显式忽略 SIGBREAK，让 daemon 只响应 IPC 层面的
        # SHUTDOWN_REQUEST（以及自身 Watchdog 在 IPC 意外断连时的兜底自杀，
        # 这套逻辑本身不依赖 SIGBREAK，不受影响）。
        if sys.platform == "win32" and hasattr(signal, "SIGBREAK"):
            signal.signal(signal.SIGBREAK, signal.SIG_IGN)

        logger.info("[Init] 信号处理器已注册")

        logger.info("=" * 60)
        logger.info("Daemon初始化完成，启动主循环")
        logger.info("=" * 60)

        # 步骤7：并发运行daemon核心和watchdog监控
        #
        # [FIX-2026-09-14-WATCHDOG-EARLY-STOP] 之前这里用
        # `asyncio.gather(daemon_core.run(), watchdog.monitor(),
        # return_exceptions=True)`。gather() 会等到*两个*任务都结束才返回
        # ——但 daemon_core.run() 在优雅关闭（收到 SHUTDOWN_REQUEST）时几乎
        # 立刻就会退出主循环返回，而 watchdog.monitor() 是一个完全独立的
        # `while not self.dead:` 循环，没有任何机制知道 daemon_core 已经
        # 关闭。self.dead 只会在 watchdog 自己判定"IPC断连超过60秒"、真正
        # 触发自杀时才会被设为 True。
        #
        # 结果：即便 daemon_core 一侧已经优雅关闭完毕，gather() 仍会继续
        # 等待 watchdog.monitor()，而 watchdog 会继续按 5 秒一次的频率轮询
        # 那个即将被关闭的 IPC 连接，直到最多 60 秒后才会自行判定超时、
        # 触发自杀退出——也就是说，一次"优雅关闭"实际上会让进程在后台
        # 多挂起长达 60 秒，才会真正退出。
        #
        # 现在改用 asyncio.wait(..., return_when=FIRST_COMPLETED)：
        # daemon_core.run() 和 watchdog.monitor() 任意一个先结束，就立刻
        # 取消另一个并继续走后面的关闭流程，不再等待 watchdog 自己按超时
        # 退出。
        tasks = [
            asyncio.create_task(daemon_core.run(), name="daemon_core.run"),
            asyncio.create_task(watchdog.monitor(), name="watchdog.monitor"),
        ]
        try:
            done, pending = await asyncio.wait(
                tasks, return_when=asyncio.FIRST_COMPLETED
            )

            # 确保 watchdog 内部状态也标记为已停止（即便它不是先结束的那个）
            watchdog.stop()

            for t in pending:
                t.cancel()
            if pending:
                # 等待被取消的任务真正退出，吞掉预期中的 CancelledError
                await asyncio.wait(pending)

            for t in done:
                exc = t.exception() if not t.cancelled() else None
                if exc:
                    logger.error(
                        f"[Error] 主循环异常 ({t.get_name()}): {exc}",
                        exc_info=exc,
                    )
        except Exception as e:
            logger.error(f"[Error] 主循环异常: {e}", exc_info=True)

        # 步骤8：优雅关闭
        logger.info("[Shutdown] 关闭Daemon...")
        await daemon_core.shutdown()
        await ipc.close()

        logger.info("=" * 60)
        logger.info("Daemon进程已关闭")
        logger.info("=" * 60)

    except Exception as e:
        logger.critical(f"[Fatal] Daemon崩溃: {e}", exc_info=True)
        raise

    finally:
        logger.info("[Cleanup] 清理完毕")


# ==================== 崩溃保护 ====================
def _install_crash_logger():
    """
    安装全局异常钩子，确保 Daemon 进程崩溃时将完整 traceback
    写入日志文件，而非默默消失（导致只能看到 IPC 断连的 WinError 64）。
    """
    _original_excepthook = sys.excepthook

    def _crash_hook(exc_type, exc_value, exc_tb):
        if exc_type is KeyboardInterrupt:
            _original_excepthook(exc_type, exc_value, exc_tb)
            return
        try:
            import traceback as _tb
            crash_msg = "".join(_tb.format_exception(exc_type, exc_value, exc_tb))
            logger.critical(f"[CRASH] Daemon进程未捕获异常:\n{crash_msg}")
            # 双保险：直接写 stderr（即使 logger 已失效）
            sys.stderr.write(f"[DAEMON-CRASH] {crash_msg}\n")
            sys.stderr.flush()
        except Exception:
            pass
        _original_excepthook(exc_type, exc_value, exc_tb)

    sys.excepthook = _crash_hook


# ==================== 进程入口（命令行方式，行为保持不变） ====================
def main():
    """进程入口点（命令行方式：`python download_daemon.py --session ... `）"""
    _install_crash_logger()
    try:
        # 解析命令行参数，转换为 daemon_main() 的显式参数
        args = parse_arguments()
        asyncio.run(daemon_main(
            session=args.session,
            account_id=args.account_id,
            ipc_socket=args.ipc_socket,
            api_id=args.api_id,
            api_hash=args.api_hash,
            log_level=args.log_level,
            watchdog_timeout=args.watchdog_timeout,
            log_file=args.log_file,
        ))
        sys.exit(0)

    except KeyboardInterrupt:
        logger.warning("[Shutdown] 用户中断")
        sys.exit(0)

    except SystemExit:
        # [FIX-2026-09-14-SPURIOUS-FATAL-ON-CLEAN-EXIT] 上面 sys.exit(0)
        # 自己触发的 SystemExit 会先经过这里——必须原样放行，否则会被下面
        # 的 `except BaseException` 当成"未处理的异常"捕获（SystemExit 本
        # 身就是 BaseException 的子类）。放行前不做任何事，这样无论
        # sys.exit() 是在这个 try 块的哪一处被调用的（正常完成后的
        # sys.exit(0)，还是别处传播上来的其他退出码），都能保留其原本的
        # 退出码，不会被这里错误地改写。
        raise

    except BaseException as e:
        # [FIX-2026-09-14-SPURIOUS-FATAL-ON-CLEAN-EXIT] 修复前，这里会捕获
        # 到上面 `sys.exit(0)` 自己抛出的 SystemExit(0)（因为没有先单独
        # 处理 SystemExit，而 SystemExit 正是 BaseException 的子类），
        # 导致每一次完全正常、优雅关闭的运行都会：
        #   1) 打印一条误导性的 "[Fatal] 未处理的异常: 0" CRITICAL 日志，
        #      看起来像是出错了，实际只是进程按预期退出；
        #   2) 用这里的 sys.exit(1) 覆盖掉原本的 sys.exit(0)，导致进程
        #      退出码永远是 1（失败），即使一切都成功完成——这会破坏任何
        #      依赖退出码判断成功/失败的上层逻辑。
        # 加上前面的 `except SystemExit: raise` 后，这里只会捕获真正意外
        # 的异常（例如 daemon_main() 内部未被捕获的其他 BaseException），
        # 行为符合这条日志本身的语义。
        logger.critical(f"[Fatal] 未处理的异常: {e}", exc_info=True)
        sys.exit(1)


# ==================== 进程入口（编程方式，供 multiprocessing.Process 使用） ====================
def run_daemon_process(
    session: str,
    account_id: str,
    ipc_socket: str,
    api_id: int,
    api_hash: str,
    log_level: str = "INFO",
    watchdog_timeout: int = 60,
    log_file: Optional[str] = None,
) -> None:
    """
    [FIX-2026-09-13-FROZEN-DAEMON-ENTRYPOINT] Daemon 的“编程方式”入口点。

    与 main() 唯一的区别是：不经过 argparse / sys.argv，而是直接接收显式
    参数。这是一个模块级、可被 pickle 的普通函数，因此可以直接作为
    `multiprocessing.Process(target=run_daemon_process, kwargs={...})` 的
    target 使用——这正是宿主应用在 PyInstaller frozen 模式下启动 daemon 所
    需要的方式：frozen 场景下 sys.executable 指向宿主自身编译出的 exe，而不
    是通用 Python 解释器，因此不能再用
    `subprocess.Popen([sys.executable, "download_daemon.py", ...])`
    这种“把脚本路径当命令行参数丢给解释器”的方式启动 daemon；而
    multiprocessing.Process 在 spawn 模式下会正确地重新执行宿主自身的 exe
    并在子进程中直接调用这里指定的 Python 函数，完全不依赖 argparse 或者
    “某个 .py 文件路径”这种在 frozen 环境下已经失效的假设。

    命令行方式（main()）行为不受任何影响，两者内部都只是薄封装，最终都调用
    同一个 daemon_main()。
    """
    _install_crash_logger()
    try:
        asyncio.run(daemon_main(
            session=session,
            account_id=account_id,
            ipc_socket=ipc_socket,
            api_id=api_id,
            api_hash=api_hash,
            log_level=log_level,
            watchdog_timeout=watchdog_timeout,
            log_file=log_file,
        ))
        sys.exit(0)

    except KeyboardInterrupt:
        logger.warning("[Shutdown] 用户中断")
        sys.exit(0)

    except SystemExit:
        # [FIX-2026-09-14-SPURIOUS-FATAL-ON-CLEAN-EXIT] 见 main() 中同名注释：
        # 必须原样放行 sys.exit(0) 自己触发的 SystemExit，否则会被下面的
        # `except BaseException` 误当成"未处理的异常"捕获并覆盖退出码。
        raise

    except BaseException as e:
        # [FIX-2026-09-14-SPURIOUS-FATAL-ON-CLEAN-EXIT] 见 main() 中同名注释。
        logger.critical(f"[Fatal] 未处理的异常: {e}", exc_info=True)
        sys.exit(1)


if __name__ == '__main__':
    main()