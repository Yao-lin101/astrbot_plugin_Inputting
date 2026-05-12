import asyncio
import time
from astrbot.api.event import filter, AstrMessageEvent, MessageEventResult, ResultContentType
from astrbot.api.star import Context, Star, register
from astrbot.api import logger
from astrbot.api.message_components import Plain

@register("astrbot_plugin_inputting", "e.e.", "消息自动合并插件：当用户正在输入或连续发送短句时进行拦截与打包，解决 LLM 响应碎片化问题。", "1.0.9")
class InputtingPlugin(Star):
    def __init__(self, context: Context, config: dict = None):
        super().__init__(context)
        self.config = config or {}
        # session_key -> {chain: [], last_event: event, timer: task, is_at_or_wake: bool, start_time: float}
        self.buffers = {} 
        
    async def initialize(self):
        self.bundle_threshold = self.config.get("bundle_threshold", 1.5)
        self.max_wait = self.config.get("max_wait", 20.0)
        
        logger.info("="*30)
        logger.info("InputtingPlugin (消息合并) 已启动")
        logger.info(f" - 当前合并阈值 (bundle_threshold): {self.bundle_threshold}s")
        logger.info(f" - 当前最大等待时间 (max_wait): {self.max_wait}s")
        logger.info("="*30)

    # 关键：设置 priority=100。
    # AstrBot 会按 priority 从大到小排序 Handler。
    # 设置高优先级确保在日报等其他插件之前拦截到碎片消息。
    @filter.event_message_type(filter.EventMessageType.ALL, priority=100)
    async def on_all_message(self, event: AstrMessageEvent):
        # 1. 检查是否已经打包过，防止循环处理
        if event.get_extra("bundled"):
            return
        
        # 2. 排除群聊消息
        if event.message_obj.group_id:
            return

        session_key = event.unified_msg_origin
        chain = event.get_messages()
        
        # 3. 检查是否为空消息（正在输入状态）
        is_empty = not chain or (len(chain) == 1 and isinstance(chain[0], Plain) and not chain[0].text.strip())
        
        if is_empty:
            if session_key in self.buffers:
                self._reset_timer(session_key)
                self._intercept_event(event)
            return

        # 4. 处理正常文本消息
        if session_key not in self.buffers:
            self.buffers[session_key] = {
                "chain": [],
                "last_event": None,
                "timer": None,
                "is_at_or_wake": False,
                "start_time": time.time()
            }
        
        buffer = self.buffers[session_key]
        
        if buffer["chain"]:
            buffer["chain"].append(Plain("\n"))
            
        buffer["chain"].extend(chain)
        buffer["last_event"] = event
        
        if getattr(event, "is_at_or_wake_command", False):
            buffer["is_at_or_wake"] = True
            
        # 拦截当前碎片消息
        self._intercept_event(event)
        self._reset_timer(session_key)

    def _intercept_event(self, event: AstrMessageEvent):
        """
        拦截事件并防止触发后续阶段。
        使用 STREAMING_FINISH 类型结果来让 RespondStage 静默返回。
        """
        event.stop_event()
        if (res := event.get_result()):
            res.set_result_content_type(ResultContentType.STREAMING_FINISH)

    def _reset_timer(self, session_key):
        buffer = self.buffers.get(session_key)
        if not buffer: return
        
        if buffer["timer"]:
            buffer["timer"].cancel()
        
        self.bundle_threshold = self.config.get("bundle_threshold", self.bundle_threshold)
        self.max_wait = self.config.get("max_wait", self.max_wait)
        
        if time.time() - buffer["start_time"] > self.max_wait:
            buffer["timer"] = asyncio.create_task(self._dispatch(session_key))
        else:
            buffer["timer"] = asyncio.create_task(self._wait_and_dispatch(session_key))

    async def _wait_and_dispatch(self, session_key):
        try:
            await asyncio.sleep(self.bundle_threshold)
            await self._dispatch(session_key)
        except asyncio.CancelledError:
            pass
        except Exception as e:
            logger.error(f"InputtingPlugin: 等待任务出错: {e}")

    async def _dispatch(self, session_key):
        buffer = self.buffers.pop(session_key, None)
        if not buffer or not buffer["chain"]:
            return
        
        event = buffer["last_event"]
        
        merged_chain = []
        for comp in buffer["chain"]:
            if isinstance(comp, Plain) and merged_chain and isinstance(merged_chain[-1], Plain):
                merged_chain[-1].text += comp.text
            else:
                merged_chain.append(comp)
        
        if hasattr(event, "message_obj"):
            event.message_obj.message = merged_chain
            event.message_obj.message_str = "".join([c.text if isinstance(c, Plain) else "" for c in merged_chain])
            event.message_str = event.message_obj.message_str
        
        if buffer["is_at_or_wake"]:
            event.is_at_or_wake_command = True
        
        # 准备重新分发事件
        event.set_extra("bundled", True)
        
        # 清除之前拦截时 RespondStage 设置的 _streaming_finished 标记。
        event.set_extra("_streaming_finished", False)
        
        event.continue_event()
        event.clear_result()
        
        logger.info(f"InputtingPlugin: 已合并来自 {session_key} 的消息并放行。内容概要: {event.get_message_outline()}")
        
        self.context.get_event_queue().put_nowait(event)

    async def terminate(self):
        for buffer in self.buffers.values():
            if buffer["timer"]:
                buffer["timer"].cancel()
        self.buffers.clear()
