import asyncio
import time
from astrbot.api.event import filter, AstrMessageEvent, MessageEventResult
from astrbot.api.star import Context, Star, register
from astrbot.api import logger
from astrbot.api.message_components import Plain

@register("astrbot_plugin_inputting", "e.e.", "消息自动合并插件：当用户正在输入或连续发送短句时进行拦截与打包，解决 LLM 响应碎片化问题。", "1.0.3")
class InputtingPlugin(Star):
    def __init__(self, context: Context, config: dict = None):
        super().__init__(context)
        self.config = config or {}
        # session_key -> {message_chain: [], last_event: event, timer: task, is_at_or_wake: bool, start_time: float}
        self.buffers = {} 
        
    async def initialize(self):
        # 获取最新的配置
        self.config = self.context.get_config()
        self.bundle_threshold = self.config.get("bundle_threshold", 1.5)
        self.max_wait = self.config.get("max_wait", 20.0)

    @filter.event_message_type(filter.EventMessageType.ALL)
    async def on_all_message(self, event: AstrMessageEvent):
        # 1. 检查是否已经打包过，防止循环处理
        if event.get_extra("bundled"):
            return

        # 获取消息来源的统一标识符 (platform:type:session_id)
        session_key = event.unified_msg_origin
        
        # 获取消息链
        chain = event.get_messages()
        # 检查是否为空消息（正在输入状态）
        # outline = event.get_message_outline().strip()
        is_empty = not chain or (len(chain) == 1 and isinstance(chain[0], Plain) and not chain[0].text.strip())
        
        # 2. 处理“正在输入”或空消息
        if is_empty:
            # 如果该会话正在等待合并，则收到任何活动（包括正在输入）都应视为用户仍在活跃，重置计时器
            if session_key in self.buffers:
                self._reset_timer(session_key)
                # 拦截此空消息，防止触发后续阶段（如 RespondStage 的日志）
                event.stop_event()
                event.clear_result() # 进一步确保 RespondStage 不打印 Prepare to send
            return

        # 3. 处理正常文本消息
        if session_key not in self.buffers:
            self.buffers[session_key] = {
                "chain": [],
                "last_event": None,
                "timer": None,
                "is_at_or_wake": False,
                "start_time": time.time()
            }
        
        buffer = self.buffers[session_key]
        
        # 如果不是第一条消息，添加换行符
        if buffer["chain"]:
            buffer["chain"].append(Plain("\n"))
            
        buffer["chain"].extend(chain)
        buffer["last_event"] = event
        
        # 如果合并包中任一条消息带有唤醒词或 @，则最终结果应保留此状态
        if getattr(event, "is_at_or_wake_command", False):
            buffer["is_at_or_wake"] = True
            
        # 拦截当前事件，防止立即进入 LLM 阶段
        event.stop_event()
        event.clear_result() # 确保不触发 RespondStage 日志
        
        # 重置计时器，开始/更新倒计时
        self._reset_timer(session_key)

    def _reset_timer(self, session_key):
        buffer = self.buffers.get(session_key)
        if not buffer: return
        
        if buffer["timer"]:
            buffer["timer"].cancel()
        
        # 检查是否超过最大容忍等待时间
        if time.time() - buffer["start_time"] > self.max_wait:
            # 立即执行合并
            buffer["timer"] = asyncio.create_task(self._dispatch(session_key))
        else:
            # 延迟执行
            buffer["timer"] = asyncio.create_task(self._wait_and_dispatch(session_key))

    async def _wait_and_dispatch(self, session_key):
        try:
            # 等待设定的静默期
            await asyncio.sleep(self.bundle_threshold)
            await self._dispatch(session_key)
        except asyncio.CancelledError:
            pass
        except Exception as e:
            logger.error(f"InputtingPlugin: 等待任务出错: {e}")

    async def _dispatch(self, session_key):
        # 弹出缓冲区数据
        buffer = self.buffers.pop(session_key, None)
        if not buffer or not buffer["chain"]:
            return
        
        event = buffer["last_event"]
        
        # 合并相邻的 Plain 组件以保持消息链整洁
        merged_chain = []
        for comp in buffer["chain"]:
            if isinstance(comp, Plain) and merged_chain and isinstance(merged_chain[-1], Plain):
                merged_chain[-1].text += comp.text
            else:
                merged_chain.append(comp)
        
        # 更新事件内容
        if hasattr(event, "message_obj"):
            event.message_obj.message = merged_chain
            # 重新生成 message_str
            event.message_obj.message_str = "".join([c.text if isinstance(c, Plain) else "" for c in merged_chain])
            event.message_str = event.message_obj.message_str
        
        # 恢复唤醒标志
        if buffer["is_at_or_wake"]:
            event.is_at_or_wake_command = True
        
        # 标记为已打包，恢复事件传播
        event.set_extra("bundled", True)
        event.continue_event()
        event.clear_result()
        
        logger.info(f"InputtingPlugin: 已合并来自 {session_key} 的消息并放行。内容概要: {event.get_message_outline()}")
        
        # 重新将事件推入总线队列，使其从头开始执行 pipeline，但这次不会被本插件拦截
        self.context.get_event_queue().put_nowait(event)

    async def terminate(self):
        # 插件卸载时清理所有定时器
        for buffer in self.buffers.values():
            if buffer["timer"]:
                buffer["timer"].cancel()
        self.buffers.clear()
