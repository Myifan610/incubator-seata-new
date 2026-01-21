package org.apache.seata.apm.skywalking.plugin;

import com.alipay.sofa.common.profile.StringUtil;
import org.apache.seata.apm.skywalking.plugin.common.SWSeataUtils;
import org.apache.seata.core.protocol.AbstractMessage;
import org.apache.seata.core.protocol.HeartbeatMessage;
import org.apache.seata.core.protocol.RpcMessage;
import org.apache.skywalking.apm.agent.core.context.CarrierItem;
import org.apache.skywalking.apm.agent.core.context.ContextCarrier;
import org.apache.skywalking.apm.agent.core.context.ContextManager;
import org.apache.skywalking.apm.agent.core.context.tag.StringTag;
import org.apache.skywalking.apm.agent.core.context.trace.AbstractSpan;
import org.apache.skywalking.apm.agent.core.context.trace.SpanLayer;
import org.apache.skywalking.apm.agent.core.plugin.interceptor.enhance.EnhancedInstance;
import org.apache.skywalking.apm.agent.core.plugin.interceptor.enhance.InstanceMethodsAroundInterceptor;
import org.apache.skywalking.apm.agent.core.plugin.interceptor.enhance.MethodInterceptResult;
import org.apache.skywalking.apm.network.trace.component.ComponentsDefine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.util.Map;

public class RemotingProcessorProcessInterceptor implements InstanceMethodsAroundInterceptor {

    private static final Logger LOGGER = LoggerFactory.getLogger(RemotingProcessorProcessInterceptor.class);

    /**
     * ThreadLocal 标记当前线程是否已成功创建 Span。
     * 使用 ThreadLocal 是因为 Netty 线程会被线程池复用，需要逐线程跟踪创建/停止状态。
     */
    private static final ThreadLocal<Boolean> SPAN_CREATED = new ThreadLocal<>();

    private RpcMessage findRpcMessage(Object[] allArguments) {
        if (allArguments == null) {
            return null;
        }
        for (Object arg : allArguments) {
            if (arg instanceof RpcMessage) {
                return (RpcMessage) arg;
            }
        }
        return null;
    }

    /**
     * 判断是否为心跳包，如果是心跳包，我们跳过监控以减少短生命周期高频 span 的产生。
     */
    private boolean isHeartbeat(RpcMessage rpcMessage) {
        if (rpcMessage == null || rpcMessage.getBody() == null) {
            return false;
        }
        return rpcMessage.getBody() instanceof HeartbeatMessage;
    }

    @Override
    public void beforeMethod(
            EnhancedInstance objInst,
            Method method,
            Object[] allArguments,
            Class<?>[] argumentsTypes,
            MethodInterceptResult result)
            throws Throwable {

        RpcMessage rpcMessage = findRpcMessage(allArguments);

        // 若无消息或为心跳包，直接返回，不创建 span
        if (rpcMessage == null || isHeartbeat(rpcMessage)) {
            return;
        }

        try {
            String operationName = SWSeataUtils.convertOperationName(rpcMessage);
            ContextCarrier contextCarrier = new ContextCarrier();
            CarrierItem next = contextCarrier.items();

            Object headMap = rpcMessage.getHeadMap();
            while (next.hasNext()) {
                next = next.next();
                try {
                    if (headMap instanceof Map) {
                        Object val = ((Map<?, ?>) headMap).get(next.getHeadKey());
                        if (val != null) {
                            next.setHeadValue(String.valueOf(val));
                        }
                    }
                } catch (Throwable headerEx) {
                    // 单个 header 解析失败不影响整体 tracing
                    LOGGER.debug("Failed to set header value for key {}", next.getHeadKey(), headerEx);
                }
            }

            AbstractSpan activeSpan = ContextManager.createEntrySpan(operationName, contextCarrier);
            SpanLayer.asRPCFramework(activeSpan);
            activeSpan.setComponent(ComponentsDefine.SEATA);

            try {
                String xid = SWSeataUtils.convertXid(rpcMessage);
                if (StringUtil.isNotBlank(xid)) {
                    activeSpan.tag(new StringTag(20, "Seata.xid"), xid);
                }
            } catch (Throwable xidEx) {
                // 忽略 XID 提取中的异常，避免影响业务
                LOGGER.debug("Failed to extract XID from RpcMessage", xidEx);
            }

            // 标记当前线程已创建 span
            SPAN_CREATED.set(Boolean.TRUE);
        } catch (Throwable t) {
            // tracing 相关任何异常都不应该影响业务流程
            SPAN_CREATED.remove();
            LOGGER.warn("SkyWalking tracing failed in RemotingProcessorProcessInterceptor.beforeMethod, skip tracing for this call.", t);
        }
    }

    @Override
    public Object afterMethod(
            EnhancedInstance objInst, Method method, Object[] allArguments, Class<?>[] argumentsTypes, Object ret)
            throws Throwable {

        try {
            if (Boolean.TRUE.equals(SPAN_CREATED.get())) {
                try {
                    ContextManager.stopSpan();
                } catch (Throwable stopEx) {
                    LOGGER.warn("SkyWalking stopSpan failed in RemotingProcessorProcessInterceptor.afterMethod.", stopEx);
                }
            }
            return ret;
        } finally {
            // 一定要清理，避免线程池重用时残留状态
            SPAN_CREATED.remove();
        }
    }

    @Override
    public void handleMethodException(
            EnhancedInstance objInst, Method method, Object[] allArguments, Class<?>[] argumentsTypes, Throwable t) {
        try {
            if (Boolean.TRUE.equals(SPAN_CREATED.get())) {
                try {
                    // 尝试将异常记录到当前 span (若 agent API 支持)
                    try {
                        Object current = ContextManager.activeSpan();
                        if (current instanceof AbstractSpan) {
                            try {
                                ((AbstractSpan) current).errorOccurred().log(t);
                            } catch (Throwable logEx) {
                                LOGGER.debug("Unable to log exception to active span", logEx);
                            }
                        }
                    } catch (Throwable activeEx) {
                        // 忽略 activeSpan() API 差异导致的问题
                        LOGGER.debug("Unable to obtain active span for logging", activeEx);
                    }
                    ContextManager.stopSpan();
                } catch (Throwable stopEx) {
                    LOGGER.warn("SkyWalking stopSpan failed in RemotingProcessorProcessInterceptor.handleMethodException.", stopEx);
                }
            }
        } finally {
            SPAN_CREATED.remove();
        }
    }
}
