package org.apache.seata.apm.skywalking.plugin;

import com.alipay.sofa.common.profile.StringUtil;
import org.apache.seata.apm.skywalking.plugin.common.SWSeataUtils;
import org.apache.seata.core.protocol.AbstractMessage;
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

public class RemotingProcessorProcessInterceptor implements InstanceMethodsAroundInterceptor {

    private static final Logger LOGGER = LoggerFactory.getLogger(RemotingProcessorProcessInterceptor.class);

    /**
     * ThreadLocal 标记：当前线程是否在 beforeMethod 中成功创建了 entry span
     * 使用 ThreadLocal 而不是 EnhancedInstance 的 dynamicField 是因为处理器可能被多个 Netty 线程复用，
     * 我们需要按线程区分 span 的创建与停止。
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

    @Override
    public void beforeMethod(
            EnhancedInstance objInst,
            Method method,
            Object[] allArguments,
            Class<?>[] argumentsTypes,
            MethodInterceptResult result)
            throws Throwable {

        RpcMessage rpcMessage = findRpcMessage(allArguments);
        if (rpcMessage == null) {
            return;
        }

        try {
            String operationName = SWSeataUtils.convertOperationName(rpcMessage);
            ContextCarrier contextCarrier = new ContextCarrier();
            CarrierItem next = contextCarrier.items();
            // 如果 headMap 为 null，避免 NPE
            Object headMap = rpcMessage.getHeadMap();
            while (next.hasNext()) {
                next = next.next();
                try {
                    if (headMap != null) {
                        Object val = ((java.util.Map) headMap).get(next.getHeadKey());
                        if (val != null) {
                            next.setHeadValue(String.valueOf(val));
                        }
                    }
                } catch (Throwable ignore) {
                    // 如果 head map 的结构不符合预期，忽略单个 header 的注入，继续处理其余 header
                    LOGGER.debug("Failed to set head value for key {}", next.getHeadKey(), ignore);
                }
            }
            AbstractSpan activeSpan = ContextManager.createEntrySpan(operationName, contextCarrier);
            SpanLayer.asRPCFramework(activeSpan);
            activeSpan.setComponent(ComponentsDefine.SEATA);

            String xid = SWSeataUtils.convertXid(rpcMessage);
            if (StringUtil.isNotBlank(xid)) {
                activeSpan.tag(new StringTag(20, "Seata.xid"), xid);
            }

            // 标记当前线程成功创建了 span
            SPAN_CREATED.set(Boolean.TRUE);
        } catch (Throwable t) {
            // 捕获任何 tracing 相关的异常，避免影响业务处理
            SPAN_CREATED.remove();
            LOGGER.warn("SkyWalking tracing failed in RemotingProcessorProcessInterceptor.beforeMethod, skip tracing for this call.", t);
        }
    }

    @Override
    public Object afterMethod(
            EnhancedInstance objInst, Method method, Object[] allArguments, Class<?>[] argumentsTypes, Object ret)
            throws Throwable {

        try {
            RpcMessage rpcMessage = findRpcMessage(allArguments);
            if (Boolean.TRUE.equals(SPAN_CREATED.get())) {
                // 只有在 beforeMethod 确实创建了 span 时才停止
                try {
                    if (rpcMessage != null && rpcMessage.getBody() instanceof AbstractMessage) {
                        ContextManager.stopSpan();
                    }
                } catch (Throwable stopEx) {
                    LOGGER.warn("SkyWalking stopSpan failed in RemotingProcessorProcessInterceptor.afterMethod.", stopEx);
                }
            }
            return ret;
        } finally {
            // 清理 ThreadLocal，防止内存泄露或复用线程时残留标记
            SPAN_CREATED.remove();
        }
    }

    @Override
    public void handleMethodException(
            EnhancedInstance objInst, Method method, Object[] allArguments, Class<?>[] argumentsTypes, Throwable t) {
        // 当方法抛异常时也尝试做清理，避免 tracing 导致二次异常影响业务
        try {
            if (Boolean.TRUE.equals(SPAN_CREATED.get())) {
                try {
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
