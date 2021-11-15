/*
 * Copyright (c) 2020-2021 VMware, Inc. or its affiliates, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package reactor.netty.transport;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFactory;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoop;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.ServerChannelFactory;
import io.netty.channel.unix.DomainSocketAddress;
import io.netty.resolver.AddressResolver;
import io.netty.resolver.AddressResolverGroup;
import io.netty.util.AttributeKey;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.FutureListener;
import io.netty.util.concurrent.Promise;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.publisher.Mono;
import reactor.netty.Connection;
import reactor.util.Logger;
import reactor.util.Loggers;
import reactor.util.annotation.Nullable;
import reactor.util.retry.Retry;

import java.net.SocketAddress;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.Predicate;
import java.util.function.Supplier;

import static reactor.netty.ReactorNetty.format;

/**
 * {@link TransportConnector} is a helper class that creates, initializes and registers the channel.
 * It performs the actual connect operation to the remote peer or binds the channel.
 *
 * @author Stephane Maldini
 * @author Violeta Georgieva
 * @since 1.0.0
 */
public final class TransportConnector {

	TransportConnector() {}

	/**
	 * Binds a {@link Channel}.
	 *
	 * @param config the transport configuration
	 * @param channelInitializer the {@link ChannelInitializer} that will be used for initializing the channel pipeline
	 * @param bindAddress the local address
	 * @return a {@link Mono} of {@link Channel}
	 */
	public static Mono<Channel> bind(TransportConfig config, ChannelInitializer<Channel> channelInitializer,
			SocketAddress bindAddress, boolean isDomainSocket) {
		Objects.requireNonNull(config, "config");
		Objects.requireNonNull(bindAddress, "bindAddress");
		Objects.requireNonNull(channelInitializer, "channelInitializer");

		return doInitAndRegister(config, channelInitializer, isDomainSocket)
				.flatMap(channel -> {
					MonoFutureListener listener = new MonoFutureListener(channel);
					channel.executor().execute(() -> channel.bind(bindAddress).addListener(listener));
					return listener;
				});
	}

	/**
	 * Connect a {@link Channel} to the remote peer.
	 *
	 * @param config the transport configuration
	 * @param remoteAddress the {@link SocketAddress} to connect to
	 * @param resolverGroup the resolver which will resolve the address of the unresolved named address
	 * @param channelInitializer the {@link ChannelInitializer} that will be used for initializing the channel pipeline
	 * @return a {@link Mono} of {@link Channel}
	 */
	public static Mono<Channel> connect(TransportConfig config, SocketAddress remoteAddress,
			AddressResolverGroup<?> resolverGroup, ChannelInitializer<Channel> channelInitializer) {
		Objects.requireNonNull(config, "config");
		Objects.requireNonNull(remoteAddress, "remoteAddress");
		Objects.requireNonNull(resolverGroup, "resolverGroup");
		Objects.requireNonNull(channelInitializer, "channelInitializer");

		boolean isDomainAddress = remoteAddress instanceof DomainSocketAddress;
		return doInitAndRegister(config, channelInitializer, isDomainAddress)
				.flatMap(channel -> doResolveAndConnect(channel, config, remoteAddress, resolverGroup)
						.onErrorResume(RetryConnectException.class,
								t -> {
									AtomicInteger index = new AtomicInteger(1);
									return Mono.defer(() ->
											doInitAndRegister(config, channelInitializer, isDomainAddress)
													.flatMap(ch -> {
														MonoFutureListener listener = new MonoFutureListener(ch);
														doConnect(t.addresses, config.bindAddress(), listener, index.get());
														return listener;
													}))
											.retryWhen(Retry.max(t.addresses.size() - 1)
															.filter(RETRY_PREDICATE)
															.doBeforeRetry(sig -> index.incrementAndGet()));
								}));
	}

	/**
	 * Set the channel attributes
	 *
	 * @param channel the channel
	 * @param attrs the attributes
	 */
	@SuppressWarnings("unchecked")
	static void setAttributes(Channel channel, Map<AttributeKey<?>, ?> attrs) {
		for (Map.Entry<AttributeKey<?>, ?> e : attrs.entrySet()) {
			channel.attr((AttributeKey<Object>) e.getKey()).set(e.getValue());
		}
	}

	/**
	 * Set the channel options
	 *
	 * @param channel the channel
	 * @param options the options
	 */
	@SuppressWarnings("unchecked")
	static void setChannelOptions(Channel channel, Map<ChannelOption<?>, ?> options, boolean isDomainSocket) {
		for (Map.Entry<ChannelOption<?>, ?> e : options.entrySet()) {
			if (isDomainSocket &&
					(ChannelOption.SO_REUSEADDR.equals(e.getKey()) || ChannelOption.TCP_NODELAY.equals(e.getKey()))) {
				continue;
			}
			try {
				if (!channel.config().setOption((ChannelOption<Object>) e.getKey(), e.getValue())) {
					log.warn(format(channel, "Unknown channel option '{}' for channel '{}'"), e.getKey(), channel);
				}
			}
			catch (Throwable t) {
				log.warn(format(channel, "Failed to set channel option '{}' with value '{}' for channel '{}'"),
						e.getKey(), e.getValue(), channel, t);
			}
		}
	}

	@SuppressWarnings("FutureReturnValueIgnored")
	static void doConnect(
			List<SocketAddress> addresses,
			@Nullable Supplier<? extends SocketAddress> bindAddress,
			MonoFutureListener listener,
			int index) {
		Channel channel = listener.channel();
		channel.executor().execute(() -> {
			SocketAddress remoteAddress = addresses.get(index);

			if (log.isDebugEnabled()) {
				log.debug(format(channel, "Connecting to [" + remoteAddress + "]."));
			}

			Future<Void> f;
			if (bindAddress == null) {
				f = channel.connect(remoteAddress);
			}
			else {
				SocketAddress local = Objects.requireNonNull(bindAddress.get(), "bindAddress");
				f = channel.connect(remoteAddress, local);
			}

			f.addListener(future -> {
				if (future.isSuccess()) {
					listener.trySuccess();
				}
				else {
					// "FutureReturnValueIgnored" this is deliberate
					channel.close();

					Throwable cause = future.cause();
					if (log.isDebugEnabled()) {
						log.debug(format(channel, "Connect attempt to [" + remoteAddress + "] failed."), cause);
					}

					int next = index + 1;
					if (next < addresses.size()) {
						listener.tryFailure(new RetryConnectException(addresses));
					}
					else {
						listener.tryFailure(cause);
					}
				}
			});
		});
	}

	static Mono<Channel> doInitAndRegister(
			TransportConfig config,
			ChannelInitializer<Channel> channelInitializer,
			boolean isDomainSocket) {

		boolean onServer = channelInitializer instanceof ServerTransport.AcceptorInitializer;
		// Create channel
		EventLoop loop = config.eventLoopGroup().next();
		final Channel channel;
		try {
			if (onServer) {
				EventLoopGroup childEventLoopGroup = ((ServerTransportConfig) config).childEventLoopGroup();
				ServerChannelFactory<? extends Channel> channelFactory = config.serverConnectionFactory(isDomainSocket);
				channel = channelFactory.newChannel(loop, childEventLoopGroup);
				((ServerTransport.AcceptorInitializer) channelInitializer).acceptor.enableAutoReadTask(channel);
			}
			else {
				ChannelFactory<? extends Channel> channelFactory = config.connectionFactory(isDomainSocket);
				channel = channelFactory.newChannel(loop);
			}
		}
		catch (Throwable t) {
			return Mono.error(t);
		}

		MonoFutureListener listener = new MonoFutureListener(channel);
		loop.execute(() -> {
			try {
				// Init channel
				setChannelOptions(channel, config.options, isDomainSocket);
				setAttributes(channel, config.attrs);

				if (onServer) {
					Promise<Channel> promise = channel.executor().newPromise();
					((ServerTransport.AcceptorInitializer) channelInitializer).promise = promise;
					// TODO think for another way of doing this
					channel.pipeline().addLast(channelInitializer);
					promise.asFuture().addListener(future -> {
						if (future.isSuccess()) {
							channel.register().addListener(listener);
						}
						else {
							channel.unsafe().closeForcibly();
							listener.tryFailure(future.cause());
						}
					});
				}
				else {
					channel.pipeline().addLast(channelInitializer);
					channel.register().addListener(listener);
				}
			}
			catch (Throwable t) {
				listener.tryFailure(t);
			}
		});
		return listener;
	}

	@SuppressWarnings({"unchecked", "FutureReturnValueIgnored"})
	static Mono<Channel> doResolveAndConnect(Channel channel, TransportConfig config,
			SocketAddress remoteAddress, AddressResolverGroup<?> resolverGroup) {
		try {
			AddressResolver<SocketAddress> resolver;
			try {
				resolver = (AddressResolver<SocketAddress>) resolverGroup.getResolver(channel.executor());
			}
			catch (Throwable t) {
				// "FutureReturnValueIgnored" this is deliberate
				channel.close();
				return Mono.error(t);
			}

			Supplier<? extends SocketAddress> bindAddress = config.bindAddress();
			if (!resolver.isSupported(remoteAddress) || resolver.isResolved(remoteAddress)) {
				MonoFutureListener listener = new MonoFutureListener(channel);
				doConnect(Collections.singletonList(remoteAddress), bindAddress, listener, 0);
				return listener;
			}

			if (config instanceof ClientTransportConfig) {
				final ClientTransportConfig<?> clientTransportConfig = (ClientTransportConfig<?>) config;
				if (clientTransportConfig.doOnResolve != null) {
					clientTransportConfig.doOnResolve.accept(Connection.from(channel));
				}
			}

			Future<List<SocketAddress>> resolveFuture = resolver.resolveAll(remoteAddress);

			if (config instanceof ClientTransportConfig) {
				final ClientTransportConfig<?> clientTransportConfig = (ClientTransportConfig<?>) config;

				if (clientTransportConfig.doOnResolveError != null) {
					resolveFuture.addListener(future -> {
						if (future.cause() != null) {
							clientTransportConfig.doOnResolveError.accept(Connection.from(channel), future.cause());
						}
					});
				}

				if (clientTransportConfig.doAfterResolve != null) {
					resolveFuture.addListener(future -> {
						if (future.isSuccess()) {
							clientTransportConfig.doAfterResolve.accept(Connection.from(channel), future.getNow().get(0));
						}
					});
				}
			}

			if (resolveFuture.isDone()) {
				Throwable cause = resolveFuture.cause();
				if (cause != null) {
					// "FutureReturnValueIgnored" this is deliberate
					channel.close();
					return Mono.error(cause);
				}
				else {
					MonoFutureListener listener = new MonoFutureListener(channel);
					doConnect(resolveFuture.getNow(), bindAddress, listener, 0);
					return listener;
				}
			}

			MonoFutureListener listener = new MonoFutureListener(channel);
			resolveFuture.addListener(future -> {
				if (future.cause() != null) {
					// "FutureReturnValueIgnored" this is deliberate
					channel.close();
					listener.tryFailure(future.cause());
				}
				else {
					doConnect(future.getNow(), bindAddress, listener, 0);
				}
			});
			return listener;
		}
		catch (Throwable t) {
			return Mono.error(t);
		}
	}

	static final class MonoFutureListener extends Mono<Channel> implements FutureListener<Void>, Subscription {

		volatile Object result;
		static final AtomicReferenceFieldUpdater<MonoFutureListener, Object> RESULT_UPDATER =
				AtomicReferenceFieldUpdater.newUpdater(MonoFutureListener.class, Object.class, "result");
		static final Object SUCCESS = new Object();

		final Channel channel;

		CoreSubscriber<? super Channel> actual;

		MonoFutureListener(Channel channel) {
			this.channel = channel;
		}

		@Override
		@SuppressWarnings("FutureReturnValueIgnored")
		public void cancel() {
			if (channel.isRegistered()) {
				// "FutureReturnValueIgnored" this is deliberate
				channel.close();
			}
			else {
				channel.unsafe().closeForcibly();
			}
		}

		@Override
		public void operationComplete(Future<? extends Void> future) {
			Throwable t = future.cause();
			if (t != null) {
				tryFailure(t);
			}
			else {
				trySuccess();
			}
		}

		@Override
		public void request(long n) {
			// noop
		}

		@Override
		public void subscribe(CoreSubscriber<? super Channel> actual) {
			EventLoop eventLoop = channel.executor();
			if (eventLoop.inEventLoop()) {
				_subscribe(actual);
			}
			else {
				eventLoop.execute(() -> _subscribe(actual));
			}
		}

		@Nullable
		Throwable cause() {
			Object result = this.result;
			return result == SUCCESS ? null : (Throwable) result;
		}

		Channel channel() {
			return channel;
		}

		boolean isDone() {
			Object result = this.result;
			return result != null;
		}

		boolean isSuccess() {
			Object result = this.result;
			return result == SUCCESS;
		}

		void _subscribe(CoreSubscriber<? super Channel> actual) {
			this.actual = actual;
			actual.onSubscribe(this);

			if (isDone()) {
				if (isSuccess()) {
					actual.onNext(channel);
					actual.onComplete();
				}
				else {
					actual.onError(cause());
					cancel();
				}
			}
		}

		void tryFailure(Throwable cause) {
			if (RESULT_UPDATER.compareAndSet(this, null, cause)) {
				if (actual != null) {
					actual.onError(cause);
					cancel();
				}
			}
		}

		void trySuccess() {
			if (RESULT_UPDATER.compareAndSet(this, null, SUCCESS)) {
				if (actual != null) {
					actual.onNext(channel);
					actual.onComplete();
				}
			}
		}
	}

	static final class RetryConnectException extends RuntimeException {

		final List<SocketAddress> addresses;

		RetryConnectException(List<SocketAddress> addresses) {
			this.addresses = addresses;
		}

		@Override
		public synchronized Throwable fillInStackTrace() {
			// omit stacktrace for this exception
			return this;
		}

		private static final long serialVersionUID = -207274323623692199L;
	}

	static final Logger log = Loggers.getLogger(TransportConnector.class);

	static final Predicate<Throwable> RETRY_PREDICATE = t -> t instanceof RetryConnectException;
}
