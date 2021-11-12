/*
 * Copyright (c) 2011-2021 VMware, Inc. or its affiliates, All Rights Reserved.
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
package reactor.netty.resources;

import java.util.Iterator;
import java.util.Spliterator;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Supplier;

import io.netty.channel.EventLoop;
import io.netty.channel.EventLoopGroup;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.FastThreadLocal;
import io.netty.util.concurrent.Future;

/**
 * Reuse local event loop if already working inside one.
 */
final class ColocatedEventLoopGroup implements EventLoopGroup, Supplier<EventLoopGroup> {

	final EventLoopGroup eventLoopGroup;
	final FastThreadLocal<EventLoop> localLoop = new FastThreadLocal<>();

	@SuppressWarnings("FutureReturnValueIgnored")
	ColocatedEventLoopGroup(EventLoopGroup eventLoopGroup) {
		this.eventLoopGroup = eventLoopGroup;
		for (EventExecutor ex : eventLoopGroup) {
			if (ex instanceof EventLoop) {
				EventLoop eventLoop = (EventLoop) ex;
				if (eventLoop.inEventLoop()) {
					if (!localLoop.isSet()) {
						localLoop.set(eventLoop);
					}
				}
				else {
					//"FutureReturnValueIgnored" this is deliberate
					eventLoop.submit(() -> {
						if (!localLoop.isSet()) {
							localLoop.set(eventLoop);
						}
					});
				}
			}
		}
	}

	@Override
	public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
		return eventLoopGroup.awaitTermination(timeout, unit);
	}

	@Override
	public void execute(Runnable command) {
		next().execute(command);
	}

	@Override
	public void forEach(Consumer<? super EventExecutor> action) {
		eventLoopGroup.forEach(action);
	}

	@Override
	public EventLoopGroup get() {
		return eventLoopGroup;
	}

	@Override
	public boolean isShutdown() {
		return eventLoopGroup.isShutdown();
	}

	@Override
	public boolean isShuttingDown() {
		return eventLoopGroup.isShuttingDown();
	}

	@Override
	public boolean isTerminated() {
		return eventLoopGroup.isTerminated();
	}

	@Override
	public Iterator<EventExecutor> iterator() {
		return eventLoopGroup.iterator();
	}

	@Override
	public EventLoop next() {
		if (localLoop.isSet()) {
			return localLoop.get();
		}
		return eventLoopGroup.next();
	}

	@Override
	public <V> Future<V> schedule(Callable<V> callable, long delay, TimeUnit unit) {
		return next().schedule(callable, delay, unit);
	}

	@Override
	public Future<Void> schedule(Runnable command, long delay, TimeUnit unit) {
		return next().schedule(command, delay, unit);
	}

	@Override
	public Future<Void> scheduleAtFixedRate(Runnable command, long initialDelay, long period, TimeUnit unit) {
		return next().scheduleAtFixedRate(command, initialDelay, period, unit);
	}

	@Override
	public Future<Void> scheduleWithFixedDelay(Runnable command, long initialDelay, long delay, TimeUnit unit) {
		return next().scheduleWithFixedDelay(command, initialDelay, delay, unit);
	}

	@Override
	public Future<Void> shutdownGracefully() {
		clean();
		return eventLoopGroup.shutdownGracefully();
	}

	@Override
	public Future<Void> shutdownGracefully(long quietPeriod, long timeout, TimeUnit unit) {
		clean();
		return eventLoopGroup.shutdownGracefully(quietPeriod, timeout, unit);
	}

	@Override
	public Spliterator<EventExecutor> spliterator() {
		return eventLoopGroup.spliterator();
	}

	@Override
	public <T> Future<T> submit(Callable<T> task) {
		return next().submit(task);
	}

	@Override
	public Future<Void> submit(Runnable task) {
		return next().submit(task);
	}

	@Override
	public <T> Future<T> submit(Runnable task, T result) {
		return next().submit(task, result);
	}

	@Override
	public Future<Void> terminationFuture() {
		return eventLoopGroup.terminationFuture();
	}

	void clean() {
		for (EventExecutor ex : eventLoopGroup) {
			ex.execute(() -> localLoop.set(null));
		}
	}
}
