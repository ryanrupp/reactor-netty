/*
 * Copyright (c) 2022 VMware, Inc. or its affiliates, All Rights Reserved.
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
package reactor.netty.internal.util;

import java.util.Map;
import java.util.function.Function;

/**
 * It's a temporary workaround for Java 8 specific performance issue JDK-8161372
 * and this class should be removed once the Java 8 support is dropped.
 * <p><strong>Note:</strong> This utility class is for internal use only. It can be removed at any time.
 *
 * @author zimatars
 * @since 1.0.15
 */
public final class MapUtils {

	/**
	 * ConcurrentHashMap.computeIfAbsent(k,v) locks bin when k present.
	 * Add pre-screen before locking inside computeIfAbsent.
	 * <p><strong>Note:</strong> This utility is not for a general purpose usage.
	 * Carefully consider the removal operations from the map.
	 * If you have many remove operations that are critical, do not use this pre-screening.
	 *
	 * @param map the ConcurrentHashMap instance
	 * @param key key with which the specified value is to be associated
	 * @param mappingFunction the function to compute a value
	 * @return the current (existing or computed) value associated with
	 *         the specified key, or null if the computed value is null
	 */
	public static <K, V> V computeIfAbsent(Map<K, V> map, K key, Function<K, V> mappingFunction) {
		V value = map.get(key);
		return value != null ? value : map.computeIfAbsent(key, mappingFunction);
	}

	private MapUtils() {
	}
}
