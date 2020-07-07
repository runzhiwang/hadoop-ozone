/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.ozone.container.common.statemachine;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Command queue with lock and the item of queue should only be
 * executed by one thread at the same time.
 */
public class CommandQueueWithLock<T> {
  private final Lock lock = new ReentrantLock();
  private final Queue<T> queue = new ConcurrentLinkedDeque<>();

  public boolean tryLock() {
    return lock.tryLock();
  }

  public void unLock() {
    lock.unlock();
  }

  public void add(T t) {
    queue.add(t);
  }

  public T peek() {
    return queue.peek();
  }

  public T poll() {
    return queue.poll();
  }

  public boolean isEmpty() {
    return queue.isEmpty();
  }
}
