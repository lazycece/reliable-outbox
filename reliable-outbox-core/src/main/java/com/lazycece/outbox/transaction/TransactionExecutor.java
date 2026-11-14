/*
 *    Copyright (C) $YEAR lazycece<lazycece@gmail.com>. All rights reserved.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */
package com.lazycece.outbox.transaction;

import java.util.function.Supplier;

/**
 * @author lazycece
 * @date 2026/11/11
 */
public interface TransactionExecutor {

    /**
     * Joins an existing transaction on the outbox data source; fails when none exists.
     */
    <T> T executeInExistingTransaction(Supplier<T> action);

    /**
     * Suspends any current transaction and creates an independent one.
     */
    <T> T executeInNewTransaction(Supplier<T> action);

    /**
     * Must be called while one of the transactions above is active.
     */
    void afterCommit(Runnable action);
}
