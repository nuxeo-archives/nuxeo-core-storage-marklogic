/*
 * (C) Copyright 2017 Nuxeo SA (http://nuxeo.com/) and others.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Contributors:
 *     Kevin Leturc
 */
package org.nuxeo.ecm.core.storage.marklogic;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.nuxeo.ecm.core.api.NuxeoException;

/**
 * A cursor registry, which holds a current cursor on DB to perform scroll operations.
 *
 * @param <C> The cursor type.
 * @since 9.1
 */
public class CursorRegistry<C> {

    private static final Log log = LogFactory.getLog(CursorRegistry.class);

    protected Map<String, CursorResult<C>> cursorResults = new ConcurrentHashMap<>();

    /**
     * This method returns the {@link CursorResult} associated to the input <code>scrollId</code>, if it exists and it
     * is not timed out.
     *
     * @param scrollId the scroll id of {@link CursorResult} to retrieve
     * @return the associated {@link CursorResult} if it exists and it's not timed out.
     */
    public Optional<CursorResult<C>> getCursor(String scrollId) {
        CursorResult<C> cursorResult = cursorResults.get(scrollId);
        if (isScrollTimedOut(scrollId, cursorResult)) {
            return Optional.empty();
        }
        return Optional.of(cursorResult);
    }

    public void checkForTimedOutScroll() {
        cursorResults.entrySet().stream().forEach(e -> isScrollTimedOut(e.getKey(), e.getValue()));
    }

    private boolean isScrollTimedOut(String scrollId, CursorResult<C> cursorResult) {
        if (cursorResult == null || cursorResult.timedOut()) {
            if (unregisterCursor(cursorResult)) {
                log.warn("Scroll '" + scrollId + "' timed out");
            }
            return true;
        }
        return false;
    }

    /**
     * Registers the input {@link C} and generates a new <code>scrollId</code> to associate with.
     *
     * @return the scrollId associated to the cursor.
     */
    public String registerCursor(C cursor, int batchSize, int keepAliveSeconds) {
        return registerCursorResult(new CursorResult<>(cursor, batchSize, keepAliveSeconds));
    }

    /**
     * Registers the input {@link C} associated to the input <code>scrollId</code>.
     *
     * @return the scrollId associated to the cursor.
     */
    public String registerCursor(String scrollId, C cursor, int batchSize, int keepAliveSeconds) {
        return registerCursorResult(scrollId, new CursorResult<>(cursor, batchSize, keepAliveSeconds));
    }

    /**
     * Registers the input {@link CursorResult} and generates a new <code>scrollId</code> to associate with.
     *
     * @return the scrollId associated to the cursor result.
     */
    public String registerCursorResult(CursorResult<C> cursorResult) {
        String scrollId = UUID.randomUUID().toString();
        return registerCursorResult(scrollId, cursorResult);
    }

    /**
     * Registers the input {@link CursorResult} associated to the input <code>scrollId</code>.
     *
     * @return the scrollId associated to the cursor result.
     */
    public String registerCursorResult(String scrollId, CursorResult<C> cursorResult) {
        cursorResults.put(scrollId, cursorResult);
        return scrollId;
    }

    public boolean unregisterCursor(String scrollId) {
        return unregisterCursor(cursorResults.remove(scrollId));
    }

    private boolean unregisterCursor(CursorResult<C> cursor) {
        if (cursor != null) {
            cursor.close();
            return true;
        }
        return false;
    }

    public static class CursorResult<C> implements Closeable {

        // Note that MongoDB cursor automatically timeout after 10 minutes of inactivity by default.
        private C cursor;

        private final int batchSize;

        private long lastCallTimestamp;

        private final int keepAliveSeconds;

        public CursorResult(C cursor, int batchSize, int keepAliveSeconds) {
            this.cursor = cursor;
            this.batchSize = batchSize;
            this.keepAliveSeconds = keepAliveSeconds;
            this.lastCallTimestamp = System.currentTimeMillis();
        }

        public C getCursor() {
            return cursor;
        }

        public int getBatchSize() {
            return batchSize;
        }

        public void touch() {
            lastCallTimestamp = System.currentTimeMillis();
        }

        public boolean timedOut() {
            long now = System.currentTimeMillis();
            return now - lastCallTimestamp > keepAliveSeconds * 1000;
        }

        /**
         * CAUTION: if your cursor doesn't implement {@link Closeable}, we just set the field to null
         */
        @Override
        public void close() {
            if (cursor instanceof Closeable) {
                try {
                    ((Closeable) cursor).close();
                } catch (IOException e) {
                    throw new NuxeoException("Unable to close cursor", e);
                }
            }
            cursor = null;
        }

    }

}
