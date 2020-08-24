/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.tomitribe.activemq.util;

import org.apache.activemq.store.kahadb.disk.journal.Location;

public class MessageInfo {
    private final String destination;
    private final String messageId;
    private final Location location;

    public MessageInfo(final String destination, final String messageId, final Location location) {
        this.destination = destination;
        this.messageId = messageId;
        this.location = location;
    }

    public String getDestination() {
        return destination;
    }

    public String getMessageId() {
        return messageId;
    }

    public Location getLocation() {
        return location;
    }
}
