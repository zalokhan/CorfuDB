/**
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.corfudb.client.entries;

import org.corfudb.client.view.Sequencer;
import org.corfudb.client.view.WriteOnceAddressSpace;
import org.corfudb.client.CorfuDBClient;
import org.corfudb.client.Timestamp;

import java.util.Map;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.Queue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;
import java.io.Serializable;

import java.io.ByteArrayOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ObjectOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectInput;
import java.io.IOException;

import org.corfudb.client.Timestamp;
import org.corfudb.client.OverwriteException;
import org.corfudb.client.abstractions.Stream;
import org.corfudb.client.view.StreamingSequencer;
import org.corfudb.client.view.WriteOnceAddressSpace;
import org.corfudb.client.CorfuDBClient;

/**
 * This class implements a bundle entry, which is encountered inside the move entries of remotely bundled
 * logs. It enables remote logs to easily write results to remotes.
 */
public class BundleEntry implements Serializable {

    byte[] payload;
    long physicalPos;
    UUID remoteLog;
    Map<UUID, Long> epochMap;

    transient Stream s;
    transient CorfuDBClient cdbc;
    transient WriteOnceAddressSpace woas;
    transient StreamingSequencer ss;

    /** Hidden default constructor */
    private BundleEntry() {

    }

    /**
    * This constructor should only be called by the bundle code.
    *
    * @param payload        The payload to insert into the bundle.
    * @param epochMap       A mapping of the current epochs which will be at the remote stream start entry.
    * @param remoteLog      The remote log the bundle is attached to.
    * @param physicalPos    The physical position of the remote slot, if allocated, or -1, if there is no remote slot.
    */
    public BundleEntry(byte[] payload, Map<UUID, Long> epochMap, UUID remoteLog, long physicalPos) {
        this.payload = payload;
        this.physicalPos = physicalPos;
        this.epochMap = new HashMap<UUID, Long>(epochMap);
        this.remoteLog = remoteLog;
    }

    /**
     * This function is called by the stream upcall. It sets the transient information for the bundle.
     *
     * @param s             The stream that read this entry.
     * @param woas          The write once address space of the stream.
     * @param ss            The streaming sequencer for the stream.
     * @param cdbc          The corfudbclient of the stream.
     */
    public void setTransientInfo(Stream s, WriteOnceAddressSpace woas, StreamingSequencer ss, CorfuDBClient cdbc)
    {
        this.s = s;
        this.woas = woas;
        this.ss = ss;
        this.cdbc = cdbc;
    }

    /**
     * Writes a payload in the remote slot, and collects the result of the bundle into the local
     * stream in 1 RTT.
     *
     * @param payload       The payload to insert into the slot.
     *
     * @return              The timestamp for the remote append operation, or null, if there was no remote slot.
     */
    public Timestamp writeSlot(byte[] payload)
    throws OverwriteException, IOException
    {
        if (physicalPos == -1)
        {
            return null;
        }

        //  Write the payload to the remote (TODO: talk to the configuration master instead)
        WriteOnceAddressSpace remote_woas = new WriteOnceAddressSpace(cdbc, remoteLog);
        CorfuDBStreamEntry cdbse = new CorfuDBStreamEntry(epochMap, payload);
        remote_woas.write(physicalPos, cdbse);

        // Read the remote payload and write it to the local slots

        return new Timestamp(epochMap, -1, physicalPos);
    }
}
