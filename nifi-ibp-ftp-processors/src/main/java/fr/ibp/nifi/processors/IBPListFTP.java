/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package fr.ibp.nifi.processors;

import java.util.ArrayList;
import java.util.List;

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.Stateful;
import org.apache.nifi.annotation.behavior.TriggerSerially;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processors.standard.GetFTP;
import org.apache.nifi.processors.standard.ListFileTransfer;
import org.apache.nifi.processors.standard.PutFTP;
import org.apache.nifi.processors.standard.util.FileTransfer;

@TriggerSerially
@InputRequirement(Requirement.INPUT_FORBIDDEN)
@Tags({"list", "ftp", "remote", "ingest", "source", "input", "files"})
@CapabilityDescription("Performs a listing of the files residing on an FTP server. For each file that is found on the remote server, a new FlowFile will be created with the filename attribute "
    + "set to the name of the file on the remote server. This can then be used in conjunction with FetchFTP in order to fetch those files.")
@SeeAlso({GetFTP.class, PutFTP.class})
@WritesAttributes({
    @WritesAttribute(attribute = "ftp.remote.host", description = "The hostname of the FTP Server"),
    @WritesAttribute(attribute = "ftp.remote.port", description = "The port that was connected to on the FTP Server"),
    @WritesAttribute(attribute = "ftp.listing.user", description = "The username of the user that performed the FTP Listing"),
    @WritesAttribute(attribute = "file.owner", description = "The numeric owner id of the source file"),
    @WritesAttribute(attribute = "file.group", description = "The numeric group id of the source file"),
    @WritesAttribute(attribute = "file.permissions", description = "The read/write/execute permissions of the source file"),
    @WritesAttribute(attribute = "filename", description = "The name of the file on the SFTP Server"),
    @WritesAttribute(attribute = "path", description = "The fully qualified name of the directory on the SFTP Server from which the file was pulled"),
})
@Stateful(scopes = {Scope.CLUSTER}, description = "After performing a listing of files, the timestamp of the newest file is stored. "
    + "This allows the Processor to list only files that have been added or modified after "
    + "this date the next time that the Processor is run. State is stored across the cluster so that this Processor can be run on Primary Node only and if "
    + "a new Primary Node is selected, the new node will not duplicate the data that was listed by the previous Primary Node.")
public class IBPListFTP extends ListFileTransfer {
	
	 
    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
       
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(HOSTNAME);
        properties.add(IBPFTPTransfer.PORT);
        properties.add(USERNAME);
        properties.add(IBPFTPTransfer.PASSWORD);
        properties.add(REMOTE_PATH);
        properties.add(DISTRIBUTED_CACHE_SERVICE);
        properties.add(IBPFTPTransfer.RECURSIVE_SEARCH);
        properties.add(IBPFTPTransfer.MAX_DEPTH);
        properties.add(IBPFTPTransfer.FILE_FILTER_REGEX);
        properties.add(IBPFTPTransfer.PATH_FILTER_REGEX);
        properties.add(IBPFTPTransfer.IGNORE_DOTTED_FILES);
        properties.add(IBPFTPTransfer.REMOTE_POLL_BATCH_SIZE);
        properties.add(IBPFTPTransfer.CONNECTION_TIMEOUT);
        properties.add(IBPFTPTransfer.DATA_TIMEOUT);
        properties.add(IBPFTPTransfer.CONNECTION_MODE);
        properties.add(IBPFTPTransfer.TRANSFER_MODE);
        properties.add(IBPFTPTransfer.PROXY_TYPE);
        properties.add(IBPFTPTransfer.PROXY_HOST);
        properties.add(IBPFTPTransfer.PROXY_PORT);
        properties.add(IBPFTPTransfer.HTTP_PROXY_USERNAME);
        properties.add(IBPFTPTransfer.HTTP_PROXY_PASSWORD);
        return properties;
    }

    @Override
    protected FileTransfer getFileTransfer(final ProcessContext context) {
        return new IBPFTPTransfer(context, getLogger());
    }

    @Override
    protected String getProtocolName() {
        return "ftp";
    }

    @Override
    protected Scope getStateScope(final ProcessContext context) {
        // Use cluster scope so that component can be run on Primary Node Only and can still
        // pick up where it left off, even if the Primary Node changes.
        return Scope.CLUSTER;
    }
}