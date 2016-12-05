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

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.processors.standard.ListFileTransfer;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Test;

public class TestIBPListFTP {

    @Test
    public void testListFTP()  {

        final TestRunner runner = TestRunners.newTestRunner(new IBPListFTP());
        runner.setValidateExpressionUsage(false);
        runner.setProperty(ListFileTransfer.HOSTNAME, "IBRPT000");
        runner.setProperty(ListFileTransfer.USERNAME, "gestLog");
        runner.setProperty(new PropertyDescriptor.Builder()
                .name("Password")
                .description("Username")
                .expressionLanguageSupported(true)
                .required(true)
                .build(), "gestLog");
        runner.setProperty(IBPFTPTransfer.PORT, "22");
        runner.run(1,true, false);

//        runner.assertAllFlowFilesTransferred(GetFile.REL_SUCCESS, 1);
//        final List<MockFlowFile> successFiles = runner.getFlowFilesForRelationship(GetFile.REL_SUCCESS);
//        successFiles.get(0).assertContentEquals("Hello, World!".getBytes("UTF-8"));
//
//        final String path = successFiles.get(0).getAttribute("path");
//        assertEquals("/", path);
//        final String absolutePath = successFiles.get(0).getAttribute(CoreAttributes.ABSOLUTE_PATH.key());
//        assertEquals(absTargetPathStr, absolutePath);
    }


}
