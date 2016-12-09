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
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Test;

public class TestIBPListFTP {

    @Test
    public void testListFTP()  {

        final TestRunner runner = TestRunners.newTestRunner(new IBPListFTP());
        runner.setValidateExpressionUsage(false);
        runner.setProperty(IBPFTPTransfer.HOSTNAME, "IBRPT000");
        runner.setProperty(IBPFTPTransfer.USERNAME, "gestLog");
        runner.setProperty(new PropertyDescriptor.Builder()
                .name("Password")
                .description("Username")
                .expressionLanguageSupported(true)
                .required(true)
                .build(), "gestLog");
        runner.setProperty(IBPFTPTransfer.PORT, "22");
        runner.setProperty(IBPFTPTransfer.FILE_FILTER_REGEX,"logs_[0-9]{6}\\.zip");
        runner.setProperty(IBPFTPTransfer.PATH_FILTER_REGEX, "^IB(CYC|EQX|WSP|CAU)[0-9]{1}[012345689]{1}[0-9]{1}$");
        runner.setProperty(IBPFTPTransfer.RECURSIVE_SEARCH,"true");
        runner.setProperty(IBPFTPTransfer.MAX_DEPTH, "1");
        runner.run(1,true, false);
    }


}
