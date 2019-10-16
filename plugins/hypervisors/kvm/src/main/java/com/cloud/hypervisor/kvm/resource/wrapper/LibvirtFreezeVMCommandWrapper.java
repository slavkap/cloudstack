//
// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
//

package com.cloud.hypervisor.kvm.resource.wrapper;

import org.apache.log4j.Logger;
import org.libvirt.Connect;
import org.libvirt.Domain;
import org.libvirt.LibvirtException;

import com.cloud.agent.api.Answer;
import com.cloud.agent.api.FreezeVMAnswer;
import com.cloud.agent.api.FreezeVMCommand;
import com.cloud.hypervisor.kvm.resource.LibvirtComputingResource;
import com.cloud.resource.CommandWrapper;
import com.cloud.resource.ResourceWrapper;
import com.cloud.utils.script.Script;

@ResourceWrapper(handles = FreezeVMCommand.class)
public class LibvirtFreezeVMCommandWrapper
        extends CommandWrapper<FreezeVMCommand, Answer, LibvirtComputingResource> {

    private static final Logger log = Logger.getLogger(LibvirtFreezeVMCommandWrapper.class);

    @Override
    public Answer execute(FreezeVMCommand command, LibvirtComputingResource serverResource) {
        String vmName = command.getVmName();
        Domain domain = null;

        try {
            final LibvirtUtilitiesHelper libvirtUtilitiesHelper = serverResource.getLibvirtUtilitiesHelper();
            Connect connection = libvirtUtilitiesHelper.getConnection();
            domain = serverResource.getDomain(connection, vmName);
            if (domain == null) {
                return new FreezeVMAnswer(command, false,
                        "Failed due to vm " + vmName + " was not found");
            }
            Script freeze = new Script("virsh");
            freeze.add("domfsfreeze", vmName);
            log.info(("Executing config drive creation command: " + freeze.toString()));
            String result = freeze.execute(null);
            log.info("LibvirtFreezeVMCommandWrapper Freeze result status is: " + result);
            if (!result.equals("0")) {
                return new FreezeVMAnswer(command, false,
                        "Failed to freeze due to result status:" + result);
            }
        } catch (LibvirtException libvirtException) {
            return new FreezeVMAnswer(command, false,
                    "Failed to freeze due to " + libvirtException.getMessage());
        }
        return new FreezeVMAnswer(command, true, "VM is suspended");
    }
}
