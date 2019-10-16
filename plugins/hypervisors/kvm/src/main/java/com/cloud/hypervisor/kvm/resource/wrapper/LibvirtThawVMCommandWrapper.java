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
import com.cloud.agent.api.ThawVMAnswer;
import com.cloud.agent.api.ThawVMCommand;
import com.cloud.hypervisor.kvm.resource.LibvirtComputingResource;
import com.cloud.resource.CommandWrapper;
import com.cloud.resource.ResourceWrapper;
import com.cloud.utils.script.Script;

@ResourceWrapper(handles = ThawVMCommand.class)
public class LibvirtThawVMCommandWrapper extends CommandWrapper<ThawVMCommand, Answer, LibvirtComputingResource>{

    private static final Logger log = Logger.getLogger(LibvirtThawVMCommandWrapper.class);
    @Override
    public Answer execute(ThawVMCommand command, LibvirtComputingResource serverResource) {
        String vmName = command.getVmName();
        Domain domain = null;

       try {
        final LibvirtUtilitiesHelper libvirtUtilitiesHelper = serverResource.getLibvirtUtilitiesHelper();
        Connect connect = libvirtUtilitiesHelper.getConnection();
        domain = serverResource.getDomain(connect, vmName);
        if (domain == null) {
            return new ThawVMAnswer(command, false, "Failed due to vm " + vmName + " was not found");
        }
        Script resumeCommand = new Script("virsh", log );
        resumeCommand.add("domfsthaw", vmName);
        log.info(("Executing config drive creation command: " + resumeCommand.toString()));
        String result = resumeCommand.execute(null);
        log.info("LibvirtThawVMCommandWrapper thaw result:" + result);
        if (!result.equals("0")) {
            return new ThawVMAnswer(command, false, "Failed to thaw vm " + vmName + " due to result status is: " + result);
        }
    } catch (LibvirtException  libvirtException) {
        return new ThawVMAnswer(command, false, "Failed to thaw vm " + vmName + " due to " + libvirtException.getMessage());
    }
        return new ThawVMAnswer(command, true, "Thaw VM success");
    }

}
