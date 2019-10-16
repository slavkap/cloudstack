/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.cloudstack.storage.vmsnapshot;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;
import javax.naming.ConfigurationException;

import org.apache.cloudstack.api.ApiErrorCode;
import org.apache.cloudstack.api.ServerApiException;
import org.apache.cloudstack.engine.subsystem.api.storage.SnapshotDataFactory;
import org.apache.cloudstack.engine.subsystem.api.storage.SnapshotInfo;
import org.apache.cloudstack.engine.subsystem.api.storage.SnapshotStrategy;
import org.apache.cloudstack.engine.subsystem.api.storage.SnapshotStrategy.SnapshotOperation;
import org.apache.cloudstack.engine.subsystem.api.storage.StorageStrategyFactory;
import org.apache.cloudstack.engine.subsystem.api.storage.StrategyPriority;
import org.apache.cloudstack.engine.subsystem.api.storage.VMSnapshotOptions;
import org.apache.cloudstack.engine.subsystem.api.storage.VMSnapshotStrategy;
import org.apache.cloudstack.engine.subsystem.api.storage.VolumeDataFactory;
import org.apache.cloudstack.engine.subsystem.api.storage.VolumeInfo;
import org.apache.cloudstack.framework.config.dao.ConfigurationDao;
import org.apache.cloudstack.storage.to.VolumeObjectTO;
import org.apache.log4j.Logger;

import com.cloud.agent.AgentManager;
import com.cloud.agent.api.Answer;
import com.cloud.agent.api.CreateVMSnapshotAnswer;
import com.cloud.agent.api.CreateVMSnapshotCommand;
import com.cloud.agent.api.DeleteVMSnapshotAnswer;
import com.cloud.agent.api.DeleteVMSnapshotCommand;
import com.cloud.agent.api.FreezeVMAnswer;
import com.cloud.agent.api.FreezeVMCommand;
import com.cloud.agent.api.RevertToVMSnapshotAnswer;
import com.cloud.agent.api.RevertToVMSnapshotCommand;
import com.cloud.agent.api.ThawVMAnswer;
import com.cloud.agent.api.ThawVMCommand;
import com.cloud.agent.api.VMSnapshotTO;
import com.cloud.event.EventTypes;
import com.cloud.event.UsageEventUtils;
import com.cloud.event.UsageEventVO;
import com.cloud.exception.AgentUnavailableException;
import com.cloud.exception.OperationTimedoutException;
import com.cloud.host.HostVO;
import com.cloud.host.dao.HostDao;
import com.cloud.hypervisor.Hypervisor;
import com.cloud.storage.CreateSnapshotPayload;
import com.cloud.storage.DiskOfferingVO;
import com.cloud.storage.GuestOSHypervisorVO;
import com.cloud.storage.GuestOSVO;
import com.cloud.storage.Snapshot;
import com.cloud.storage.SnapshotVO;
import com.cloud.storage.VolumeApiService;
import com.cloud.storage.VolumeVO;
import com.cloud.storage.dao.DiskOfferingDao;
import com.cloud.storage.dao.GuestOSDao;
import com.cloud.storage.dao.GuestOSHypervisorDao;
import com.cloud.storage.dao.SnapshotDao;
import com.cloud.storage.dao.VolumeDao;
import com.cloud.storage.snapshot.SnapshotApiService;
import com.cloud.user.AccountService;
import com.cloud.uservm.UserVm;
import com.cloud.utils.NumbersUtil;
import com.cloud.utils.component.ManagerBase;
import com.cloud.utils.db.DB;
import com.cloud.utils.db.SearchBuilder;
import com.cloud.utils.db.SearchCriteria;
import com.cloud.utils.db.SearchCriteria.Op;
import com.cloud.utils.db.Transaction;
import com.cloud.utils.db.TransactionCallbackWithExceptionNoReturn;
import com.cloud.utils.db.TransactionStatus;
import com.cloud.utils.exception.CloudRuntimeException;
import com.cloud.utils.fsm.NoTransitionException;
import com.cloud.vm.UserVmVO;
import com.cloud.vm.dao.UserVmDao;
import com.cloud.vm.snapshot.VMSnapshot;
import com.cloud.vm.snapshot.VMSnapshotVO;
import com.cloud.vm.snapshot.dao.VMSnapshotDao;

public class DefaultVMSnapshotStrategy extends ManagerBase implements VMSnapshotStrategy {
    private static final Logger s_logger = Logger.getLogger(DefaultVMSnapshotStrategy.class);
    @Inject
    VMSnapshotHelper vmSnapshotHelper;
    @Inject
    GuestOSDao guestOSDao;
    @Inject
    GuestOSHypervisorDao guestOsHypervisorDao;
    @Inject
    UserVmDao userVmDao;
    @Inject
    VMSnapshotDao vmSnapshotDao;
    int _wait;
    @Inject
    ConfigurationDao configurationDao;
    @Inject
    AgentManager agentMgr;
    @Inject
    VolumeDao volumeDao;
    @Inject
    DiskOfferingDao diskOfferingDao;
    @Inject
    HostDao hostDao;
    @Inject
    VolumeApiService _volumeService;
    @Inject
    AccountService _accountService;
    @Inject
    VolumeDataFactory volumeDataFactory;
    @Inject
    SnapshotApiService _snapshotService;
    @Inject
    SnapshotDao _snapshotDao;
    @Inject
    StorageStrategyFactory _storageStrategyFactory;
    @Inject
    SnapshotDataFactory _snapshotDataFactory;

    @Override
    public boolean configure(String name, Map<String, Object> params) throws ConfigurationException {
        String value = configurationDao.getValue("vmsnapshot.create.wait");
        _wait = NumbersUtil.parseInt(value, 1800);
        return true;
    }

    @Override
    public VMSnapshot takeVMSnapshot(VMSnapshot vmSnapshot) {
        Long hostId = vmSnapshotHelper.pickRunningHost(vmSnapshot.getVmId());
        UserVm userVm = userVmDao.findById(vmSnapshot.getVmId());
        VMSnapshotVO vmSnapshotVO = (VMSnapshotVO)vmSnapshot;
        boolean isKVMsnapshotsEnabled = Boolean.parseBoolean(configurationDao.getValue("kvm.vmsnapshot.enabled"));

        try {
            if (userVm.getHypervisorType() ==Hypervisor.HypervisorType.KVM && vmSnapshotVO.getType().equals(VMSnapshot.Type.Disk) && isKVMsnapshotsEnabled) {
                vmSnapshotHelper.vmSnapshotStateTransitTo(vmSnapshotVO, VMSnapshot.Event.KVMCreateRequested);
            }else {
                vmSnapshotHelper.vmSnapshotStateTransitTo(vmSnapshotVO, VMSnapshot.Event.CreateRequested);
            }
        } catch (NoTransitionException e) {
            throw new CloudRuntimeException(e.getMessage());
        }

        CreateVMSnapshotAnswer answer = null;
        boolean result = false;
        try {
            GuestOSVO guestOS = guestOSDao.findById(userVm.getGuestOSId());

            List<VolumeObjectTO> volumeTOs = vmSnapshotHelper.getVolumeTOList(userVm.getId());

            long prev_chain_size = 0;
            long virtual_size=0;
            for (VolumeObjectTO volume : volumeTOs) {
                virtual_size += volume.getSize();
                VolumeVO volumeVO = volumeDao.findById(volume.getId());
                prev_chain_size += volumeVO.getVmSnapshotChainSize() == null ? 0 : volumeVO.getVmSnapshotChainSize();
            }

            VMSnapshotTO current = null;
            VMSnapshotVO currentSnapshot = vmSnapshotDao.findCurrentSnapshotByVmId(userVm.getId());
            if (currentSnapshot != null)
                current = vmSnapshotHelper.getSnapshotWithParents(currentSnapshot);
            VMSnapshotOptions options = ((VMSnapshotVO)vmSnapshot).getOptions();
            boolean quiescevm = true;
            if (options != null)
                quiescevm = options.needQuiesceVM();
            VMSnapshotTO target =
                new VMSnapshotTO(vmSnapshot.getId(), vmSnapshot.getName(), vmSnapshot.getType(), null, vmSnapshot.getDescription(), false, current, quiescevm);
            if (current == null)
                vmSnapshotVO.setParent(null);
            else
                vmSnapshotVO.setParent(current.getId());

            HostVO host = hostDao.findById(hostId);
            GuestOSHypervisorVO guestOsMapping = guestOsHypervisorDao.findByOsIdAndHypervisor(guestOS.getId(), host.getHypervisorType().toString(), host.getHypervisorVersion());

            CreateVMSnapshotCommand ccmd = new CreateVMSnapshotCommand(userVm.getInstanceName(), userVm.getUuid(), target, volumeTOs, guestOS.getDisplayName());
            if (userVm.getHypervisorType().equals(Hypervisor.HypervisorType.KVM)
                    && vmSnapshotVO.getType().equals(VMSnapshot.Type.Disk) && isKVMsnapshotsEnabled) {
                s_logger.info("Creating VM snapshot for KVM hypervisor without memory");

                FreezeVMCommand freezeCommand = new FreezeVMCommand(userVm.getInstanceName());
                FreezeVMAnswer freezeAnswer = (FreezeVMAnswer) agentMgr.send(hostId, freezeCommand);
                if (freezeAnswer != null && freezeAnswer.getResult()) {
                    s_logger.info("The virtual machine is frozen");
                    ThawVMCommand freeCmd = new ThawVMCommand(userVm.getInstanceName());
                    ThawVMAnswer answerThaw = null;
                    List<VolumeInfo> vinfos = new ArrayList<>();
                    List<SnapshotInfo> forRollback = new ArrayList<>();
                    for (VolumeObjectTO volumeObjectTO : volumeTOs) {
                        vinfos.add(volumeDataFactory.getVolume(volumeObjectTO.getId()));
                    }
                    try {
                        for (VolumeInfo vol : vinfos) {
                            try {
                                    String snapshotName = vmSnapshot.getUuid() + "_" + vol.getUuid();
                                    SnapshotVO snapshotCreate = new SnapshotVO(vol.getDataCenterId(), vol.getAccountId(), vol.getDomainId(), vol.getId(), vol.getDiskOfferingId(),
                                            snapshotName, (short) SnapshotVO.MANUAL_POLICY_ID, "MANUAL", vol.getSize(), vol.getMinIops(), vol.getMaxIops(), Hypervisor.HypervisorType.KVM, null);
                                    snapshotCreate.setState(Snapshot.State.AllocatedKVM);
                                    SnapshotVO snapshot = _snapshotDao.persist(snapshotCreate);
                                    vol.addPayload(setPayload(quiescevm, vol, snapshotCreate, snapshot));
                                    SnapshotInfo snapshotInfo = _snapshotDataFactory.getSnapshot(snapshot.getId(), vol.getDataStore());
                                    snapshotInfo.addPayload(vol.getpayload());
                                    SnapshotStrategy  snapshotStrategy = _storageStrategyFactory.getSnapshotStrategy(snapshotInfo, SnapshotOperation.TAKE);
                                    if (snapshotStrategy ==null) {
                                        throw new CloudRuntimeException("Could not find strategy for snapshot uuid:" + snapshotInfo.getUuid());
                                    }
                                    SnapshotInfo snapInfo = snapshotStrategy.takeSnapshot(snapshotInfo);
                                    if (snapInfo == null) {
                                        throw new CloudRuntimeException("Failed to create snapshot");
                                    }else {
                                        forRollback.add(snapInfo);
                                    }
                                    s_logger.info("Created snapshot for volume with snapshot ID: " + snapInfo.getId());
                                } catch (CloudRuntimeException e) {
                                    for (SnapshotInfo snapshotInfo : forRollback) {
                                        Long snapshotID =snapshotInfo.getId();
                                        SnapshotVO snapshot = _snapshotDao.findById(snapshotID);
                                        snapshot.setState(Snapshot.State.BackedUp);
                                        _snapshotDao.persist(snapshot);
                                        _snapshotService.deleteSnapshot(snapshot.getId());
                                        s_logger.debug("Deleting snapshot with id:" + snapshotID);
                                    }
                                    throw new CloudRuntimeException("Could not create snapshot for VM snapshot" + e.getMessage());
                                }
                        }
                        answer = new CreateVMSnapshotAnswer(ccmd, true, "");
                        answer.setVolumeTOs(volumeTOs);
                        answerThaw = (ThawVMAnswer) agentMgr.send(hostId, freeCmd);
                        if (answerThaw != null && answerThaw.getResult()) {
                            s_logger.info("Thaw vm answe:" + answerThaw.getDetails());
                            for (VolumeInfo vol : vinfos) {
                                CreateSnapshotPayload payload = (CreateSnapshotPayload) vol.getpayload();
                                Long snapshotId = payload.getSnapshotId();
                                SnapshotInfo snapshotInfo = _snapshotDataFactory.getSnapshot(snapshotId, vol.getDataStore());
                                snapshotInfo.addPayload(vol.getpayload());
                                try {
                                    SnapshotStrategy snapshotStrategy = _storageStrategyFactory.getSnapshotStrategy(snapshotInfo, SnapshotOperation.TAKE);
                                    SnapshotInfo snInfo = snapshotStrategy.backupSnapshot(snapshotInfo);
                                    if (snInfo ==null) {
                                        throw new CloudRuntimeException("Could not backup snapshot for volume");
                                    }
                                    s_logger.info(String.format("Backedup snapshot with id=%s, path=%s", snInfo.getId(), snInfo.getPath()));
                                } catch (Exception e) {
                                    for (SnapshotInfo snapshotIn : forRollback) {
                                        Long snapshotID =snapshotIn.getId();
                                        SnapshotVO snapshot = _snapshotDao.findById(snapshotID);
                                        snapshot.setState(Snapshot.State.BackedUp);
                                        _snapshotDao.persist(snapshot);
                                        _snapshotService.deleteSnapshot(snapshot.getId());
                                        s_logger.debug("Deleting snapshot with id:" + snapshotID);
                                    }
                                    throw new CloudRuntimeException("Could not backup snapshot for volume " + e.getMessage());
                                }
                            }
                        }
                    } catch (CloudRuntimeException e) {
                        throw new CloudRuntimeException("Could not create vm snapsot for " + vmSnapshot.getName());
                    } finally {
                        if(answerThaw == null) {
                            answerThaw =  (ThawVMAnswer) agentMgr.send(hostId, freeCmd);
                        }
                    }
                } else {
                    throw new CloudRuntimeException("Could not freeze VM." + freezeAnswer.getDetails());
                }
            } else {
                if (guestOsMapping == null) {
                    ccmd.setPlatformEmulator(null);
                } else {
                    ccmd.setPlatformEmulator(guestOsMapping.getGuestOsName());
                }
                ccmd.setWait(_wait);

                answer = (CreateVMSnapshotAnswer) agentMgr.send(hostId, ccmd);
            }
            if (answer != null && answer.getResult()) {
                processAnswer(vmSnapshotVO, userVm, answer, hostId);
                s_logger.debug("Create vm snapshot " + vmSnapshot.getName() + " succeeded for vm: " + userVm.getInstanceName());
                result = true;
                long new_chain_size=0;
                for (VolumeObjectTO volumeTo : answer.getVolumeTOs()) {
                    publishUsageEvent(EventTypes.EVENT_VM_SNAPSHOT_CREATE, vmSnapshot, userVm, volumeTo);
                    new_chain_size += volumeTo.getSize();
                }
                publishUsageEvent(EventTypes.EVENT_VM_SNAPSHOT_ON_PRIMARY, vmSnapshot, userVm, new_chain_size - prev_chain_size, virtual_size);
                return vmSnapshot;
            } else {
                String errMsg = "Creating VM snapshot: " + vmSnapshot.getName() + " failed";
                if (answer != null && answer.getDetails() != null)
                    errMsg = errMsg + " due to " + answer.getDetails();
                s_logger.error(errMsg);
                throw new CloudRuntimeException(errMsg);
            }
        } catch (OperationTimedoutException e) {
            s_logger.debug("Creating VM snapshot: " + vmSnapshot.getName() + " failed: " + e.toString());
            throw new CloudRuntimeException("Creating VM snapshot: " + vmSnapshot.getName() + " failed: " + e.toString());
        } catch (AgentUnavailableException e) {
            s_logger.debug("Creating VM snapshot: " + vmSnapshot.getName() + " failed", e);
            throw new CloudRuntimeException("Creating VM snapshot: " + vmSnapshot.getName() + " failed: " + e.toString());
        } finally {
            if (!result) {
                try {
                    vmSnapshotHelper.vmSnapshotStateTransitTo(vmSnapshot, VMSnapshot.Event.OperationFailed);
                } catch (NoTransitionException e1) {
                    s_logger.error("Cannot set vm snapshot state due to: " + e1.getMessage());
                }
            }
        }
    }

    @Override
    public boolean deleteVMSnapshot(VMSnapshot vmSnapshot) {
        UserVmVO userVm = userVmDao.findById(vmSnapshot.getVmId());
        VMSnapshotVO vmSnapshotVO = (VMSnapshotVO)vmSnapshot;
        try {
            vmSnapshotHelper.vmSnapshotStateTransitTo(vmSnapshot, VMSnapshot.Event.ExpungeRequested);
        } catch (NoTransitionException e) {
            s_logger.debug("Failed to change vm snapshot state with event ExpungeRequested");
            throw new CloudRuntimeException("Failed to change vm snapshot state with event ExpungeRequested: " + e.getMessage());
        }

        try {
            Long hostId = vmSnapshotHelper.pickRunningHost(vmSnapshot.getVmId());

            List<VolumeObjectTO> volumeTOs = vmSnapshotHelper.getVolumeTOList(vmSnapshot.getVmId());

            String vmInstanceName = userVm.getInstanceName();
            VMSnapshotTO parent = vmSnapshotHelper.getSnapshotWithParents(vmSnapshotVO).getParent();
            VMSnapshotTO vmSnapshotTO =
                new VMSnapshotTO(vmSnapshot.getId(), vmSnapshot.getName(), vmSnapshot.getType(), vmSnapshot.getCreated().getTime(), vmSnapshot.getDescription(),
                    vmSnapshot.getCurrent(), parent, true);
            GuestOSVO guestOS = guestOSDao.findById(userVm.getGuestOSId());
            DeleteVMSnapshotCommand deleteSnapshotCommand = new DeleteVMSnapshotCommand(vmInstanceName, vmSnapshotTO, volumeTOs, guestOS.getDisplayName());

            Answer answer = null;

            boolean isKVMsnapshotsEnabled = Boolean.parseBoolean(configurationDao.getValue("kvm.vmsnapshot.enabled"));
            if (userVm.getHypervisorType() == Hypervisor.HypervisorType.KVM  && vmSnapshotVO.getType().equals(VMSnapshot.Type.Disk) && isKVMsnapshotsEnabled) {
                List<VolumeInfo> volumeInfos = new ArrayList<>();
                for (VolumeObjectTO volumeObjectTO : volumeTOs) {
                    volumeInfos.add(volumeDataFactory.getVolume(volumeObjectTO.getId()));
                }
                for (VolumeInfo vol : volumeInfos) {
                    try {
                        String snapshotName = vmSnapshot.getUuid() + "_" + vol.getUuid();
                        SnapshotVO snapshot = findSnapshotByName(snapshotName);

                        if (snapshot == null) {
                            throw new CloudRuntimeException("Could not delete snapshot for VM snapshot");
                        }

                        snapshot.setState(Snapshot.State.BackedUp);
                        _snapshotDao.persist(snapshot);
                        boolean snapshotForDelete = _snapshotService.deleteSnapshot(snapshot.getId());
                        if (!snapshotForDelete) {
                            throw new ServerApiException(ApiErrorCode.INTERNAL_ERROR, "Failed to delete snapshot");
                        }
                    } catch (CloudRuntimeException e) {
                        throw new CloudRuntimeException("Could not delete snapshot for VM snapshot" + e.getMessage());
                    }
                }
                answer = new DeleteVMSnapshotAnswer(deleteSnapshotCommand, volumeTOs);
            } else {
                answer = agentMgr.send(hostId, deleteSnapshotCommand);
            }
                if (answer != null && answer.getResult()) {
                    DeleteVMSnapshotAnswer deleteVMSnapshotAnswer = (DeleteVMSnapshotAnswer) answer;
                    processAnswer(vmSnapshotVO, userVm, answer, hostId);
                    long full_chain_size = 0;
                    for (VolumeObjectTO volumeTo : deleteVMSnapshotAnswer.getVolumeTOs()) {
                        publishUsageEvent(EventTypes.EVENT_VM_SNAPSHOT_DELETE, vmSnapshot, userVm, volumeTo);
                        full_chain_size += volumeTo.getSize();
                    }
                    publishUsageEvent(EventTypes.EVENT_VM_SNAPSHOT_OFF_PRIMARY, vmSnapshot, userVm, full_chain_size, 0L);
                    return true;
                } else {
                    String errMsg = (answer == null) ? null : answer.getDetails();
                    s_logger.error("Delete vm snapshot " + vmSnapshot.getName() + " of vm " + userVm.getInstanceName() + " failed due to " + errMsg);
                    throw new CloudRuntimeException("Delete vm snapshot " + vmSnapshot.getName() + " of vm " + userVm.getInstanceName() + " failed due to " + errMsg);
                }
        } catch (OperationTimedoutException e) {
            throw new CloudRuntimeException("Delete vm snapshot " + vmSnapshot.getName() + " of vm " + userVm.getInstanceName() + " failed due to " + e.getMessage());
        } catch (AgentUnavailableException e) {
            throw new CloudRuntimeException("Delete vm snapshot " + vmSnapshot.getName() + " of vm " + userVm.getInstanceName() + " failed due to " + e.getMessage());
        }
    }

    @DB
    protected void processAnswer(final VMSnapshotVO vmSnapshot, UserVm userVm, final Answer as, Long hostId) {
        try {
            Transaction.execute(new TransactionCallbackWithExceptionNoReturn<NoTransitionException>() {
                @Override
                public void doInTransactionWithoutResult(TransactionStatus status) throws NoTransitionException {
                    if (as instanceof CreateVMSnapshotAnswer) {
                        CreateVMSnapshotAnswer answer = (CreateVMSnapshotAnswer)as;
                        finalizeCreate(vmSnapshot, answer.getVolumeTOs());
                        vmSnapshotHelper.vmSnapshotStateTransitTo(vmSnapshot, VMSnapshot.Event.OperationSucceeded);
                    } else if (as instanceof RevertToVMSnapshotAnswer) {
                        RevertToVMSnapshotAnswer answer = (RevertToVMSnapshotAnswer)as;
                        finalizeRevert(vmSnapshot, answer.getVolumeTOs());
                        vmSnapshotHelper.vmSnapshotStateTransitTo(vmSnapshot, VMSnapshot.Event.OperationSucceeded);
                    } else if (as instanceof DeleteVMSnapshotAnswer) {
                        DeleteVMSnapshotAnswer answer = (DeleteVMSnapshotAnswer)as;
                        finalizeDelete(vmSnapshot, answer.getVolumeTOs());
                        vmSnapshotDao.remove(vmSnapshot.getId());
                    }
                }
            });
        } catch (Exception e) {
            String errMsg = "Error while process answer: " + as.getClass() + " due to " + e.getMessage();
            s_logger.error(errMsg, e);
            throw new CloudRuntimeException(errMsg);
        }
    }

    protected void finalizeDelete(VMSnapshotVO vmSnapshot, List<VolumeObjectTO> volumeTOs) {
        // update volumes paths
        updateVolumePath(volumeTOs);

        // update children's parent snapshots
        List<VMSnapshotVO> children = vmSnapshotDao.listByParent(vmSnapshot.getId());
        for (VMSnapshotVO child : children) {
            child.setParent(vmSnapshot.getParent());
            vmSnapshotDao.persist(child);
        }

        // update current snapshot
        VMSnapshotVO current = vmSnapshotDao.findCurrentSnapshotByVmId(vmSnapshot.getVmId());
        if (current != null && current.getId() == vmSnapshot.getId() && vmSnapshot.getParent() != null) {
            VMSnapshotVO parent = vmSnapshotDao.findById(vmSnapshot.getParent());
            parent.setCurrent(true);
            vmSnapshotDao.persist(parent);
        }
        vmSnapshot.setCurrent(false);
        vmSnapshotDao.persist(vmSnapshot);
    }

    protected void finalizeCreate(VMSnapshotVO vmSnapshot, List<VolumeObjectTO> volumeTOs) {
        // update volumes path
        updateVolumePath(volumeTOs);

        vmSnapshot.setCurrent(true);

        // change current snapshot
        if (vmSnapshot.getParent() != null) {
            VMSnapshotVO previousCurrent = vmSnapshotDao.findById(vmSnapshot.getParent());
            previousCurrent.setCurrent(false);
            vmSnapshotDao.persist(previousCurrent);
        }
        vmSnapshotDao.persist(vmSnapshot);
    }

    protected void finalizeRevert(VMSnapshotVO vmSnapshot, List<VolumeObjectTO> volumeToList) {
        // update volumes path
        updateVolumePath(volumeToList);

        // update current snapshot, current snapshot is the one reverted to
        VMSnapshotVO previousCurrent = vmSnapshotDao.findCurrentSnapshotByVmId(vmSnapshot.getVmId());
        if (previousCurrent != null) {
            previousCurrent.setCurrent(false);
            vmSnapshotDao.persist(previousCurrent);
        }
        vmSnapshot.setCurrent(true);
        vmSnapshotDao.persist(vmSnapshot);
    }

    private void updateVolumePath(List<VolumeObjectTO> volumeTOs) {
        for (VolumeObjectTO volume : volumeTOs) {
            if (volume.getPath() != null) {
                VolumeVO volumeVO = volumeDao.findById(volume.getId());
                volumeVO.setPath(volume.getPath());
                volumeVO.setVmSnapshotChainSize(volume.getSize());
                volumeDao.persist(volumeVO);
            }
        }
    }

    private void publishUsageEvent(String type, VMSnapshot vmSnapshot, UserVm userVm, VolumeObjectTO volumeTo) {
        VolumeVO volume = volumeDao.findById(volumeTo.getId());
        Long diskOfferingId = volume.getDiskOfferingId();
        Long offeringId = null;
        if (diskOfferingId != null) {
            DiskOfferingVO offering = diskOfferingDao.findById(diskOfferingId);
            if (offering != null && (offering.getType() == DiskOfferingVO.Type.Disk)) {
                offeringId = offering.getId();
            }
        }
        Map<String, String> details = new HashMap<>();
        if (vmSnapshot != null) {
            details.put(UsageEventVO.DynamicParameters.vmSnapshotId.name(), String.valueOf(vmSnapshot.getId()));
        }
        UsageEventUtils.publishUsageEvent(type, vmSnapshot.getAccountId(), userVm.getDataCenterId(), userVm.getId(), vmSnapshot.getName(), offeringId, volume.getId(), // save volume's id into templateId field
                volumeTo.getSize(), VMSnapshot.class.getName(), vmSnapshot.getUuid(), details);
    }

    private void publishUsageEvent(String type, VMSnapshot vmSnapshot, UserVm userVm, Long vmSnapSize, Long virtualSize) {
        try {
            Map<String, String> details = new HashMap<>();
            if (vmSnapshot != null) {
                details.put(UsageEventVO.DynamicParameters.vmSnapshotId.name(), String.valueOf(vmSnapshot.getId()));
            }
            UsageEventUtils.publishUsageEvent(type, vmSnapshot.getAccountId(), userVm.getDataCenterId(), userVm.getId(), vmSnapshot.getName(), 0L, 0L, vmSnapSize, virtualSize,
                    VMSnapshot.class.getName(), vmSnapshot.getUuid(), details);
        } catch (Exception e) {
            s_logger.error("Failed to publis usage event " + type, e);
        }
    }

    @Override
    public boolean revertVMSnapshot(VMSnapshot vmSnapshot) {
        VMSnapshotVO vmSnapshotVO = (VMSnapshotVO)vmSnapshot;
        UserVmVO userVm = userVmDao.findById(vmSnapshot.getVmId());
        try {
            vmSnapshotHelper.vmSnapshotStateTransitTo(vmSnapshotVO, VMSnapshot.Event.RevertRequested);
        } catch (NoTransitionException e) {
            throw new CloudRuntimeException(e.getMessage());
        }

        boolean result = false;
        try {
            VMSnapshotVO snapshot = vmSnapshotDao.findById(vmSnapshotVO.getId());
            List<VolumeObjectTO> volumeTOs = vmSnapshotHelper.getVolumeTOList(userVm.getId());
            String vmInstanceName = userVm.getInstanceName();
            VMSnapshotTO parent = vmSnapshotHelper.getSnapshotWithParents(snapshot).getParent();

            VMSnapshotTO vmSnapshotTO =
                new VMSnapshotTO(snapshot.getId(), snapshot.getName(), snapshot.getType(), snapshot.getCreated().getTime(), snapshot.getDescription(),
                    snapshot.getCurrent(), parent, true);
            Long hostId = vmSnapshotHelper.pickRunningHost(vmSnapshot.getVmId());
            GuestOSVO guestOS = guestOSDao.findById(userVm.getGuestOSId());
            RevertToVMSnapshotCommand revertToSnapshotCommand = new RevertToVMSnapshotCommand(vmInstanceName, userVm.getUuid(), vmSnapshotTO, volumeTOs, guestOS.getDisplayName());
            HostVO host = hostDao.findById(hostId);
            GuestOSHypervisorVO guestOsMapping = guestOsHypervisorDao.findByOsIdAndHypervisor(guestOS.getId(), host.getHypervisorType().toString(), host.getHypervisorVersion());
            if (guestOsMapping == null) {
                revertToSnapshotCommand.setPlatformEmulator(null);
            } else {
                revertToSnapshotCommand.setPlatformEmulator(guestOsMapping.getGuestOsName());
            }
            //For KVM VM snapshots without memory and the disk could be in different storage providers
            boolean isKVMsnapshotsEnabled = Boolean.parseBoolean(configurationDao.getValue("kvm.vmsnapshot.enabled"));

            RevertToVMSnapshotAnswer answer= null;
            s_logger.info("KVM snapshot is enabled " + isKVMsnapshotsEnabled);
            if (userVm.getHypervisorType().equals(Hypervisor.HypervisorType.KVM) && vmSnapshotVO.getType().equals(VMSnapshot.Type.Disk) && isKVMsnapshotsEnabled) {
                List<VolumeInfo> volumeInfos = new ArrayList<>();
                for (VolumeObjectTO volumeObjectTO : volumeTOs) {
                    volumeInfos.add(volumeDataFactory.getVolume(volumeObjectTO.getId()));
                }
                for (VolumeInfo vol : volumeInfos) {
                    try {
                        String snapshotName = vmSnapshot.getUuid() + "_" + vol.getUuid();
                        s_logger.info("snapshot name " + snapshotName);
                        SnapshotVO snap = findSnapshotByName(snapshotName);
                        Snapshot snap2 = _snapshotService.revertSnapshot(snap.getId());
                        if (snap2 == null) {
                            throw new ServerApiException(ApiErrorCode.INTERNAL_ERROR, "Failed to revert snapshot");
                        }
                    } catch (Exception e) {
                        throw new CloudRuntimeException("Could not revert snapshot for VM snapshot" + e.getMessage());
                    }
                }
                answer = new RevertToVMSnapshotAnswer(revertToSnapshotCommand, true, "");
                answer.setVolumeTOs(volumeTOs);
                if (answer ==null || !answer.getResult()) {
                    throw new CloudRuntimeException("Could not create vm snapsot for " + vmSnapshot.getName());
                }
            }else {
                answer = (RevertToVMSnapshotAnswer)agentMgr.send(hostId, revertToSnapshotCommand);
            }
            if (answer != null && answer.getResult()) {
                processAnswer(vmSnapshotVO, userVm, answer, hostId);
                result = true;
            } else {
                String errMsg = "Revert VM: " + userVm.getInstanceName() + " to snapshot: " + vmSnapshotVO.getName() + " failed";
                if (answer != null && answer.getDetails() != null)
                    errMsg = errMsg + " due to " + answer.getDetails();
                s_logger.error(errMsg);
                throw new CloudRuntimeException(errMsg);
            }
        } catch (OperationTimedoutException e) {
            s_logger.debug("Failed to revert vm snapshot", e);
            throw new CloudRuntimeException(e.getMessage());
        } catch (AgentUnavailableException e) {
            s_logger.debug("Failed to revert vm snapshot", e);
            throw new CloudRuntimeException(e.getMessage());
        } finally {
            if (!result) {
                try {
                    vmSnapshotHelper.vmSnapshotStateTransitTo(vmSnapshot, VMSnapshot.Event.OperationFailed);
                } catch (NoTransitionException e1) {
                    s_logger.error("Cannot set vm snapshot state due to: " + e1.getMessage());
                }
            }
        }
        return result;
    }

    private SnapshotVO findSnapshotByName(String snapshotName) {
        SearchBuilder<SnapshotVO> sb = _snapshotDao.createSearchBuilder();
        SearchCriteria<SnapshotVO> sc = sb.create();
        sc.addAnd("name", Op.EQ, snapshotName);
        SnapshotVO snap = _snapshotDao.findOneBy(sc);
        return snap;
    }

    @Override
    public StrategyPriority canHandle(VMSnapshot vmSnapshot) {
        return StrategyPriority.DEFAULT;
    }

    @Override
    public boolean deleteVMSnapshotFromDB(VMSnapshot vmSnapshot) {
        try {
            vmSnapshotHelper.vmSnapshotStateTransitTo(vmSnapshot, VMSnapshot.Event.ExpungeRequested);
        } catch (NoTransitionException e) {
            s_logger.debug("Failed to change vm snapshot state with event ExpungeRequested");
            throw new CloudRuntimeException("Failed to change vm snapshot state with event ExpungeRequested: " + e.getMessage());
        }
        UserVm userVm = userVmDao.findById(vmSnapshot.getVmId());
        List<VolumeObjectTO> volumeTOs = vmSnapshotHelper.getVolumeTOList(userVm.getId());
        for (VolumeObjectTO volumeTo: volumeTOs) {
            volumeTo.setSize(0);
            publishUsageEvent(EventTypes.EVENT_VM_SNAPSHOT_DELETE, vmSnapshot, userVm, volumeTo);
        }
        return vmSnapshotDao.remove(vmSnapshot.getId());
    }

    private CreateSnapshotPayload setPayload(boolean quiescevm, VolumeInfo vol, SnapshotVO snapshotCreate,
            SnapshotVO snapshot) {
        CreateSnapshotPayload payload = new CreateSnapshotPayload();
        payload.setSnapshotId(snapshot.getId());
        payload.setSnapshotPolicyId(SnapshotVO.MANUAL_POLICY_ID);
        payload.setLocationType(snapshotCreate.getLocationType());
        payload.setAccount(_accountService.getAccount(vol.getAccountId()));
        payload.setAsyncBackup(false);
        payload.setQuiescevm(quiescevm);
        return payload;
    }
}
