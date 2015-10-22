/*
 * Hops Database abstraction layer for storing the hops metadata in MySQL Cluster
 * Copyright (C) 2015  hops.io
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */
package io.hops.metadata.ndb;

import io.hops.DalStorageFactory;
import io.hops.StorageConnector;
import io.hops.exception.StorageInitializtionException;
import io.hops.metadata.common.EntityDataAccess;
import io.hops.metadata.election.dal.HdfsLeDescriptorDataAccess;
import io.hops.metadata.election.dal.YarnLeDescriptorDataAccess;
import io.hops.metadata.hdfs.dal.AccessTimeLogDataAccess;
import io.hops.metadata.hdfs.dal.BlockChecksumDataAccess;
import io.hops.metadata.hdfs.dal.BlockInfoDataAccess;
import io.hops.metadata.hdfs.dal.BlockLookUpDataAccess;
import io.hops.metadata.hdfs.dal.CorruptReplicaDataAccess;
import io.hops.metadata.hdfs.dal.EncodingJobsDataAccess;
import io.hops.metadata.hdfs.dal.EncodingStatusDataAccess;
import io.hops.metadata.hdfs.dal.ExcessReplicaDataAccess;
import io.hops.metadata.hdfs.dal.INodeAttributesDataAccess;
import io.hops.metadata.hdfs.dal.INodeDataAccess;
import io.hops.metadata.hdfs.dal.InvalidateBlockDataAccess;
import io.hops.metadata.hdfs.dal.LeaseDataAccess;
import io.hops.metadata.hdfs.dal.LeasePathDataAccess;
import io.hops.metadata.hdfs.dal.MetadataLogDataAccess;
import io.hops.metadata.hdfs.dal.MisReplicatedRangeQueueDataAccess;
import io.hops.metadata.hdfs.dal.OngoingSubTreeOpsDataAccess;
import io.hops.metadata.hdfs.dal.PendingBlockDataAccess;
import io.hops.metadata.hdfs.dal.QuotaUpdateDataAccess;
import io.hops.metadata.hdfs.dal.RepairJobsDataAccess;
import io.hops.metadata.hdfs.dal.ReplicaDataAccess;
import io.hops.metadata.hdfs.dal.ReplicaUnderConstructionDataAccess;
import io.hops.metadata.hdfs.dal.SafeBlocksDataAccess;
import io.hops.metadata.hdfs.dal.SizeLogDataAccess;
import io.hops.metadata.hdfs.dal.StorageIdMapDataAccess;
import io.hops.metadata.hdfs.dal.UnderReplicatedBlockDataAccess;
import io.hops.metadata.hdfs.dal.VariableDataAccess;
import io.hops.metadata.ndb.dalimpl.election.HdfsLeaderClusterj;
import io.hops.metadata.ndb.dalimpl.election.YarnLeaderClusterj;
import io.hops.metadata.ndb.dalimpl.hdfs.AccessTimeLogClusterj;
import io.hops.metadata.ndb.dalimpl.hdfs.BlockChecksumClusterj;
import io.hops.metadata.ndb.dalimpl.hdfs.BlockInfoClusterj;
import io.hops.metadata.ndb.dalimpl.hdfs.BlockLookUpClusterj;
import io.hops.metadata.ndb.dalimpl.hdfs.CorruptReplicaClusterj;
import io.hops.metadata.ndb.dalimpl.hdfs.EncodingJobsClusterj;
import io.hops.metadata.ndb.dalimpl.hdfs.EncodingStatusClusterj;
import io.hops.metadata.ndb.dalimpl.hdfs.ExcessReplicaClusterj;
import io.hops.metadata.ndb.dalimpl.hdfs.INodeAttributesClusterj;
import io.hops.metadata.ndb.dalimpl.hdfs.INodeClusterj;
import io.hops.metadata.ndb.dalimpl.hdfs.InvalidatedBlockClusterj;
import io.hops.metadata.ndb.dalimpl.hdfs.LeaseClusterj;
import io.hops.metadata.ndb.dalimpl.hdfs.LeasePathClusterj;
import io.hops.metadata.ndb.dalimpl.hdfs.MetadataLogClusterj;
import io.hops.metadata.ndb.dalimpl.hdfs.MisReplicatedRangeQueueClusterj;
import io.hops.metadata.ndb.dalimpl.hdfs.OnGoingSubTreeOpsClusterj;
import io.hops.metadata.ndb.dalimpl.hdfs.PendingBlockClusterj;
import io.hops.metadata.ndb.dalimpl.hdfs.QuotaUpdateClusterj;
import io.hops.metadata.ndb.dalimpl.hdfs.RepairJobsClusterj;
import io.hops.metadata.ndb.dalimpl.hdfs.ReplicaClusterj;
import io.hops.metadata.ndb.dalimpl.hdfs.ReplicaUnderConstructionClusterj;
import io.hops.metadata.ndb.dalimpl.hdfs.SafeBlocksClusterj;
import io.hops.metadata.ndb.dalimpl.hdfs.SizeLogClusterj;
import io.hops.metadata.ndb.dalimpl.hdfs.StorageIdMapClusterj;
import io.hops.metadata.ndb.dalimpl.hdfs.UnderReplicatedBlockClusterj;
import io.hops.metadata.ndb.dalimpl.hdfs.VariableClusterj;
import io.hops.metadata.ndb.dalimpl.yarn.AppSchedulingInfoBlacklistClusterJ;
import io.hops.metadata.ndb.dalimpl.yarn.AppSchedulingInfoClusterJ;
import io.hops.metadata.ndb.dalimpl.yarn.ContainerClusterJ;
import io.hops.metadata.ndb.dalimpl.yarn.ContainerIdToCleanClusterJ;
import io.hops.metadata.ndb.dalimpl.yarn.ContainerStatusClusterJ;
import io.hops.metadata.ndb.dalimpl.yarn.FiCaSchedulerAppLiveContainersClusterJ;
import io.hops.metadata.ndb.dalimpl.yarn.FiCaSchedulerAppNewlyAllocatedContainersClusterJ;
import io.hops.metadata.ndb.dalimpl.yarn.FiCaSchedulerNodeClusterJ;
import io.hops.metadata.ndb.dalimpl.yarn.FinishedApplicationsClusterJ;
import io.hops.metadata.ndb.dalimpl.yarn.FullRMNodeClusterJ;
import io.hops.metadata.ndb.dalimpl.yarn.JustLaunchedContainersClusterJ;
import io.hops.metadata.ndb.dalimpl.yarn.LaunchedContainersClusterJ;
import io.hops.metadata.ndb.dalimpl.yarn.NextHeartbeatClusterJ;
import io.hops.metadata.ndb.dalimpl.yarn.NodeClusterJ;
import io.hops.metadata.ndb.dalimpl.yarn.NodeHBResponseClusterJ;
import io.hops.metadata.ndb.dalimpl.yarn.PendingEventClusterJ;
import io.hops.metadata.ndb.dalimpl.yarn.QueueMetricsClusterJ;
import io.hops.metadata.ndb.dalimpl.yarn.RMContainerClusterJ;
import io.hops.metadata.ndb.dalimpl.yarn.RMContextActiveNodesClusterJ;
import io.hops.metadata.ndb.dalimpl.yarn.RMContextInactiveNodesClusterJ;
import io.hops.metadata.ndb.dalimpl.yarn.RMLoadClusterJ;
import io.hops.metadata.ndb.dalimpl.yarn.RMNodeClusterJ;
import io.hops.metadata.ndb.dalimpl.yarn.ResourceClusterJ;
import io.hops.metadata.ndb.dalimpl.yarn.ResourceRequestClusterJ;
import io.hops.metadata.ndb.dalimpl.yarn.SchedulerApplicationClusterJ;
import io.hops.metadata.ndb.dalimpl.yarn.UpdatedContainerInfoClusterJ;
import io.hops.metadata.ndb.dalimpl.yarn.YarnVariablesClusterJ;
import io.hops.metadata.ndb.dalimpl.yarn.FiCaSchedulerAppLastScheduledContainerClusterJ;
import io.hops.metadata.ndb.dalimpl.yarn.FiCaSchedulerAppReservationsClusterJ;
import io.hops.metadata.ndb.dalimpl.yarn.capacity.FiCaSchedulerAppReservedContainersClusterJ;
import io.hops.metadata.ndb.dalimpl.yarn.FiCaSchedulerAppSchedulingOpportunitiesClusterJ;
import io.hops.metadata.ndb.dalimpl.yarn.capacity.CSLeafQueueUserInfoClusterJ;
import io.hops.metadata.ndb.dalimpl.yarn.capacity.CSQueueClusterJ;
import io.hops.metadata.ndb.dalimpl.yarn.fair.FSSchedulerNodeClusterJ;
import io.hops.metadata.ndb.dalimpl.yarn.fair.LocalityLevelClusterJ;
import io.hops.metadata.ndb.dalimpl.yarn.fair.PreemptionMapClusterJ;
import io.hops.metadata.ndb.dalimpl.yarn.fair.RunnableAppsClusterJ;
import io.hops.metadata.ndb.dalimpl.yarn.rmstatestore.AllocateResponseClusterJ;
import io.hops.metadata.ndb.dalimpl.yarn.rmstatestore.AllocatedContainersClusterJ;
import io.hops.metadata.ndb.dalimpl.yarn.rmstatestore.ApplicationAttemptStateClusterJ;
import io.hops.metadata.ndb.dalimpl.yarn.rmstatestore.ApplicationStateClusterJ;
import io.hops.metadata.ndb.dalimpl.yarn.rmstatestore.CompletedContainersStatusClusterJ;
import io.hops.metadata.ndb.dalimpl.yarn.rmstatestore.DelegationKeyClusterJ;
import io.hops.metadata.ndb.dalimpl.yarn.rmstatestore.DelegationTokenClusterJ;
import io.hops.metadata.ndb.dalimpl.yarn.rmstatestore.RMStateVersionClusterJ;
import io.hops.metadata.ndb.dalimpl.yarn.rmstatestore.RPCClusterJ;
import io.hops.metadata.ndb.dalimpl.yarn.rmstatestore.RanNodeClusterJ;
import io.hops.metadata.ndb.dalimpl.yarn.rmstatestore.SecretMamagerKeysClusterJ;
import io.hops.metadata.ndb.dalimpl.yarn.rmstatestore.SequenceNumberClusterJ;
import io.hops.metadata.ndb.dalimpl.yarn.rmstatestore.UpdatedNodeClusterJ;
import io.hops.metadata.ndb.mysqlserver.MysqlServerConnector;
import io.hops.metadata.yarn.dal.AppSchedulingInfoBlacklistDataAccess;
import io.hops.metadata.yarn.dal.AppSchedulingInfoDataAccess;
import io.hops.metadata.yarn.dal.ContainerDataAccess;
import io.hops.metadata.yarn.dal.ContainerIdToCleanDataAccess;
import io.hops.metadata.yarn.dal.ContainerStatusDataAccess;
import io.hops.metadata.yarn.dal.FiCaSchedulerAppLiveContainersDataAccess;
import io.hops.metadata.yarn.dal.FiCaSchedulerAppNewlyAllocatedContainersDataAccess;
import io.hops.metadata.yarn.dal.FiCaSchedulerNodeDataAccess;
import io.hops.metadata.yarn.dal.FinishedApplicationsDataAccess;
import io.hops.metadata.yarn.dal.FullRMNodeDataAccess;
import io.hops.metadata.yarn.dal.JustLaunchedContainersDataAccess;
import io.hops.metadata.yarn.dal.LaunchedContainersDataAccess;
import io.hops.metadata.yarn.dal.NextHeartbeatDataAccess;
import io.hops.metadata.yarn.dal.NodeDataAccess;
import io.hops.metadata.yarn.dal.NodeHBResponseDataAccess;
import io.hops.metadata.yarn.dal.PendingEventDataAccess;
import io.hops.metadata.yarn.dal.QueueMetricsDataAccess;
import io.hops.metadata.yarn.dal.RMContainerDataAccess;
import io.hops.metadata.yarn.dal.RMContextActiveNodesDataAccess;
import io.hops.metadata.yarn.dal.RMContextInactiveNodesDataAccess;
import io.hops.metadata.yarn.dal.RMLoadDataAccess;
import io.hops.metadata.yarn.dal.RMNodeDataAccess;
import io.hops.metadata.yarn.dal.ResourceDataAccess;
import io.hops.metadata.yarn.dal.ResourceRequestDataAccess;
import io.hops.metadata.yarn.dal.SchedulerApplicationDataAccess;
import io.hops.metadata.yarn.dal.UpdatedContainerInfoDataAccess;
import io.hops.metadata.yarn.dal.YarnVariablesDataAccess;
import io.hops.metadata.yarn.dal.FiCaSchedulerAppLastScheduledContainerDataAccess;
import io.hops.metadata.yarn.dal.FiCaSchedulerAppReservationsDataAccess;
import io.hops.metadata.yarn.dal.capacity.FiCaSchedulerAppReservedContainersDataAccess;
import io.hops.metadata.yarn.dal.FiCaSchedulerAppSchedulingOpportunitiesDataAccess;
import io.hops.metadata.yarn.dal.capacity.CSLeafQueueUserInfoDataAccess;
import io.hops.metadata.yarn.dal.capacity.CSQueueDataAccess;
import io.hops.metadata.yarn.dal.fair.FSSchedulerNodeDataAccess;
import io.hops.metadata.yarn.dal.fair.LocalityLevelDataAccess;
import io.hops.metadata.yarn.dal.fair.PreemptionMapDataAccess;
import io.hops.metadata.yarn.dal.fair.RunnableAppsDataAccess;
import io.hops.metadata.yarn.dal.rmstatestore.AllocateResponseDataAccess;
import io.hops.metadata.yarn.dal.rmstatestore.AllocatedContainersDataAccess;
import io.hops.metadata.yarn.dal.rmstatestore.ApplicationAttemptStateDataAccess;
import io.hops.metadata.yarn.dal.rmstatestore.ApplicationStateDataAccess;
import io.hops.metadata.yarn.dal.rmstatestore.CompletedContainersStatusDataAccess;
import io.hops.metadata.yarn.dal.rmstatestore.DelegationKeyDataAccess;
import io.hops.metadata.yarn.dal.rmstatestore.DelegationTokenDataAccess;
import io.hops.metadata.yarn.dal.rmstatestore.RMStateVersionDataAccess;
import io.hops.metadata.yarn.dal.rmstatestore.RPCDataAccess;
import io.hops.metadata.yarn.dal.rmstatestore.RanNodeDataAccess;
import io.hops.metadata.yarn.dal.rmstatestore.SecretMamagerKeysDataAccess;
import io.hops.metadata.yarn.dal.rmstatestore.SequenceNumberDataAccess;
import io.hops.metadata.yarn.dal.rmstatestore.UpdatedNodeDataAccess;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class NdbStorageFactory implements DalStorageFactory {

  private Map<Class, EntityDataAccess> dataAccessMap =
      new HashMap<Class, EntityDataAccess>();

  @Override
  public void setConfiguration(Properties conf)
          throws StorageInitializtionException {
    try {
      ClusterjConnector.getInstance().setConfiguration(conf);
      MysqlServerConnector.getInstance().setConfiguration(conf);
      initDataAccessMap();
    } catch (IOException ex) {
      throw new StorageInitializtionException(ex);
    }
  }

  private void initDataAccessMap() {
    initYarnStats();
    dataAccessMap
            .put(RMStateVersionDataAccess.class, new RMStateVersionClusterJ());
    dataAccessMap
            .put(ApplicationStateDataAccess.class, new ApplicationStateClusterJ());
    dataAccessMap.put(UpdatedNodeDataAccess.class, new UpdatedNodeClusterJ());
    dataAccessMap.put(ApplicationAttemptStateDataAccess.class,
            new ApplicationAttemptStateClusterJ());
    dataAccessMap.put(RanNodeDataAccess.class, new RanNodeClusterJ());
    dataAccessMap
            .put(DelegationTokenDataAccess.class, new DelegationTokenClusterJ());
    dataAccessMap
            .put(SequenceNumberDataAccess.class, new SequenceNumberClusterJ());
    dataAccessMap
            .put(DelegationKeyDataAccess.class, new DelegationKeyClusterJ());
    dataAccessMap
            .put(YarnVariablesDataAccess.class, new YarnVariablesClusterJ());
    dataAccessMap.put(RPCDataAccess.class, new RPCClusterJ());
    dataAccessMap.put(QueueMetricsDataAccess.class, new QueueMetricsClusterJ());
    dataAccessMap.put(FiCaSchedulerNodeDataAccess.class,
            new FiCaSchedulerNodeClusterJ());
    dataAccessMap.put(ResourceDataAccess.class, new ResourceClusterJ());
    dataAccessMap.put(NodeDataAccess.class, new NodeClusterJ());
    dataAccessMap.put(ResourceDataAccess.class, new ResourceClusterJ());
    dataAccessMap.put(RMNodeDataAccess.class, new RMNodeClusterJ());
    dataAccessMap.put(RMContextActiveNodesDataAccess.class,
            new RMContextActiveNodesClusterJ());
    dataAccessMap.put(RMContextInactiveNodesDataAccess.class,
            new RMContextInactiveNodesClusterJ());
    dataAccessMap
            .put(ContainerStatusDataAccess.class, new ContainerStatusClusterJ());
    dataAccessMap
            .put(NodeHBResponseDataAccess.class, new NodeHBResponseClusterJ());
    dataAccessMap.put(UpdatedContainerInfoDataAccess.class,
            new UpdatedContainerInfoClusterJ());
    dataAccessMap.put(ContainerIdToCleanDataAccess.class,
            new ContainerIdToCleanClusterJ());
    dataAccessMap.put(JustLaunchedContainersDataAccess.class,
            new JustLaunchedContainersClusterJ());
    dataAccessMap.put(LaunchedContainersDataAccess.class,
            new LaunchedContainersClusterJ());
    dataAccessMap.put(FinishedApplicationsDataAccess.class,
            new FinishedApplicationsClusterJ());
    dataAccessMap.put(SchedulerApplicationDataAccess.class,
            new SchedulerApplicationClusterJ());
    dataAccessMap.put(FiCaSchedulerAppNewlyAllocatedContainersDataAccess.class,
            new FiCaSchedulerAppNewlyAllocatedContainersClusterJ());
    dataAccessMap.put(FiCaSchedulerAppSchedulingOpportunitiesDataAccess.class,
            new FiCaSchedulerAppSchedulingOpportunitiesClusterJ());
    dataAccessMap.put(FiCaSchedulerAppLastScheduledContainerDataAccess.class,
            new FiCaSchedulerAppLastScheduledContainerClusterJ());
    dataAccessMap.put(FiCaSchedulerAppLiveContainersDataAccess.class,
            new FiCaSchedulerAppLiveContainersClusterJ());
    dataAccessMap.put(FiCaSchedulerAppReservedContainersDataAccess.class,
            new FiCaSchedulerAppReservedContainersClusterJ());
    dataAccessMap.put(FiCaSchedulerAppReservationsDataAccess.class,
            new FiCaSchedulerAppReservationsClusterJ());
    dataAccessMap.put(RMContainerDataAccess.class, new RMContainerClusterJ());
    dataAccessMap.put(ContainerDataAccess.class, new ContainerClusterJ());
    dataAccessMap.put(AppSchedulingInfoDataAccess.class,
            new AppSchedulingInfoClusterJ());
    dataAccessMap.put(AppSchedulingInfoBlacklistDataAccess.class,
            new AppSchedulingInfoBlacklistClusterJ());
    dataAccessMap
            .put(ResourceRequestDataAccess.class, new ResourceRequestClusterJ());
    dataAccessMap.put(BlockInfoDataAccess.class, new BlockInfoClusterj());
    dataAccessMap.put(PendingBlockDataAccess.class, new PendingBlockClusterj());
    dataAccessMap.put(ReplicaUnderConstructionDataAccess.class,
            new ReplicaUnderConstructionClusterj());
    dataAccessMap.put(INodeDataAccess.class, new INodeClusterj());
    dataAccessMap
            .put(INodeAttributesDataAccess.class, new INodeAttributesClusterj());
    dataAccessMap.put(LeaseDataAccess.class, new LeaseClusterj());
    dataAccessMap.put(LeasePathDataAccess.class, new LeasePathClusterj());
    dataAccessMap.put(OngoingSubTreeOpsDataAccess.class, new OnGoingSubTreeOpsClusterj());
    dataAccessMap
            .put(HdfsLeDescriptorDataAccess.class, new HdfsLeaderClusterj());
    dataAccessMap
            .put(YarnLeDescriptorDataAccess.class, new YarnLeaderClusterj());
    dataAccessMap.put(ReplicaDataAccess.class, new ReplicaClusterj());
    dataAccessMap
            .put(CorruptReplicaDataAccess.class, new CorruptReplicaClusterj());
    dataAccessMap
            .put(ExcessReplicaDataAccess.class, new ExcessReplicaClusterj());
    dataAccessMap
            .put(InvalidateBlockDataAccess.class, new InvalidatedBlockClusterj());
    dataAccessMap.put(UnderReplicatedBlockDataAccess.class,
            new UnderReplicatedBlockClusterj());
    dataAccessMap.put(VariableDataAccess.class, new VariableClusterj());
    dataAccessMap.put(StorageIdMapDataAccess.class, new StorageIdMapClusterj());
    dataAccessMap
            .put(EncodingStatusDataAccess.class, new EncodingStatusClusterj() {
            });
    dataAccessMap.put(BlockLookUpDataAccess.class, new BlockLookUpClusterj());
    dataAccessMap
            .put(FSSchedulerNodeDataAccess.class, new FSSchedulerNodeClusterJ());
    dataAccessMap.put(SafeBlocksDataAccess.class, new SafeBlocksClusterj());
    dataAccessMap.put(MisReplicatedRangeQueueDataAccess.class,
            new MisReplicatedRangeQueueClusterj());
    dataAccessMap.put(QuotaUpdateDataAccess.class, new QuotaUpdateClusterj());
    dataAccessMap.put(SecretMamagerKeysDataAccess.class,
            new SecretMamagerKeysClusterJ());
    dataAccessMap
            .put(AllocateResponseDataAccess.class, new AllocateResponseClusterJ());
    dataAccessMap
            .put(AllocatedContainersDataAccess.class, new AllocatedContainersClusterJ());
    dataAccessMap.
            put(CompletedContainersStatusDataAccess.class,
                    new CompletedContainersStatusClusterJ());
    dataAccessMap.put(PendingEventDataAccess.class, new PendingEventClusterJ());
    dataAccessMap
            .put(BlockChecksumDataAccess.class, new BlockChecksumClusterj());
    dataAccessMap
            .put(NextHeartbeatDataAccess.class, new NextHeartbeatClusterJ());
    dataAccessMap.put(RMLoadDataAccess.class, new RMLoadClusterJ());
    dataAccessMap.put(FullRMNodeDataAccess.class, new FullRMNodeClusterJ());
    dataAccessMap.put(MetadataLogDataAccess.class, new MetadataLogClusterj());
    dataAccessMap.put(AccessTimeLogDataAccess.class,
            new AccessTimeLogClusterj());
    dataAccessMap.put(SizeLogDataAccess.class, new SizeLogClusterj());
    dataAccessMap.put(EncodingJobsDataAccess.class, new EncodingJobsClusterj());
    dataAccessMap.put(RepairJobsDataAccess.class, new RepairJobsClusterj());
    dataAccessMap.
            put(LocalityLevelDataAccess.class, new LocalityLevelClusterJ());
    dataAccessMap.put(RunnableAppsDataAccess.class, new RunnableAppsClusterJ());
    dataAccessMap.
            put(PreemptionMapDataAccess.class, new PreemptionMapClusterJ());
    dataAccessMap.put(CSQueueDataAccess.class, new CSQueueClusterJ());
    dataAccessMap.put(CSLeafQueueUserInfoDataAccess.class,
            new CSLeafQueueUserInfoClusterJ());
  }

  @Override
  public StorageConnector getConnector() {
    return ClusterjConnector.getInstance();
  }

  @Override
  public EntityDataAccess getDataAccess(Class type) {
    return dataAccessMap.get(type);
  }

  Map<String, Integer> yarnStats = new HashMap<String, Integer>();
  private void initYarnStats(){
    yarnStats.put("AppSchedulingInfoBlacklisAdd",
            AppSchedulingInfoBlacklistClusterJ.add);
    yarnStats.put("AppSchedulingInfoBlacklisRemove",
            AppSchedulingInfoBlacklistClusterJ.remove);
    yarnStats.put("AppSchedulingInfoAdd", AppSchedulingInfoClusterJ.add);
    yarnStats.put("AppSchedulingInfoRemove", AppSchedulingInfoClusterJ.remove);
    yarnStats.put("ContainerAdd", ContainerClusterJ.add);
    yarnStats.put("ContainerAdd_totalPersistableSize", ContainerClusterJ.totalPersistableSize);
    yarnStats.put("ContainerAdd_totalContainerSize", ContainerClusterJ.totalContainerSize);
    yarnStats.put("ContainerIdToCleanAdd", ContainerIdToCleanClusterJ.add);
    yarnStats.put("ContainerIdToCleanRemove", ContainerIdToCleanClusterJ.remove);
    yarnStats.put("ContainerStatusAdd", ContainerStatusClusterJ.add);
    yarnStats.put("FiCaSchedulerAppLastScheduledContainerAdd",
            FiCaSchedulerAppLastScheduledContainerClusterJ.add);
    yarnStats.put("FiCaSchedulerAppLastScheduledContainerRemove",
            FiCaSchedulerAppLastScheduledContainerClusterJ.remove);
    yarnStats.put("FiCaSchedulerAppLiveContainersAdd",
            FiCaSchedulerAppLiveContainersClusterJ.add);
    yarnStats.put("FiCaSchedulerAppLiveContainersRemove",
            FiCaSchedulerAppLiveContainersClusterJ.remove);
    yarnStats.put("FiCaSchedulerAppNewlyAllocatedContainersAdd",
            FiCaSchedulerAppNewlyAllocatedContainersClusterJ.add);
    yarnStats.put("FiCaSchedulerAppNewlyAllocatedContainersRemove",
            FiCaSchedulerAppNewlyAllocatedContainersClusterJ.remove);
    yarnStats.put("FiCaSchedulerAppReservationsAdd",
            FiCaSchedulerAppReservationsClusterJ.add);
    yarnStats.put("FiCaSchedulerAppReservationsRemove",
            FiCaSchedulerAppReservationsClusterJ.remove);
    yarnStats.put("FiCaSchedulerAppSchedulingOpportunitiesAdd",
            FiCaSchedulerAppSchedulingOpportunitiesClusterJ.add);
    yarnStats.put("FiCaSchedulerAppSchedulingOpportunitiesRemove",
            FiCaSchedulerAppSchedulingOpportunitiesClusterJ.remove);
    yarnStats.put("FiCaSchedulerNodeAdd", FiCaSchedulerNodeClusterJ.add);
    yarnStats.put("FiCaSchedulerNodeRemove", FiCaSchedulerNodeClusterJ.remove);
    yarnStats.put("FinishedApplicationsClusterJAdd",
            FinishedApplicationsClusterJ.add);
    yarnStats.put("FinishedApplicationsClusterJRemove",
            FinishedApplicationsClusterJ.remove);
    yarnStats.put("JustLaunchedContainersClusterJAdd",
            JustLaunchedContainersClusterJ.add);
    yarnStats.put("JustLaunchedContainersClusterJRemove",
            JustLaunchedContainersClusterJ.remove);
    yarnStats.put("LaunchedContainersClusterJAdd",
            LaunchedContainersClusterJ.add);
    yarnStats.put("LaunchedContainersClusterJRemove",
            LaunchedContainersClusterJ.remove);
    yarnStats.put("NextHeartbeatClusterJAdd",
            NextHeartbeatClusterJ.add);
    yarnStats.put("NextHeartbeatClusterJRemove",
            NextHeartbeatClusterJ.remove);
    yarnStats.put("NodeClusterJAdd",
            NodeClusterJ.add);
    yarnStats.put("NodeHBResponseClusterJAdd",
            NodeHBResponseClusterJ.add);
    yarnStats.put("nodeHbResponseSize", NodeHBResponseClusterJ.totalSize);
    yarnStats.put("QueueMetricsClusterJAdd",
            QueueMetricsClusterJ.add);
    yarnStats.put("RMContainerClusterJAdd",
            RMContainerClusterJ.add.get());
    yarnStats.put("RMContainerClusterJRemove",
            RMContainerClusterJ.remove);
    yarnStats.put("RMContextActiveNodesClusterJAdd",
            RMContextActiveNodesClusterJ.add);
    yarnStats.put("RMContextActiveNodesClusterJRemove",
            RMContextActiveNodesClusterJ.remove);
    yarnStats.put("RMContextInactiveNodesClusterJAdd",
            RMContextInactiveNodesClusterJ.add);
    yarnStats.put("RMContextInactiveNodesClusterJRemove",
            RMContextInactiveNodesClusterJ.remove);
    yarnStats.put("RMLoadClusterJAdd",
            RMLoadClusterJ.add);
    yarnStats.put("RMNodeClusterJAdd",
            RMNodeClusterJ.add);
    yarnStats.put("RMNodeClusterJRemove",
            RMNodeClusterJ.remove);
    yarnStats.put("PendingEventClusterJAdd",
            PendingEventClusterJ.add);
    yarnStats.put("PendingEventClusterJRemove",
            PendingEventClusterJ.remove);
    yarnStats.put("ResourceClusterJAdd",
            ResourceClusterJ.add);
    yarnStats.put("ResourceClusterJRemove",
            ResourceClusterJ.remove);
    yarnStats.put("ResourceRequestClusterJAdd",
            ResourceRequestClusterJ.add);
    yarnStats.put("RequestSize",
            NodeHBResponseClusterJ.totalSize);
    yarnStats.put("ResourceRequestClusterJRemove",
            ResourceRequestClusterJ.remove);
    yarnStats.put("SchedulerApplicationClusterJAdd",
            SchedulerApplicationClusterJ.add);
    yarnStats.put("SchedulerApplicationClusterJRemove",
            SchedulerApplicationClusterJ.remove);
    yarnStats.put("UpdatedContainerInfoClusterJAdd",
            UpdatedContainerInfoClusterJ.add);
    yarnStats.put("UpdatedContainerInfoClusterJRemove",
            UpdatedContainerInfoClusterJ.remove);
    yarnStats.put("YarnVariablesClusterJAdd",
            YarnVariablesClusterJ.add);
    yarnStats.put("AllocateResponseClusterJAdd",
            AllocateResponseClusterJ.add);
    yarnStats.put("AllocateResponseClusterJRemove",
            AllocateResponseClusterJ.remove);
    yarnStats.put("AllocatedContainersClusterJAdd",
            AllocatedContainersClusterJ.add);
    yarnStats.put("AllocatedContainersClusterJRemove",
            AllocatedContainersClusterJ.remove);
    yarnStats.put("AllocatedContainersClusterJAdd",
            AllocatedContainersClusterJ.add);
    yarnStats.put("AllocatedContainersClusterJRemove",
            AllocatedContainersClusterJ.remove);
    yarnStats.put("AllocatedContainersClusterJAdd",
            AllocatedContainersClusterJ.add);
    yarnStats.put("AllocatedContainersClusterJRemove",
            AllocatedContainersClusterJ.remove);
    yarnStats.put("DelegationKeyClusterJAdd",
            DelegationKeyClusterJ.add);
    yarnStats.put("DelegationKeyClusterJRemove",
            DelegationKeyClusterJ.remove);
    yarnStats.put("DelegationTokenClusterJAdd",
            DelegationTokenClusterJ.add);
    yarnStats.put("DelegationTokenClusterJRemove",
            DelegationTokenClusterJ.remove);
    yarnStats.put("RMStateVersionClusterJAdd",
            RMStateVersionClusterJ.add);
    yarnStats.put("RPCClusterJAdd",
            RPCClusterJ.add);
    yarnStats.put("RPCClusterJRemove",
            RPCClusterJ.remove);
    yarnStats.put("RanNodeClusterJAdd",
            RanNodeClusterJ.add);
    yarnStats.put("SecretMamagerKeysClusterJAdd",
            SecretMamagerKeysClusterJ.add);
    yarnStats.put("SecretMamagerKeysClusterJRemove",
            SecretMamagerKeysClusterJ.remove);
    yarnStats.put("SequenceNumberClusterJAdd",
            SequenceNumberClusterJ.add);
    yarnStats.put("UpdatedNodeClusterJAdd",
            UpdatedNodeClusterJ.add);

  }

  
  public String printYarnState() {
    int value = 0;
    String result = "";
    result = result.concat("AppSchedulingInfoBlacklis:\t");
    result = result.concat("\tadd: ");
    value = AppSchedulingInfoBlacklistClusterJ.add - yarnStats.get(
            "AppSchedulingInfoBlacklisAdd");
    yarnStats.put("AppSchedulingInfoBlacklisAdd",
            AppSchedulingInfoBlacklistClusterJ.add);
    result = result.concat(value + "\t");
    result = result.concat("\tremove: ");
    value = AppSchedulingInfoBlacklistClusterJ.remove - yarnStats.get(
            "AppSchedulingInfoBlacklisRemove");
    yarnStats.put("AppSchedulingInfoBlacklisRemove",
            AppSchedulingInfoBlacklistClusterJ.remove);
    result = result.concat(value + "\n");

    result = result.concat("AppSchedulingInfo:\t");
    result = result.concat("\tadd: ");
    value = AppSchedulingInfoClusterJ.add - yarnStats.
            get("AppSchedulingInfoAdd");
    yarnStats.put("AppSchedulingInfoAdd", AppSchedulingInfoClusterJ.add);
    result = result.concat(value + "\t");
    result = result.concat("\tremove: ");
    value = AppSchedulingInfoClusterJ.remove - yarnStats.get(
            "AppSchedulingInfoRemove");
    yarnStats.put("AppSchedulingInfoRemove", AppSchedulingInfoClusterJ.remove);
    result = result.concat(value + "\n");

    result = result.concat("Container:\t");
    result = result.concat("\tadd: ");
    value = ContainerClusterJ.add - yarnStats.get("ContainerAdd");
    yarnStats.put("ContainerAdd", ContainerClusterJ.add);
    if (value != 0) {
      int lastSecondTotalContainerSize = ContainerClusterJ.totalContainerSize
              - yarnStats.get("ContainerAdd_totalContainerSize");
      int avgLastSecondTotalContainerSize = lastSecondTotalContainerSize / value;
      yarnStats.put("ContainerAdd_totalContainerSize",
              ContainerClusterJ.totalContainerSize);
      int lastSecondTotalPersistableSize
              = ContainerClusterJ.totalPersistableSize - yarnStats.get(
                      "ContainerAdd_totalPersistableSize");
      int avgLastSecondTotalPersistableSize = lastSecondTotalPersistableSize
              / value;
      yarnStats.put("ContainerAdd_totalPersistableSize",
              ContainerClusterJ.totalPersistableSize);
      result = result.concat(value + " (avg container size: "
              + avgLastSecondTotalContainerSize + ", avg persistable size: "
              + avgLastSecondTotalPersistableSize + ")\n");
    } else {
      result = result.concat(value + "\n");
    }

    result = result.concat("ContainerIdToClean:\t");
    result = result.concat("\tadd: ");
    value = ContainerIdToCleanClusterJ.add - yarnStats.get(
            "ContainerIdToCleanAdd");
    yarnStats.put("ContainerIdToCleanAdd", ContainerIdToCleanClusterJ.add);
    result = result.concat(value + "\t");
    result = result.concat("\tremove: ");
    value = ContainerIdToCleanClusterJ.remove - yarnStats.get(
            "ContainerIdToCleanRemove");
    yarnStats.put("ContainerIdToCleanRemove", ContainerIdToCleanClusterJ.remove);
    result = result.concat(value + "\n");

    result = result.concat("ContainerStatus:\t");
    result = result.concat("\tadd: ");
    value = ContainerStatusClusterJ.add - yarnStats.get("ContainerStatusAdd");
    yarnStats.put("ContainerStatusAdd", ContainerStatusClusterJ.add);
    result = result.concat(value + "\n");


    result = result.concat("FiCaSchedulerAppLastScheduledContainer:\t");
    result = result.concat("\tadd: ");
    value = FiCaSchedulerAppLastScheduledContainerClusterJ.add - yarnStats.get(
            "FiCaSchedulerAppLastScheduledContainerAdd");
    yarnStats.put("FiCaSchedulerAppLastScheduledContainerAdd",
            FiCaSchedulerAppLastScheduledContainerClusterJ.add);
    result = result.concat(value + "\t");
    result = result.concat("\tremove: ");
    value = FiCaSchedulerAppLastScheduledContainerClusterJ.remove - yarnStats.
            get("FiCaSchedulerAppLastScheduledContainerRemove");
    yarnStats.put("FiCaSchedulerAppLastScheduledContainerRemove",
            FiCaSchedulerAppLastScheduledContainerClusterJ.remove);
    result = result.concat(value + "\n");

    result = result.concat("FiCaSchedulerAppLiveContainers:\t");
    result = result.concat("\tadd: ");
    value = FiCaSchedulerAppLiveContainersClusterJ.add - yarnStats.get(
            "FiCaSchedulerAppLiveContainersAdd");
    yarnStats.put("FiCaSchedulerAppLiveContainersAdd",
            FiCaSchedulerAppLiveContainersClusterJ.add);
    result = result.concat(value + "\t");
    result = result.concat("\tremove: ");
    value = FiCaSchedulerAppLiveContainersClusterJ.remove - yarnStats.get(
            "FiCaSchedulerAppLiveContainersRemove");
    yarnStats.put("FiCaSchedulerAppLiveContainersRemove",
            FiCaSchedulerAppLiveContainersClusterJ.remove);
    result = result.concat(value + "\n");

    result = result.concat("FiCaSchedulerAppNewlyAllocatedContainers:\t");
    result = result.concat("\tadd: ");
    value = FiCaSchedulerAppNewlyAllocatedContainersClusterJ.add - yarnStats.
            get("FiCaSchedulerAppNewlyAllocatedContainersAdd");
    yarnStats.put("FiCaSchedulerAppNewlyAllocatedContainersAdd",
            FiCaSchedulerAppNewlyAllocatedContainersClusterJ.add);
    result = result.concat(value + "\t");
    result = result.concat("\tremove: ");
    value = FiCaSchedulerAppNewlyAllocatedContainersClusterJ.remove - yarnStats.
            get("FiCaSchedulerAppNewlyAllocatedContainersRemove");
    yarnStats.put("FiCaSchedulerAppNewlyAllocatedContainersRemove",
            FiCaSchedulerAppNewlyAllocatedContainersClusterJ.remove);
    result = result.concat(value + "\n");

    result = result.concat("FiCaSchedulerAppReservations:\t");
    result = result.concat("\tadd: ");
    value = FiCaSchedulerAppReservationsClusterJ.add - yarnStats.get(
            "FiCaSchedulerAppReservationsAdd");
    yarnStats.put("FiCaSchedulerAppReservationsAdd",
            FiCaSchedulerAppReservationsClusterJ.add);
    result = result.concat(value + "\t");
    result = result.concat("\tremove: ");
    value = FiCaSchedulerAppReservationsClusterJ.remove - yarnStats.get(
            "FiCaSchedulerAppReservationsRemove");
    yarnStats.put("FiCaSchedulerAppReservationsRemove",
            FiCaSchedulerAppReservationsClusterJ.remove);
    result = result.concat(value + "\n");

    result = result.concat("FiCaSchedulerAppSchedulingOpportunities:\t");
    result = result.concat("\tadd: ");
    value = FiCaSchedulerAppSchedulingOpportunitiesClusterJ.add - yarnStats.get(
            "FiCaSchedulerAppSchedulingOpportunitiesAdd");
    yarnStats.put("FiCaSchedulerAppSchedulingOpportunitiesAdd",
            FiCaSchedulerAppSchedulingOpportunitiesClusterJ.add);
    result = result.concat(value + "\t");
    result = result.concat("\tremove: ");
    value = FiCaSchedulerAppSchedulingOpportunitiesClusterJ.remove - yarnStats.
            get("FiCaSchedulerAppSchedulingOpportunitiesRemove");
    yarnStats.put("FiCaSchedulerAppSchedulingOpportunitiesRemove",
            FiCaSchedulerAppSchedulingOpportunitiesClusterJ.remove);
    result = result.concat(value + "\n");

    result = result.concat("FiCaSchedulerNode:\t");
    result = result.concat("\tadd: ");
    value = FiCaSchedulerNodeClusterJ.add - yarnStats.
            get("FiCaSchedulerNodeAdd");
    yarnStats.put("FiCaSchedulerNodeAdd", FiCaSchedulerNodeClusterJ.add);
    result = result.concat(value + "\t");
    result = result.concat("\tremove: ");
    value = FiCaSchedulerNodeClusterJ.remove - yarnStats.get(
            "FiCaSchedulerNodeRemove");
    yarnStats.put("FiCaSchedulerNodeRemove", FiCaSchedulerNodeClusterJ.remove);
    result = result.concat(value + "\n");

    result = result.concat("FinishedApplicationsClusterJ:\t");
    result = result.concat("\tadd: ");
    value = FinishedApplicationsClusterJ.add - yarnStats.get(
            "FinishedApplicationsClusterJAdd");
    yarnStats.put("FinishedApplicationsClusterJAdd",
            FinishedApplicationsClusterJ.add);
    result = result.concat(value + "\t");
    result = result.concat("\tremove: ");
    value = FinishedApplicationsClusterJ.remove - yarnStats.get(
            "FinishedApplicationsClusterJRemove");
    yarnStats.put("FinishedApplicationsClusterJRemove",
            FinishedApplicationsClusterJ.remove);
    result = result.concat(value + "\n");

    result = result.concat("JustLaunchedContainersClusterJ:\t");
    result = result.concat("\tadd: ");
    value = JustLaunchedContainersClusterJ.add - yarnStats.get(
            "JustLaunchedContainersClusterJAdd");
    yarnStats.put("JustLaunchedContainersClusterJAdd",
            JustLaunchedContainersClusterJ.add);
    result = result.concat(value + "\t");
    result = result.concat("\tremove: ");
    value = JustLaunchedContainersClusterJ.remove - yarnStats.get(
            "JustLaunchedContainersClusterJRemove");
    yarnStats.put("JustLaunchedContainersClusterJRemove",
            JustLaunchedContainersClusterJ.remove);
    result = result.concat(value + "\n");

    result = result.concat("LaunchedContainersClusterJ:\t");
    result = result.concat("\tadd: ");
    value = LaunchedContainersClusterJ.add - yarnStats.get(
            "LaunchedContainersClusterJAdd");
    yarnStats.put("LaunchedContainersClusterJAdd",
            LaunchedContainersClusterJ.add);
    result = result.concat(value + "\t");
    result = result.concat("\tremove: ");
    value = LaunchedContainersClusterJ.remove - yarnStats.get(
            "LaunchedContainersClusterJRemove");
    yarnStats.put("LaunchedContainersClusterJRemove",
            LaunchedContainersClusterJ.remove);
    result = result.concat(value + "\n");

    result = result.concat("NextHeartbeatClusterJ:\t");
    result = result.concat("\tadd: ");
    value = NextHeartbeatClusterJ.add - yarnStats.get(
            "NextHeartbeatClusterJAdd");
    yarnStats.put("NextHeartbeatClusterJAdd",
            NextHeartbeatClusterJ.add);
    result = result.concat(value + "\t");
    result = result.concat("\tremove: ");
    value = NextHeartbeatClusterJ.remove - yarnStats.get(
            "NextHeartbeatClusterJRemove");
    yarnStats.put("NextHeartbeatClusterJRemove",
            NextHeartbeatClusterJ.remove);
    result = result.concat(value + "\n");

    
    result = result.concat("NodeClusterJ:\t");
    result = result.concat("\tadd: ");
    value = NodeClusterJ.add - yarnStats.get(
            "NodeClusterJAdd");
    yarnStats.put("NodeClusterJAdd",
            NodeClusterJ.add);
    result = result.concat(value + "\n");

    
    result = result.concat("NodeHBResponseClusterJ:\t");
    result = result.concat("\tadd: ");
    value = NodeHBResponseClusterJ.add - yarnStats.get(
            "NodeHBResponseClusterJAdd");
    yarnStats.put("NodeHBResponseClusterJAdd",
            NodeHBResponseClusterJ.add);

    if (value != 0) {
      int lastSecondTotalResponseSize = NodeHBResponseClusterJ.totalSize
              - yarnStats.get("nodeHbResponseSize");
      int avgLastSecondTotalResponseSize = lastSecondTotalResponseSize / value;
      yarnStats.put("nodeHbResponseSize",
              NodeHBResponseClusterJ.totalSize);

      result = result.concat(value + " (avg size: "
              + avgLastSecondTotalResponseSize + ")\n");
    } else {
      result = result.concat(value + "\n");
    }

    
    result = result.concat("QueueMetricsClusterJ:\t");
    result = result.concat("\tadd: ");
    value = QueueMetricsClusterJ.add - yarnStats.get(
            "QueueMetricsClusterJAdd");
    yarnStats.put("QueueMetricsClusterJAdd",
            QueueMetricsClusterJ.add);
    result = result.concat(value + "\n");

    
    result = result.concat("RMContainerClusterJ:\t");
    result = result.concat("\tadd: ");
    value = RMContainerClusterJ.add.get() - yarnStats.get(
            "RMContainerClusterJAdd");
    yarnStats.put("RMContainerClusterJAdd",
            RMContainerClusterJ.add.get());
    result = result.concat(value + "\t");
    result = result.concat("\tremove: ");
    value = RMContainerClusterJ.remove - yarnStats.get(
            "RMContainerClusterJRemove");
    yarnStats.put("RMContainerClusterJRemove",
            RMContainerClusterJ.remove);
    result = result.concat(value + "\n");

    result = result.concat("RMContextActiveNodesClusterJ:\t");
    result = result.concat("\tadd: ");
    value = RMContextActiveNodesClusterJ.add - yarnStats.get(
            "RMContextActiveNodesClusterJAdd");
    yarnStats.put("RMContextActiveNodesClusterJAdd",
            RMContextActiveNodesClusterJ.add);
    result = result.concat(value + "\t");
    result = result.concat("\tremove: ");
    value = RMContextActiveNodesClusterJ.remove - yarnStats.get(
            "RMContextActiveNodesClusterJRemove");
    yarnStats.put("RMContextActiveNodesClusterJRemove",
            RMContextActiveNodesClusterJ.remove);
    result = result.concat(value + "\n");

    result = result.concat("RMContextInactiveNodesClusterJ:\t");
    result = result.concat("\tadd: ");
    value = RMContextInactiveNodesClusterJ.add - yarnStats.get(
            "RMContextInactiveNodesClusterJAdd");
    yarnStats.put("RMContextInactiveNodesClusterJAdd",
            RMContextInactiveNodesClusterJ.add);
    result = result.concat(value + "\t");
    result = result.concat("\tremove: ");
    value = RMContextInactiveNodesClusterJ.remove - yarnStats.get(
            "RMContextInactiveNodesClusterJRemove");
    yarnStats.put("RMContextInactiveNodesClusterJRemove",
            RMContextInactiveNodesClusterJ.remove);
    result = result.concat(value + "\n");

    result = result.concat("RMLoadClusterJ:\t");
    result = result.concat("\tadd: ");
    value = RMLoadClusterJ.add - yarnStats.get(
            "RMLoadClusterJAdd");
    yarnStats.put("RMLoadClusterJAdd",
            RMLoadClusterJ.add);
    result = result.concat(value + "\n");

    
    result = result.concat("RMNodeClusterJ:\t");
    result = result.concat("\tadd: ");
    value = RMNodeClusterJ.add - yarnStats.get(
            "RMNodeClusterJAdd");
    yarnStats.put("RMNodeClusterJAdd",
            RMNodeClusterJ.add);
    result = result.concat(value + "\t");
    result = result.concat("\tremove: ");
    value = RMNodeClusterJ.remove - yarnStats.get(
            "RMNodeClusterJRemove");
    yarnStats.put("RMNodeClusterJRemove",
            RMNodeClusterJ.remove);
    result = result.concat(value + "\n");

    result = result.concat("PendingEventClusterJ:\t");
    result = result.concat("\tadd: ");
    value = PendingEventClusterJ.add - yarnStats.get(
            "PendingEventClusterJAdd");
    yarnStats.put("PendingEventClusterJAdd",
            PendingEventClusterJ.add);
    result = result.concat(value + "\t");
    result = result.concat("\tremove: ");
    value = PendingEventClusterJ.remove - yarnStats.get(
            "PendingEventClusterJRemove");
    yarnStats.put("PendingEventClusterJRemove",
            PendingEventClusterJ.remove);
    result = result.concat(value + "\n");
    
    
    result = result.concat("ResourceClusterJ:\t");
    result = result.concat("\tadd: ");
    value = ResourceClusterJ.add - yarnStats.get(
            "ResourceClusterJAdd");
    yarnStats.put("ResourceClusterJAdd",
            ResourceClusterJ.add);
    result = result.concat(value + "\t");
    result = result.concat("\tremove: ");
    value = ResourceClusterJ.remove - yarnStats.get(
            "ResourceClusterJRemove");
    yarnStats.put("ResourceClusterJRemove",
            ResourceClusterJ.remove);
    result = result.concat(value + "\n");

    result = result.concat("ResourceRequestClusterJ:\t");
    result = result.concat("\tadd: ");
    value = ResourceRequestClusterJ.add - yarnStats.get(
            "ResourceRequestClusterJAdd");
    yarnStats.put("ResourceRequestClusterJAdd",
            ResourceRequestClusterJ.add);

    if (value != 0) {
      int lastSecondTotalRequestSize = ResourceRequestClusterJ.totalSize
              - yarnStats.get("RequestSize");
      int avgLastSecondTotalRequestSize = lastSecondTotalRequestSize / value;
      yarnStats.put("RequestSize",
              ResourceRequestClusterJ.totalSize);

      result = result.concat(value + " (avg size: "
              + avgLastSecondTotalRequestSize + ")\t");
    } else {
      result = result.concat(value + "\t");
    }
    result = result.concat("\tremove: ");
    value = ResourceRequestClusterJ.remove - yarnStats.get(
            "ResourceRequestClusterJRemove");
    yarnStats.put("ResourceRequestClusterJRemove",
            ResourceRequestClusterJ.remove);
    result = result.concat(value + "\n");

    result = result.concat("SchedulerApplicationClusterJ:\t");
    result = result.concat("\tadd: ");
    value = SchedulerApplicationClusterJ.add - yarnStats.get(
            "SchedulerApplicationClusterJAdd");
    yarnStats.put("SchedulerApplicationClusterJAdd",
            SchedulerApplicationClusterJ.add);
    result = result.concat(value + "\t");
    result = result.concat("\tremove: ");
    value = SchedulerApplicationClusterJ.remove - yarnStats.get(
            "SchedulerApplicationClusterJRemove");
    yarnStats.put("SchedulerApplicationClusterJRemove",
            SchedulerApplicationClusterJ.remove);
    result = result.concat(value + "\n");

    result = result.concat("UpdatedContainerInfoClusterJ:\t");
    result = result.concat("\tadd: ");
    value = UpdatedContainerInfoClusterJ.add - yarnStats.get(
            "UpdatedContainerInfoClusterJAdd");
    yarnStats.put("UpdatedContainerInfoClusterJAdd",
            UpdatedContainerInfoClusterJ.add);
    result = result.concat(value + "\t");
    result = result.concat("\tremove: ");
    value = UpdatedContainerInfoClusterJ.remove - yarnStats.get(
            "UpdatedContainerInfoClusterJRemove");
    yarnStats.put("UpdatedContainerInfoClusterJRemove",
            UpdatedContainerInfoClusterJ.remove);
    result = result.concat(value + "\n");

    result = result.concat("YarnVariablesClusterJ:\t");
    result = result.concat("\tadd: ");
    value = YarnVariablesClusterJ.add - yarnStats.get(
            "YarnVariablesClusterJAdd");
    yarnStats.put("YarnVariablesClusterJAdd",
            YarnVariablesClusterJ.add);
    result = result.concat(value + "\n");

    
    result = result.concat("AllocateResponseClusterJ:\t");
    result = result.concat("\tadd: ");
    value = AllocateResponseClusterJ.add - yarnStats.get(
            "AllocateResponseClusterJAdd");
    yarnStats.put("AllocateResponseClusterJAdd",
            AllocateResponseClusterJ.add);
    result = result.concat(value + "\t");
    result = result.concat("\tremove: ");
    value = AllocateResponseClusterJ.remove - yarnStats.get(
            "AllocateResponseClusterJRemove");
    yarnStats.put("AllocateResponseClusterJRemove",
            AllocateResponseClusterJ.remove);
    result = result.concat(value + "\n");

    result = result.concat("AllocatedContainersClusterJ:\t");
    result = result.concat("\tadd: ");
    value = AllocatedContainersClusterJ.add - yarnStats.get(
            "AllocatedContainersClusterJAdd");
    yarnStats.put("AllocatedContainersClusterJAdd",
            AllocatedContainersClusterJ.add);
    result = result.concat(value + "\t");
    result = result.concat("\tremove: ");
    value = AllocatedContainersClusterJ.remove - yarnStats.get(
            "AllocatedContainersClusterJRemove");
    yarnStats.put("AllocatedContainersClusterJRemove",
            AllocatedContainersClusterJ.remove);
    result = result.concat(value + "\n");

    result = result.concat("ApplicationAttemptStateClusterJ:\t");
    result = result.concat("\tadd: ");
    value = AllocatedContainersClusterJ.add - yarnStats.get(
            "AllocatedContainersClusterJAdd");
    yarnStats.put("AllocatedContainersClusterJAdd",
            AllocatedContainersClusterJ.add);
    result = result.concat(value + "\t");
    result = result.concat("\tremove: ");
    value = AllocatedContainersClusterJ.remove - yarnStats.get(
            "AllocatedContainersClusterJRemove");
    yarnStats.put("AllocatedContainersClusterJRemove",
            AllocatedContainersClusterJ.remove);
    result = result.concat(value + "\n");

    result = result.concat("ApplicationStateClusterJ:\t");
    result = result.concat("\tadd: ");
    value = AllocatedContainersClusterJ.add - yarnStats.get(
            "AllocatedContainersClusterJAdd");
    yarnStats.put("AllocatedContainersClusterJAdd",
            AllocatedContainersClusterJ.add);
    result = result.concat(value + "\t");
    result = result.concat("\tremove: ");
    value = AllocatedContainersClusterJ.remove - yarnStats.get(
            "AllocatedContainersClusterJRemove");
    yarnStats.put("AllocatedContainersClusterJRemove",
            AllocatedContainersClusterJ.remove);
    result = result.concat(value + "\n");

    result = result.concat("DelegationKeyClusterJ:\t");
    result = result.concat("\tadd: ");
    value = DelegationKeyClusterJ.add - yarnStats.get(
            "DelegationKeyClusterJAdd");
    yarnStats.put("DelegationKeyClusterJAdd",
            DelegationKeyClusterJ.add);
    result = result.concat(value + "\t");
    result = result.concat("\tremove: ");
    value = DelegationKeyClusterJ.remove - yarnStats.get(
            "DelegationKeyClusterJRemove");
    yarnStats.put("DelegationKeyClusterJRemove",
            DelegationKeyClusterJ.remove);
    result = result.concat(value + "\n");

    result = result.concat("DelegationTokenClusterJ:\t");
    result = result.concat("\tadd: ");
    value = DelegationTokenClusterJ.add - yarnStats.get(
            "DelegationTokenClusterJAdd");
    yarnStats.put("DelegationTokenClusterJAdd",
            DelegationTokenClusterJ.add);
    result = result.concat(value + "\t");
    result = result.concat("\tremove: ");
    value = DelegationTokenClusterJ.remove - yarnStats.get(
            "DelegationTokenClusterJRemove");
    yarnStats.put("DelegationTokenClusterJRemove",
            DelegationTokenClusterJ.remove);
    result = result.concat(value + "\n");

    result = result.concat("RMStateVersionClusterJ:\t");
    result = result.concat("\tadd: ");
    value = RMStateVersionClusterJ.add - yarnStats.get(
            "RMStateVersionClusterJAdd");
    yarnStats.put("RMStateVersionClusterJAdd",
            RMStateVersionClusterJ.add);
    result = result.concat(value + "\n");

    
    result = result.concat("RPCClusterJ:\t");
    result = result.concat("\tadd: ");
    value = RPCClusterJ.add - yarnStats.get(
            "RPCClusterJAdd");
    yarnStats.put("RPCClusterJAdd",
            RPCClusterJ.add);
    result = result.concat(value + "\t");
    result = result.concat("\tremove: ");
    value = RPCClusterJ.remove - yarnStats.get(
            "RPCClusterJRemove");
    yarnStats.put("RPCClusterJRemove",
            RPCClusterJ.remove);
    result = result.concat(value + "\n");

    result = result.concat("RanNodeClusterJ:\t");
    result = result.concat("\tadd: ");
    value = RanNodeClusterJ.add - yarnStats.get(
            "RanNodeClusterJAdd");
    yarnStats.put("RanNodeClusterJAdd",
            RanNodeClusterJ.add);
    result = result.concat(value + "\n");

    
    result = result.concat("SecretMamagerKeysClusterJ:\t");
    result = result.concat("\tadd: ");
    value = SecretMamagerKeysClusterJ.add - yarnStats.get(
            "SecretMamagerKeysClusterJAdd");
    yarnStats.put("SecretMamagerKeysClusterJAdd",
            SecretMamagerKeysClusterJ.add);
    result = result.concat(value + "\t");
    result = result.concat("\tremove: ");
    value = SecretMamagerKeysClusterJ.remove - yarnStats.get(
            "SecretMamagerKeysClusterJRemove");
    yarnStats.put("SecretMamagerKeysClusterJRemove",
            SecretMamagerKeysClusterJ.remove);
    result = result.concat(value + "\n");

    result = result.concat("SequenceNumberClusterJ:\t");
    result = result.concat("\tadd: ");
    value = SequenceNumberClusterJ.add - yarnStats.get(
            "SequenceNumberClusterJAdd");
    yarnStats.put("SequenceNumberClusterJAdd",
            SequenceNumberClusterJ.add);
    result = result.concat(value + "\n");

    
    result = result.concat("UpdatedNodeClusterJ:\t");
    result = result.concat("\tadd: ");
    value = UpdatedNodeClusterJ.add - yarnStats.get(
            "UpdatedNodeClusterJAdd");
    yarnStats.put("UpdatedNodeClusterJAdd",
            UpdatedNodeClusterJ.add);
    result = result.concat(value + "\n");

    
    return result;
  }
}
