/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package io.hops.metadata.ndb.dalimpl.yarn;

import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;
import io.hops.exception.StorageException;
import io.hops.metadata.ndb.ClusterjConnector;
import static io.hops.metadata.ndb.dalimpl.yarn.YarnApplicationsQuotaClusterJ.createMap;
import io.hops.metadata.ndb.wrapper.HopsQuery;
import io.hops.metadata.ndb.wrapper.HopsQueryBuilder;
import io.hops.metadata.ndb.wrapper.HopsQueryDomainType;
import io.hops.metadata.ndb.wrapper.HopsSession;
import io.hops.metadata.yarn.TablesDef;
import static io.hops.metadata.yarn.TablesDef.YarnApplicationsQuotaTableDef.APPLICATIONID;
import static io.hops.metadata.yarn.TablesDef.YarnApplicationsQuotaTableDef.TIMEUSED;
import static io.hops.metadata.yarn.TablesDef.YarnApplicationsQuotaTableDef.TABLE_NAME;
import static io.hops.metadata.yarn.TablesDef.YarnApplicationsQuotaTableDef.BUDGETUSED;
import io.hops.metadata.yarn.dal.YarnApplicationsQuotaDataAccess;
import io.hops.metadata.yarn.entity.YarnApplicationsQuota;
import io.hops.metadata.yarn.entity.YarnApplicationsQuota;
import io.hops.metadata.yarn.entity.YarnApplicationsQuota;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 *
 * @author rizvi
 */
public class YarnApplicationsQuotaClusterJ implements
        TablesDef.YarnApplicationsQuotaTableDef,
        YarnApplicationsQuotaDataAccess<YarnApplicationsQuota> {

    private static final Log LOG = LogFactory.getLog(YarnApplicationsQuotaClusterJ.class);

    @PersistenceCapable(table = TABLE_NAME)
    public interface YarnApplicationsQuotaDTO {

        @PrimaryKey
        @Column(name = APPLICATIONID)
        String getApplicationId();

        void setApplicationId(String projectid);

        @Column(name = TIMEUSED)
        long getTimeUsed();

        void setTimeUsed(long timeUsed);

        @Column(name = BUDGETUSED)
        float getBudgetUsed();

        void setBudgetUsed(float budgetUsed);

    }

    private final ClusterjConnector connector = ClusterjConnector.getInstance();

    @Override
    public Map<String, YarnApplicationsQuota> getAll() throws StorageException {
        LOG.debug("HOP :: ClusterJ YarnApplicationsQuota.getAll - START");
        HopsSession session = connector.obtainSession();
        HopsQueryBuilder qb = session.getQueryBuilder();

        HopsQueryDomainType<YarnApplicationsQuotaClusterJ.YarnApplicationsQuotaDTO> dobj = qb.createQueryDefinition(YarnApplicationsQuotaClusterJ.YarnApplicationsQuotaDTO.class);
        HopsQuery<YarnApplicationsQuotaClusterJ.YarnApplicationsQuotaDTO> query = session.createQuery(dobj);

        List<YarnApplicationsQuotaClusterJ.YarnApplicationsQuotaDTO> queryResults = query.getResultList();
        LOG.debug("HOP :: ClusterJ YarnApplicationsQuota.getAll - STOP");
        Map<String, YarnApplicationsQuota> result = createMap(queryResults);
        session.release(queryResults);
        return result;
    }

    public static Map<String, YarnApplicationsQuota> createMap(List<YarnApplicationsQuotaDTO> results) {
        Map<String, YarnApplicationsQuota> map = new HashMap<String, YarnApplicationsQuota>();
        for (YarnApplicationsQuotaClusterJ.YarnApplicationsQuotaDTO persistable : results) {
            YarnApplicationsQuota hop = createHopYarnApplicationsQuota(persistable);
            map.put(hop.getApplicationId(), hop);
        }
        return map;
    }

    private static YarnApplicationsQuota createHopYarnApplicationsQuota(
            YarnApplicationsQuotaClusterJ.YarnApplicationsQuotaDTO csDTO) {
        YarnApplicationsQuota hop = new YarnApplicationsQuota(csDTO.getApplicationId(), csDTO.getTimeUsed(), csDTO.getBudgetUsed());
        return hop;
    }

    @Override
    public void addAll(Collection<YarnApplicationsQuota> yarnApplicationsQuota) throws StorageException {
    HopsSession session = connector.obtainSession();
    List<YarnApplicationsQuotaClusterJ.YarnApplicationsQuotaDTO> toAdd = new ArrayList<YarnApplicationsQuotaClusterJ.YarnApplicationsQuotaDTO>();
    for (YarnApplicationsQuota _yarnApplicationsQuota : yarnApplicationsQuota) {
        toAdd.add(createPersistable(_yarnApplicationsQuota, session));
    }
    session.savePersistentAll(toAdd);
    //    session.flush();
    session.release(toAdd);

    }
    
    private YarnApplicationsQuotaClusterJ.YarnApplicationsQuotaDTO createPersistable(YarnApplicationsQuota hopPQ, HopsSession session) throws StorageException {
    YarnApplicationsQuotaClusterJ.YarnApplicationsQuotaDTO pqDTO = session.newInstance(YarnApplicationsQuotaClusterJ.YarnApplicationsQuotaDTO.class);
    //Set values to persist new ContainerStatus
    pqDTO.setApplicationId(hopPQ.getApplicationId());
    pqDTO.setTimeUsed(hopPQ.getTimeUsed());
    pqDTO.setBudgetUsed(hopPQ.getBudgetUsed());

    return pqDTO;

  }


}
