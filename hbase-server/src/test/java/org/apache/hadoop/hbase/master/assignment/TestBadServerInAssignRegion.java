/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.master.assignment;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import java.io.IOException;
import java.util.Collection;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionObserver;
import org.apache.hadoop.hbase.exceptions.TimeoutIOException;
import org.apache.hadoop.hbase.procedure2.ProcedureExecutor;
import org.apache.hadoop.hbase.procedure2.ProcedureTestingUtility;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.handler.AssignRegionHandler;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FutureUtils;
import org.apache.hadoop.hbase.util.JVMClusterUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ MasterTests.class, MediumTests.class })
public class TestBadServerInAssignRegion {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestBadServerInAssignRegion.class);

  private static final HBaseTestingUtil UTIL = new HBaseTestingUtil();

  private static final TableName TABLE_NAME = TableName.valueOf("test");

  private static final byte[] CF = Bytes.toBytes("cf");

  @Before
  public void setUp() throws Exception {
    UTIL.getConfiguration().setStrings(CoprocessorHost.REGION_COPROCESSOR_CONF_KEY,
      BadServerSimulator.class.getName());
    Configuration conf = UTIL.getConfiguration();
    UTIL.startMiniCluster(3);
    UTIL.getAdmin().balancerSwitch(false, true);
    UTIL.createTable(TABLE_NAME, CF);
    UTIL.waitTableAvailable(TABLE_NAME);
    BadServerSimulator.BAD_SERVER = getRegionServer(2);
  }

  @After
  public void tearDown() throws Exception {
    UTIL.shutdownMiniCluster();
  }

  @Test
  public void testAssignmentHangsIndefinitely() throws IOException, InterruptedException {
    boolean willSucceed = false;
    AssignRegionHandler.USE_TIMEOUT.set(willSucceed);
    assignRegionToBadHost(willSucceed);
  }

  @Test
  public void testAssignmentSucceeds() throws IOException, InterruptedException {
    boolean willSucceed = true;
    AssignRegionHandler.USE_TIMEOUT.set(willSucceed);
    assignRegionToBadHost(willSucceed);
  }

  private void assignRegionToBadHost(boolean willSucceed) throws IOException, InterruptedException {
    ProcedureExecutor procedureExecutor =
      UTIL.getMiniHBaseCluster().getMaster().getMasterProcedureExecutor();

    JVMClusterUtil.RegionServerThread rsThread = null;
    for (JVMClusterUtil.RegionServerThread t : UTIL.getMiniHBaseCluster()
      .getRegionServerThreads()) {
      if (!t.getRegionServer().getRegions(TABLE_NAME).isEmpty()) {
        rsThread = t;
        break;
      }
    }
    // find the rs and hri of the table, then unassign
    HRegionServer rs = rsThread.getRegionServer();
    RegionInfo hri = rs.getRegions(TABLE_NAME).get(0).getRegionInfo();
    RegionStateNode regionNode = UTIL.getMiniHBaseCluster().getMaster().getAssignmentManager()
      .getRegionStates().getOrCreateRegionStateNode(hri);

    unassignRegion(hri, regionNode, procedureExecutor);

    TransitRegionStateProcedure assignRegionProcedure = TransitRegionStateProcedure.assign(
      UTIL.getMiniHBaseCluster().getMaster().getMasterProcedureExecutor().getEnvironment(), hri,
      BadServerSimulator.BAD_SERVER.getServerName());
    regionNode.setProcedure(assignRegionProcedure);

    long assignProcId = procedureExecutor.submitProcedure(assignRegionProcedure);

    if (willSucceed) {
      ProcedureTestingUtility.waitProcedure(procedureExecutor, assignProcId);
      Thread.sleep(10_000);
      Collection<ServerName> servers = UTIL.getAdmin().getRegionServers();
      assertFalse(servers.contains(BadServerSimulator.BAD_SERVER.getServerName())); // crash procedure should have run
    } else {
      assertThrows(TimeoutIOException.class, () -> FutureUtils.get(
        CompletableFuture.runAsync(
          () -> ProcedureTestingUtility.waitProcedure(procedureExecutor, assignProcId)
        ),
        120, TimeUnit.SECONDS // 120s timeout, but will hang indefinitely
      ));
    }
  }

  private void unassignRegion(RegionInfo hri, RegionStateNode regionNode, ProcedureExecutor procedureExecutor) {
    TransitRegionStateProcedure unassignRegionProcedure = TransitRegionStateProcedure.unassign(
      UTIL.getMiniHBaseCluster().getMaster().getMasterProcedureExecutor().getEnvironment(), hri);
    regionNode.setProcedure(unassignRegionProcedure);
    long unassignProcId = procedureExecutor.submitProcedure(unassignRegionProcedure);
    ProcedureTestingUtility.waitProcedure(procedureExecutor, unassignProcId);
  }

  private static HRegionServer getRegionServer(int index) {
    return UTIL.getMiniHBaseCluster().getRegionServer(index);
  }

  public static class BadServerSimulator implements RegionCoprocessor, RegionObserver {

    private static HRegionServer BAD_SERVER = null;

    /**
     * We override postOpen to simulate a worst case scenario failure: where the
     * region assignment procedure has achieved real side effects, but has failed
     * to finish successfully for some unknown reason.
     */
    @Override
    public void postOpen(ObserverContext<RegionCoprocessorEnvironment> c) {
      if (
        BAD_SERVER != null
        && BAD_SERVER.getServerName().equals(c.getEnvironment().getServerName())
        && c.getEnvironment().getRegionInfo().getTable().equals(TABLE_NAME)
      ) {
        while(true) {
          System.out.println("Bad server can't accept or gracefully decline assignment");
          try {
            Thread.sleep(30_000L);
          } catch (InterruptedException e) {
            throw new RuntimeException(e);
          }
        }
      }
    }

    @Override
    public Optional<RegionObserver> getRegionObserver() {
      return Optional.of(this);
    }
  }
}
