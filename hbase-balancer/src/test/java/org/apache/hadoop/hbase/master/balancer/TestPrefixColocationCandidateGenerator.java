package org.apache.hadoop.hbase.master.balancer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.master.RegionPlan;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestPrefixColocationCandidateGenerator {

  private static final Logger LOG = LoggerFactory.getLogger(StochasticLoadBalancer.class);

  private static final TableName TABLE_NAME = TableName.valueOf("testTable");

  // Fixed prefix cardinality for controlled prefix distribution
  private static final int PREFIX_CARDINALITY = 360;

  private static final int NUM_SERVERS = 300;
  private static final int NUM_REGIONS = 50_000;
  private static final int PREFIX_LENGTH = 3; // arbitrary prefix length

  private static final ServerName[] servers = new ServerName[NUM_SERVERS];
  private static final Map<ServerName, List<RegionInfo>> serverToRegions = new HashMap<>();

  @BeforeClass
  public static void beforeClass() {
    for (int i = 0; i < NUM_SERVERS; i++) {
      servers[i] = ServerName.valueOf("server" + i, i, System.currentTimeMillis());
    }

    Random rnd = new Random(12345L);
    List<RegionInfo> allRegions = new ArrayList<>(NUM_REGIONS);

    for (int r = 0; r < NUM_REGIONS; r++) {
      int prefixVal = r % PREFIX_CARDINALITY;

      // Create the prefixKey based on PREFIX_LENGTH.
      // For a 3-byte prefix:
      byte[] prefixKey = new byte[PREFIX_LENGTH];
      // Encode prefixVal into prefixKey (assuming prefixVal < 65535, it's safe):
      // High-level: just fill from the right
      prefixKey[0] = (byte) ((prefixVal >> 8) & 0xFF);
      prefixKey[1] = (byte) (prefixVal & 0xFF);
      prefixKey[2] = 0x00;  // Since PREFIX_CARDINALITY=360 fits in 2 bytes, the 3rd byte can be 0

      // Construct full startKey with the prefix at the start.
      byte[] startKey = new byte[10];
      System.arraycopy(prefixKey, 0, startKey, 0, PREFIX_LENGTH);
      // Fill the remainder of startKey with random bytes without overwriting the prefix
      for (int i = PREFIX_LENGTH; i < startKey.length; i++) {
        startKey[i] = (byte) rnd.nextInt(256);
      }

      byte[] endKey = new byte[10];
      rnd.nextBytes(endKey);

      RegionInfo regionInfo = RegionInfoBuilder.newBuilder(TABLE_NAME)
        .setStartKey(startKey)
        .setEndKey(endKey)
        .setReplicaId(0)
        .build();
      allRegions.add(regionInfo);
    }

    // Assign all regions to the first server initially
    serverToRegions.put(servers[0], new ArrayList<>(allRegions));
    for (int i = 1; i < NUM_SERVERS; i++) {
      serverToRegions.put(servers[i], new ArrayList<>());
    }
  }

  /**
   * This test sets up a stochastic load balancer without running a mini cluster.
   * This allows us to simulate far more regions and servers than we'd be able to
   * if we were actually managing the mini cluster locally.
   */
  @Test
  public void testCandidateGenerationAtScale() {
    // Set up conditionals
    Configuration conf = new Configuration(false);
    conf.setInt(BalancerConditionals.PREFIX_COLOCATION_LENGTH_KEY, PREFIX_LENGTH);
    conf.setDouble(BalancerConditionals.PREFIX_COLOCATION_MAX_PROPORTION_PER_NODE, (double) NUM_SERVERS / NUM_REGIONS);

    // Set balancer costs to disable all except Region Count Cost
    conf.setFloat("hbase.master.balancer.stochastic.regionCountCost", 1.0f);  // Keep Region Count Cost enabled at weight 1.0
    conf.setFloat("hbase.master.balancer.stochastic.moveCost", 0.0f);         // Disable Move Cost
    conf.setFloat("hbase.master.balancer.stochastic.tableSkewCost", 0.0f);   // Disable Table Skew Cost
    conf.setFloat("hbase.master.balancer.stochastic.localityCost", 0.0f);    // Disable Locality Cost
    conf.setFloat("hbase.master.balancer.stochastic.rackLocalityCost", 0.0f);// Disable Rack Locality Cost
    conf.setFloat("hbase.master.balancer.stochastic.regionReplicaHostCost", 0.0f); // Disable Region Replica Host Cost
    conf.setFloat("hbase.master.balancer.stochastic.regionReplicaRackCost", 0.0f); // Disable Region Replica Rack Cost

    // Generate a mock BalancerClusterState with all regions on one server
    BalancerClusterState cluster = createMockClusterState();
    printClusterDistribution(cluster, 0);

    StochasticLoadBalancer stochasticLoadBalancer = buildStochasticLoadBalancer(cluster, conf);
    boolean needsBalance = true;
    while (needsBalance) {
      List<RegionPlan> regionPlans = stochasticLoadBalancer.balanceTable(TABLE_NAME, serverToRegions);
      if (regionPlans == null) {
        break;
      }
      regionPlans.stream()
        .map(regionPlan -> convertRegionPlanToBalanceAction(cluster, regionPlan))
        .forEach(cluster::doAction);
      serverToRegions.clear();
      for (int i = 0; i < cluster.numServers; i++) {
        List<RegionInfo> regions = new ArrayList<>();
        for (int regionIdx : cluster.regionsPerServer[i]) {
          regions.add(cluster.regions[regionIdx]);
        }
        serverToRegions.put(cluster.servers[i], regions);
      }
      printClusterDistribution(cluster, regionPlans.size());
      needsBalance = stochasticLoadBalancer.needsBalance(TABLE_NAME, cluster);
    }
  }

  private StochasticLoadBalancer buildStochasticLoadBalancer(BalancerClusterState cluster, Configuration conf) {
    StochasticLoadBalancer stochasticLoadBalancer = new StochasticLoadBalancer();
    stochasticLoadBalancer.loadConf(conf);
    stochasticLoadBalancer.initCosts(cluster);
    stochasticLoadBalancer.setClusterInfoProvider(new DummyClusterInfoProvider(conf));
    return stochasticLoadBalancer;
  }

  private static MoveRegionAction convertRegionPlanToBalanceAction(BalancerClusterState cluster, RegionPlan regionPlan) {
    int fromServer = cluster.serversToIndex.get(regionPlan.getSource().getAddress());
    int toServer = cluster.serversToIndex.get(regionPlan.getDestination().getAddress());
    int regionIndex = cluster.regionsToIndex.get(regionPlan.getRegionInfo());
    return new MoveRegionAction(regionIndex, fromServer, toServer);
  }

  /**
   * Prints the current cluster distribution of regions per server, including unique prefix counts.
   */
  private void printClusterDistribution(BalancerClusterState cluster, long actionsTaken) {
    LOG.info("=== Cluster Distribution at {} actions ===", actionsTaken);

    // Retrieve the conditional to get prefix distribution info
    PrefixColocationConditional conditional = BalancerConditionals.INSTANCE.getPrefixColocationConditional();

    Map<Integer, Set<PrefixColocationConditional.PrefixByteArrayWrapper>> serverIndexToPrefixes = null;
    if (conditional != null) {
      serverIndexToPrefixes = conditional.getServerIndexToPrefixes();
    }

    for (int i = 0; i < cluster.numServers; i++) {
      int[] regions = cluster.regionsPerServer[i];
      int regionCount = (regions == null) ? 0 : regions.length;

      int prefixCount = 0;
      if (serverIndexToPrefixes != null && serverIndexToPrefixes.containsKey(i)) {
        prefixCount = serverIndexToPrefixes.get(i).size();
      }

      LOG.info("Server {}: {} regions, {} unique prefixes",
        cluster.servers[i].getServerName(),
        regionCount,
        prefixCount);
    }

    LOG.info("===========================================");
  }

  private BalancerClusterState createMockClusterState() {
    Assert.assertEquals("Assigned region count should match the total number of regions",
      NUM_REGIONS, serverToRegions.get(servers[0]).size());

    return new BalancerClusterState(serverToRegions, null, null, null, null);
  }

}
