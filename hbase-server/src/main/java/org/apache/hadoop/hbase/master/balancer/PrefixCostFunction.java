package org.apache.hadoop.hbase.master.balancer;

import java.util.Arrays;
import org.apache.hadoop.hbase.hubspot.HubSpotCellUtilities;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@InterfaceAudience.Private abstract class PrefixCostFunction extends CostFunction {

  private static final Logger LOG = LoggerFactory.getLogger(PrefixCostFunction.class);

  public static final String PREFIX_ISOLATION_TO_PERFORMANCE_RATIO =
    "hbase.master.balancer.stochastic.prefixIsolationToPerformanceRatio";

  public static final float DEFAULT_PREFIX_ISOLATION_TO_PERFORMANCE_RATIO = 0;

  private float targetIsolationToPerformanceRatio = 0.0f;
  private int targetPrefixCountPerServer;

  private double[] serverCosts;

  private boolean costUpdated = false;
  private double cost;

  void emitClusterState() {
    if (LOG.isTraceEnabled() && isNeeded() && cluster.regions != null
      && cluster.regions.length > 0) {
      try {
        LOG.trace("{} cluster state @ target isolation:performance of {} ({} per server):\n{}",
          cluster.tables,
          targetIsolationToPerformanceRatio,
          targetPrefixCountPerServer,
          HubSpotCellUtilities.OBJECT_MAPPER.toJson(cluster));
      } catch (Exception ex) {
        LOG.error("Failed to write cluster state", ex);
      }
    }
  }

  void setIsolationToPerformanceRatio(float ratio) {
    this.targetIsolationToPerformanceRatio = ratio;
  }

  @Override void prepare(BalancerClusterState cluster) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Preparing {}, isolation:performance ratio of {}, target prefix count per server is {}",
        getClass().getSimpleName(), String.format("%.2f", targetIsolationToPerformanceRatio), targetPrefixCountPerServer);
    }
    super.prepare(cluster);
    targetPrefixCountPerServer = Math.max(
      1,
      Math.round((float) cluster.numRegions / cluster.numServers * (1.0f - targetIsolationToPerformanceRatio))
    );

    serverCosts = new double[cluster.numServers];
    for (int server = 0; server < serverCosts.length; server++) {
      serverCosts[server] = computeServerCost(server);
    }
    costUpdated = true;
    emitClusterState();
  }

  private double computeServerCost(int server) {
    int distinctPrefixes = (int) Arrays.stream(cluster.regionsPerServer[server])
      .mapToObj(regionIdx -> cluster.regions[regionIdx]).flatMap(
        region -> HubSpotCellUtilities.toCells(region.getStartKey(), region.getEndKey(),
          HubSpotCellUtilities.MAX_CELL_COUNT).stream()).distinct().count();
    double serverRatio = 1.0f - (double) distinctPrefixes / cluster.regionsPerServer[server].length;

    return computeServerCost(serverRatio, targetIsolationToPerformanceRatio);
  }

  abstract double computeServerCost(double serverRatio, double targetRatio);

  @Override protected void regionMoved(int region, int oldServer, int newServer) {
    // recompute the stat for the given two region servers
    serverCosts[oldServer] = computeServerCost(oldServer);
    serverCosts[newServer] = computeServerCost(newServer);
    costUpdated = true;
  }

  @Override protected final double cost() {
    if (!costUpdated) {
      return cost;
    }

    cost = Arrays.stream(serverCosts).sum() / cluster.numServers;
    costUpdated = false;

    return cost;
  }
}
