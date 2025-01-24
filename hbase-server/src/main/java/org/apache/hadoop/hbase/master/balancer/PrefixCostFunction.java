package org.apache.hadoop.hbase.master.balancer;

import java.util.Arrays;
import org.apache.hadoop.hbase.hubspot.HubSpotCellUtilities;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@InterfaceAudience.Private abstract class PrefixCostFunction extends CostFunction {

  private static final Logger LOG = LoggerFactory.getLogger(PrefixCostFunction.class);

  private float targetIsolationToPerformanceRatio = 0.5f;
  private int targetPrefixCountPerServer;

  private double[] serverCosts;

  private boolean costUpdated = false;
  private double cost;

  void emitClusterState() {
    if (LOG.isTraceEnabled() && isNeeded() && cluster.regions != null
      && cluster.regions.length > 0) {
      try {
        LOG.trace("{} cluster state:\n{}", cluster.tables,
          HubSpotCellUtilities.OBJECT_MAPPER.toJson(cluster));
      } catch (Exception ex) {
        LOG.error("Failed to write cluster state", ex);
      }
    }
  }

  @Override void prepare(BalancerClusterState cluster) {
    super.prepare(cluster);
    targetPrefixCountPerServer = Math.round(
      (float) cluster.numRegions / cluster.numServers * targetIsolationToPerformanceRatio);

    serverCosts = new double[cluster.numServers];
    for (int server = 0; server < serverCosts.length; server++) {
      serverCosts[server] = computeServerCost(server);
    }
    costUpdated = true;
  }

  private double computeServerCost(int server) {
    int distinctPrefixes = (int) Arrays.stream(cluster.regionsPerServer[server])
      .mapToObj(regionIdx -> cluster.regions[regionIdx]).flatMap(
        region -> HubSpotCellUtilities.toCells(region.getStartKey(), region.getEndKey(),
          HubSpotCellUtilities.MAX_CELL_COUNT).stream()).distinct().count();
    double serverRatio = (double) distinctPrefixes / cluster.regionsPerServer[server].length;

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
