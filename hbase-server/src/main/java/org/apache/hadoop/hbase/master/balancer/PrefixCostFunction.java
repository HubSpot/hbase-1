package org.apache.hadoop.hbase.master.balancer;

import java.util.Arrays;
import java.util.Set;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.hubspot.HubSpotCellUtilities;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@InterfaceAudience.Private abstract class PrefixCostFunction extends CostFunction {

  private static final Logger LOG = LoggerFactory.getLogger(PrefixCostFunction.class);

  public static final String PREFIX_DISPERSION =
    "hbase.master.balancer.stochastic.prefixDispersion";

  public static final float DEFAULT_PREFIX_DISPERSION = 0.75f;

  private float targetPrefixDispersion = 0.0f;
  private int targetPrefixCountPerServer;

  private double[] serverCosts;

  private boolean costUpdated = false;
  private double cost;

  private Set<TableName> tableNames;

  void emitClusterState() {
    if (LOG.isTraceEnabled() && isNeeded() && cluster.regions != null
      && cluster.regions.length > 0) {
      try {
        LOG.trace("{} cluster state @ target dispersion of {} ({} per server):\n{}", cluster.tables,
          targetPrefixDispersion, targetPrefixCountPerServer,
          HubSpotCellUtilities.OBJECT_MAPPER.toJson(cluster));
      } catch (Exception ex) {
        LOG.error("Failed to write cluster state", ex);
      }
    }
  }

  void setTargetPrefixDispersion(float dispersion) {
    this.targetPrefixDispersion = dispersion;
  }

  @Override void prepare(final BalancerClusterState cluster) {
    super.prepare(cluster);
    Arrays.stream(cluster.regions).forEach(region -> tableNames.add(region.getTable()));

    float averageRegionsPerServer = (float) cluster.numRegions / cluster.numServers;
    targetPrefixCountPerServer =
      Math.max(1, Math.round(averageRegionsPerServer * targetPrefixDispersion));

    serverCosts = new double[cluster.numServers];
    for (int server = 0; server < serverCosts.length; server++) {
      serverCosts[server] = computeServerCost(server);
    }
    costUpdated = true;

    double startingCost = cost();
    if (LOG.isDebugEnabled()) {
      LOG.debug("Preparing {} for {}, dispersion of {}, {} total regions, "
          + "average of {} regions/server, target prefix count per server is {}. Initial cluster cost is {}",
        getClass().getSimpleName(), cluster.tables, String.format("%.2f", targetPrefixDispersion),
        cluster.numRegions, String.format("%.2f", averageRegionsPerServer),
        targetPrefixCountPerServer, startingCost);
    }

    emitClusterState();
  }

  @Override boolean isNeeded() {
    return getMultiplier() > 0.0 && tableNames.stream().noneMatch(TableName::isSystemTable);
  }

  private double computeServerCost(int server) {
    int distinctPrefixes = (int) Arrays.stream(cluster.regionsPerServer[server])
      .mapToObj(regionIdx -> cluster.regions[regionIdx]).flatMap(
        region -> HubSpotCellUtilities.toCells(region.getStartKey(), region.getEndKey(),
          HubSpotCellUtilities.MAX_CELL_COUNT).stream()).distinct().count();
    double serverDispersion = (double) distinctPrefixes / cluster.regionsPerServer[server].length;

    return computeServerCost(serverDispersion, targetPrefixDispersion);
  }

  abstract double computeServerCost(double serverDispersion, double targetDispersion);

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
