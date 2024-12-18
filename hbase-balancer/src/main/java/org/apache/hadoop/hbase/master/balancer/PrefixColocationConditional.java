package org.apache.hadoop.hbase.master.balancer;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.master.RegionPlan;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PrefixColocationConditional extends RegionPlanConditional {

  private static final Logger LOG = LoggerFactory.getLogger(PrefixColocationConditional.class);

  private final BalancerClusterState cluster;
  private final int prefixLength;
  private final long prefixCardinality;
  private final double maxProportionPerNode;
  private final Map<Integer, Set<PrefixByteArrayWrapper>> serverIndexToPrefixes;
  private final Map<PrefixByteArrayWrapper, Set<Integer>> prefixToServerIndexes = new HashMap<>();

  // New configuration key for prefix distribution
  // This defines how many different servers we want each prefix to be on.
  public static final String PREFIX_DISTRIBUTION_FACTOR_KEY = "hbase.master.balancer.prefix.distribution.factor";
  public static final int PREFIX_DISTRIBUTION_FACTOR_DEFAULT = 2;
  private final int desiredPrefixDistribution;

  public PrefixColocationConditional(Configuration conf, BalancerClusterState cluster) {
    super(conf, cluster);
    this.cluster = cluster;
    this.prefixLength = conf.getInt(
      BalancerConditionals.PREFIX_COLOCATION_LENGTH_KEY,
      BalancerConditionals.PREFIX_COLOCATION_LENGTH_DEFAULT);

    this.maxProportionPerNode = Math.min(1.0, conf.getDouble(
      BalancerConditionals.PREFIX_COLOCATION_MAX_PROPORTION_PER_NODE,
      BalancerConditionals.PREFIX_COLOCATION_MAX_PROPORTION_PER_NODE_DEFAULT
    ));

    // Initialize desired prefix distribution factor
    this.desiredPrefixDistribution = conf.getInt(
      PREFIX_DISTRIBUTION_FACTOR_KEY,
      PREFIX_DISTRIBUTION_FACTOR_DEFAULT
    );

    Set<PrefixByteArrayWrapper> allPrefixes = new HashSet<>();
    if (prefixLength == BalancerConditionals.PREFIX_COLOCATION_LENGTH_DEFAULT) {
      // turned off by default
      this.serverIndexToPrefixes = Collections.emptyMap();
    } else {
      if (maxProportionPerNode < 0) {
        LOG.warn("You must configure {} and {} to use prefix colocation balancing",
          BalancerConditionals.PREFIX_COLOCATION_LENGTH_KEY,
          BalancerConditionals.PREFIX_COLOCATION_MAX_PROPORTION_PER_NODE);
        this.serverIndexToPrefixes = Collections.emptyMap();
        this.prefixCardinality = -1;
        return;
      }
      if (maxProportionPerNode == 1.0) {
        LOG.warn("You have configured {} to 1.0 (the maximum). This conditional will have no effect, likely not what you want.",
          BalancerConditionals.PREFIX_COLOCATION_MAX_PROPORTION_PER_NODE);
        this.serverIndexToPrefixes = Collections.emptyMap();
        this.prefixCardinality = -1;
        return;
      }
      this.serverIndexToPrefixes = new HashMap<>(cluster.numServers);
      for (int serverIdx = 0; serverIdx < cluster.regionsPerServer.length; serverIdx++) {
        serverIndexToPrefixes.put(serverIdx, new HashSet<>());
        int[] regionsOnServer = cluster.regionsPerServer[serverIdx];
        for (int regionIdx : regionsOnServer) {
          RegionInfo regionInfo = cluster.regions[regionIdx];
          if (regionInfo.getTable().isSystemTable()) {
            continue;
          }
          byte[] startKeyPrefix = getPrefix(regionInfo, prefixLength);
          PrefixByteArrayWrapper wrapper = new PrefixByteArrayWrapper(startKeyPrefix);
          serverIndexToPrefixes.get(serverIdx).add(wrapper);
          prefixToServerIndexes.computeIfAbsent(wrapper, k -> new HashSet<>()).add(serverIdx);
          allPrefixes.add(wrapper);
        }
      }
    }
    this.prefixCardinality = allPrefixes.size();
  }

  Map<Integer, Set<PrefixByteArrayWrapper>> getServerIndexToPrefixes() {
    return serverIndexToPrefixes;
  }

  Map<PrefixByteArrayWrapper, Set<Integer>> getPrefixToServerIndexes() {
    return prefixToServerIndexes;
  }

  @Override
  boolean isViolating(RegionPlan regionPlan) {
    if (prefixLength == BalancerConditionals.PREFIX_COLOCATION_LENGTH_DEFAULT) {
      return false;
    }
    int destinationServerIdx = cluster.serversToIndex.get(regionPlan.getDestination().getAddress());
    Set<PrefixByteArrayWrapper> destinationPrefixes = serverIndexToPrefixes.get(destinationServerIdx);
    if (destinationPrefixes == null) {
      return false;
    }

    double maxPrefixCount = Math.ceil(maxProportionPerNode * prefixCardinality);
    byte[] startPrefix = getPrefix(regionPlan.getRegionInfo(), prefixLength);
    PrefixByteArrayWrapper prefixWrapper = new PrefixByteArrayWrapper(startPrefix);

    int currentUniquePrefixCount = destinationPrefixes.size();
    int newUniquePrefixCount = destinationPrefixes.contains(prefixWrapper) ? currentUniquePrefixCount : currentUniquePrefixCount + 1;

    // If adding this prefix does not cause us to exceed the max proportion per node, no violation.
    if (newUniquePrefixCount <= maxPrefixCount) {
      return false;
    }

    // Now, if this would exceed the proportion limit, let's consider distribution.
    Set<Integer> serverSetForPrefix = prefixToServerIndexes.get(prefixWrapper);
    int currentDistributionCount = (serverSetForPrefix == null) ? 0 : serverSetForPrefix.size();

    // If the prefix is currently under-distributed (fewer servers than desired),
    // we encourage distribution by not labeling this as a violation.
    if (currentDistributionCount < desiredPrefixDistribution) {
      // Adding this prefix to another server actually helps distribution,
      // so we do not consider this a violation.
      return false;
    }

    // Otherwise, the prefix is already well-distributed, and placing another region of this prefix
    // here would exceed the desired proportion of unique prefixes on the node, so it's a violation.
    return true;
  }

  double getPrefixProportion(int prefixCountOnNode) {
    if (prefixCardinality == 0 || prefixCardinality == -1) {
      return 0;
    }
    return (double) prefixCountOnNode / prefixCardinality;
  }

  boolean isTooManyPrefixesOnNode(int prefixCountOnNode) {
    return getPrefixProportion(prefixCountOnNode) > maxProportionPerNode;
  }

  PrefixByteArrayWrapper getPrefix(RegionInfo regionInfo) {
    return new PrefixByteArrayWrapper(getPrefix(regionInfo, prefixLength));
  }

  public static byte[] getPrefix(RegionInfo regionInfo, int prefixLength) {
    if (regionInfo.getStartKey() == null || regionInfo.getStartKey().length == 0) {
      if (regionInfo.getEndKey() == null || regionInfo.getEndKey().length == 0) {
        return new byte[0];
      } else {
        return getPrefix(regionInfo.getEndKey(), prefixLength);
      }
    }
    return getPrefix(regionInfo.getStartKey(), prefixLength);
  }

  private static byte[] getPrefix(byte[] regionBoundary, int prefixLength) {
    byte[] prefix = new byte[Math.min(prefixLength, regionBoundary.length)];
    System.arraycopy(regionBoundary, 0, prefix, 0, prefix.length);
    return prefix;
  }

  static class PrefixByteArrayWrapper {
    private final byte[] prefix;

    public PrefixByteArrayWrapper(byte[] prefix) {
      this.prefix = Arrays.copyOf(prefix, prefix.length);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (!(o instanceof PrefixByteArrayWrapper)) return false;
      PrefixByteArrayWrapper other = (PrefixByteArrayWrapper) o;
      return Arrays.equals(this.prefix, other.prefix);
    }

    @Override
    public int hashCode() {
      return Arrays.hashCode(prefix);
    }

    public byte[] getPrefix() {
      return Arrays.copyOf(prefix, prefix.length);
    }
  }
}
