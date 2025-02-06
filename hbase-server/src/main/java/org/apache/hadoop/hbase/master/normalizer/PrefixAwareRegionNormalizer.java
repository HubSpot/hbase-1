package org.apache.hadoop.hbase.master.normalizer;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.hubspot.HubSpotCellUtilities;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.hbase.thirdparty.com.google.common.collect.Sets;

@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.CONFIG)
public class PrefixAwareRegionNormalizer extends SimpleRegionNormalizer {
  @Override
  protected boolean shouldMergeRegionIntoRange(SimpleRegionNormalizer.NormalizeContext ctx,
    List<NormalizationTarget> rangeMembers, RegionInfo regionInfo) {
    boolean shouldMergeSimpleRegion =
      super.shouldMergeRegionIntoRange(ctx, rangeMembers, regionInfo);

    if (!shouldMergeSimpleRegion) {
      return false;
    }

    Set<Short> cellsInRange = rangeMembers.stream().map(NormalizationTarget::getRegionInfo).flatMap(
        region -> HubSpotCellUtilities.range(region.getStartKey(), region.getEndKey()).stream())
      .collect(Collectors.toSet());

    Set<Short> regionCells =
      HubSpotCellUtilities.range(regionInfo.getStartKey(), regionInfo.getEndKey());

    if (cellsInRange.isEmpty()) {
      return true;
    }

    return Sets.difference(regionCells, cellsInRange).isEmpty();
  }
}
