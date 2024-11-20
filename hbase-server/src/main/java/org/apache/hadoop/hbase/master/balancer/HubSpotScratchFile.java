package org.apache.hadoop.hbase.master.balancer;

import java.io.IOException;
import java.net.URL;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import com.google.common.math.Quantiles;
import org.apache.hadoop.conf.Configuration;
import org.apache.hbase.thirdparty.com.google.common.math.Stats;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hbase.thirdparty.com.google.common.base.Charsets;
import org.apache.hbase.thirdparty.com.google.common.io.Resources;

public class HubSpotScratchFile {
  private static final Logger LOG = LoggerFactory.getLogger(HubSpotScratchFile.class);

  public static void main(String[] args) throws IOException {
    String file = Resources.readLines(new URL("file:///Users/eszabowexler/Downloads/cluster.json"), Charsets.UTF_8).stream()
      .collect(Collectors.joining("\n"));
    BalancerClusterState state = HubSpotCellCostFunction.OBJECT_MAPPER.fromJson(file, BalancerClusterState.class);


    HubSpotCellCostFunction func =
      new HubSpotCellCostFunction(new Configuration());
    HubSpotCellBasedCandidateGenerator generator = new HubSpotCellBasedCandidateGenerator();

    func.prepare(state);
    double cost = func.cost();
    Set<Integer> movedRegions = new HashSet<>();
    Set<Integer> fromServers = new HashSet<>();
    Set<Integer> toServers = new HashSet<>();
    Set<Integer> repeatMoveRegions = new HashSet<>();

    double lastCost = cost;
    int printFrequency = 500;

    for (int step = 0; step < 200_000; step++) {
      if (step % printFrequency == 0) {
        double costDelta = cost - lastCost;
        lastCost = cost;
        double costPerStep = costDelta / printFrequency;

        List<Integer> size = HubSpotCellBasedCandidateGenerator.computeCellsPerRs(state);
        Map<Integer, Double> quantiles =
          Quantiles.scale(100).indexes(10, 20, 30, 40, 50, 60, 70, 80, 90, 100).compute(size);

        System.out.printf("Step %d --> %.2f - %d regions moved (%d more than once), %d sources, %d targets. Moving %.2f per step, cumulative %.2f drop\t\t\t%.2f\t%.2f\t%.2f\t%.2f\t%.2f\t%.2f\t%.2f\t%.2f\t%.2f\t%.2f\t\n",
          step, cost, movedRegions.size(), repeatMoveRegions.size(), fromServers.size(), toServers.size(), costPerStep, costDelta,
          quantiles.get(10), quantiles.get(20),quantiles.get(30),quantiles.get(40),quantiles.get(50),quantiles.get(60),quantiles.get(70),quantiles.get(80),quantiles.get(90),quantiles.get(100));
      }
      BalanceAction action = generator.generate(state);
      if (action instanceof SwapRegionsAction) {
        SwapRegionsAction swapRegionsAction = (SwapRegionsAction) action;

        if (movedRegions.contains(swapRegionsAction.getFromRegion())) {
          repeatMoveRegions.add(swapRegionsAction.getFromServer());
        }
        if (movedRegions.contains(swapRegionsAction.getToRegion())) {
          repeatMoveRegions.add(swapRegionsAction.getToRegion());
        }

        movedRegions.add(swapRegionsAction.getFromRegion());
        movedRegions.add(swapRegionsAction.getToRegion());
        fromServers.add(swapRegionsAction.getFromServer());
        toServers.add(swapRegionsAction.getToServer());
      }

      state.doAction(action);
      func.postAction(action);
      cost = func.cost();
    }

    LOG.info("{}", state);
  }
}
