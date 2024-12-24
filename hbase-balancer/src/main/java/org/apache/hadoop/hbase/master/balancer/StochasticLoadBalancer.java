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
package org.apache.hadoop.hbase.master.balancer;

import com.google.errorprone.annotations.RestrictedApi;
import java.lang.reflect.Constructor;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Supplier;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ClusterMetrics;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.RegionMetrics;
import org.apache.hadoop.hbase.ServerMetrics;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.BalancerDecision;
import org.apache.hadoop.hbase.client.BalancerRejection;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.master.RackManager;
import org.apache.hadoop.hbase.master.RegionPlan;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.ReflectionUtils;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>
 * This is a best effort load balancer. Given a Cost function F(C) =&gt; x It will randomly try and
 * mutate the cluster to Cprime. If F(Cprime) &lt; F(C) then the new cluster state becomes the plan.
 * It includes costs functions to compute the cost of:
 * </p>
 * <ul>
 * <li>Region Load</li>
 * <li>Table Load</li>
 * <li>Data Locality</li>
 * <li>Memstore Sizes</li>
 * <li>Storefile Sizes</li>
 * </ul>
 * <p>
 * Every cost function returns a number between 0 and 1 inclusive; where 0 is the lowest cost best
 * solution, and 1 is the highest possible cost and the worst solution. The computed costs are
 * scaled by their respective multipliers:
 * </p>
 * <ul>
 * <li>hbase.master.balancer.stochastic.regionLoadCost</li>
 * <li>hbase.master.balancer.stochastic.moveCost</li>
 * <li>hbase.master.balancer.stochastic.tableLoadCost</li>
 * <li>hbase.master.balancer.stochastic.localityCost</li>
 * <li>hbase.master.balancer.stochastic.memstoreSizeCost</li>
 * <li>hbase.master.balancer.stochastic.storefileSizeCost</li>
 * </ul>
 * <p>
 * You can also add custom Cost function by setting the the following configuration value:
 * </p>
 * <ul>
 * <li>hbase.master.balancer.stochastic.additionalCostFunctions</li>
 * </ul>
 * <p>
 * All custom Cost Functions needs to extends {@link CostFunction}
 * </p>
 * <p>
 * In addition to the above configurations, the balancer can be tuned by the following configuration
 * values:
 * </p>
 * <ul>
 * <li>hbase.master.balancer.stochastic.maxMoveRegions which controls what the max number of regions
 * that can be moved in a single invocation of this balancer.</li>
 * <li>hbase.master.balancer.stochastic.stepsPerRegion is the coefficient by which the number of
 * regions is multiplied to try and get the number of times the balancer will mutate all
 * servers.</li>
 * <li>hbase.master.balancer.stochastic.maxSteps which controls the maximum number of times that the
 * balancer will try and mutate all the servers. The balancer will use the minimum of this value and
 * the above computation.</li>
 * </ul>
 * <p>
 * This balancer is best used with hbase.master.loadbalance.bytable set to false so that the
 * balancer gets the full picture of all loads on the cluster.
 * </p>
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.CONFIG)
public class StochasticLoadBalancer extends BaseLoadBalancer {

  private static final Logger LOG = LoggerFactory.getLogger(StochasticLoadBalancer.class);

  protected static final String STEPS_PER_REGION_KEY =
    "hbase.master.balancer.stochastic.stepsPerRegion";
  protected static final int DEFAULT_STEPS_PER_REGION = 800;
  protected static final String MAX_STEPS_KEY = "hbase.master.balancer.stochastic.maxSteps";
  protected static final int DEFAULT_MAX_STEPS = 1000000;
  protected static final String RUN_MAX_STEPS_KEY = "hbase.master.balancer.stochastic.runMaxSteps";
  protected static final boolean DEFAULT_RUN_MAX_STEPS = false;
  protected static final String MAX_RUNNING_TIME_KEY =
    "hbase.master.balancer.stochastic.maxRunningTime";
  protected static final long DEFAULT_MAX_RUNNING_TIME = 30 * 1000; // 30 seconds.
  protected static final String KEEP_REGION_LOADS =
    "hbase.master.balancer.stochastic.numRegionLoadsToRemember";
  protected static final int DEFAULT_KEEP_REGION_LOADS = 15;
  private static final String TABLE_FUNCTION_SEP = "_";
  protected static final String MIN_COST_NEED_BALANCE_KEY =
    "hbase.master.balancer.stochastic.minCostNeedBalance";
  protected static final float DEFAULT_MIN_COST_NEED_BALANCE = 0.025f;
  protected static final String COST_FUNCTIONS_COST_FUNCTIONS_KEY =
    "hbase.master.balancer.stochastic.additionalCostFunctions";
  public static final String OVERALL_COST_FUNCTION_NAME = "Overall";

  Map<String, Deque<BalancerRegionLoad>> loads = new HashMap<>();

  // values are defaults
  private int maxSteps = DEFAULT_MAX_STEPS;
  private boolean runMaxSteps = DEFAULT_RUN_MAX_STEPS;
  private int stepsPerRegion = DEFAULT_STEPS_PER_REGION;
  private long maxRunningTime = DEFAULT_MAX_RUNNING_TIME;
  private int numRegionLoadsToRemember = DEFAULT_KEEP_REGION_LOADS;
  private float minCostNeedBalance = DEFAULT_MIN_COST_NEED_BALANCE;
  Map<String, Pair<ServerName, Float>> regionCacheRatioOnOldServerMap = new HashMap<>();

  protected List<CostFunction> costFunctions; // FindBugs: Wants this protected;
                                              // IS2_INCONSISTENT_SYNC
  // To save currently configed sum of multiplier. Defaulted at 1 for cases that carry high cost
  private float sumMultiplier;
  // to save and report costs to JMX
  private double curOverallCost = 0d;
  private double[] tempFunctionCosts;
  private double[] curFunctionCosts;

  // Keep locality based picker and cost function to alert them
  // when new services are offered
  private LocalityBasedCandidateGenerator localityCandidateGenerator;
  private ServerLocalityCostFunction localityCost;
  private RackLocalityCostFunction rackLocalityCost;
  private RegionReplicaHostCostFunction regionReplicaHostCostFunction;
  private RegionReplicaRackCostFunction regionReplicaRackCostFunction;

  protected Map<Class<? extends CandidateGenerator>, CandidateGenerator> candidateGenerators;
  private Map<Class<? extends CandidateGenerator>, Double> weightsOfGenerators;

  public enum GeneratorType {
    RANDOM,
    LOAD,
    LOCALITY,
    RACK,
    SYSTEM_TABLE_ISOLATION,
    META_TABLE_ISOLATION,
  }

  private final BalancerConditionals balancerConditionals = BalancerConditionals.INSTANCE;

  /**
   * The constructor that pass a MetricsStochasticBalancer to BaseLoadBalancer to replace its
   * default MetricsBalancer
   */
  public StochasticLoadBalancer() {
    super(new MetricsStochasticBalancer());
  }

  @RestrictedApi(explanation = "Should only be called in tests", link = "",
      allowedOnPath = ".*/src/test/.*")
  public StochasticLoadBalancer(MetricsStochasticBalancer metricsStochasticBalancer) {
    super(metricsStochasticBalancer);
  }

  private static CostFunction createCostFunction(Class<? extends CostFunction> clazz,
    Configuration conf) {
    try {
      Constructor<? extends CostFunction> ctor = clazz.getDeclaredConstructor(Configuration.class);
      return ReflectionUtils.instantiate(clazz.getName(), ctor, conf);
    } catch (NoSuchMethodException e) {
      // will try construct with no parameter
    }
    return ReflectionUtils.newInstance(clazz);
  }

  private void loadCustomCostFunctions(Configuration conf) {
    String[] functionsNames = conf.getStrings(COST_FUNCTIONS_COST_FUNCTIONS_KEY);

    if (null == functionsNames) {
      return;
    }
    for (String className : functionsNames) {
      Class<? extends CostFunction> clazz;
      try {
        clazz = Class.forName(className).asSubclass(CostFunction.class);
      } catch (ClassNotFoundException e) {
        LOG.warn("Cannot load class '{}': {}", className, e.getMessage());
        continue;
      }
      CostFunction func = createCostFunction(clazz, conf);
      LOG.info("Successfully loaded custom CostFunction '{}'", func.getClass().getSimpleName());
      costFunctions.add(func);
    }
  }

  @RestrictedApi(explanation = "Should only be called in tests", link = "",
      allowedOnPath = ".*/src/test/.*")
  Map<Class<? extends CandidateGenerator>, CandidateGenerator> getCandidateGenerators() {
    return this.candidateGenerators;
  }

  protected Map<Class<? extends CandidateGenerator>, CandidateGenerator>
    createCandidateGenerators() {
    Map<Class<? extends CandidateGenerator>, CandidateGenerator> candidateGenerators =
      new HashMap<>(4 + balancerConditionals.getConditionals().size());
    candidateGenerators.put(RandomCandidateGenerator.class, new RandomCandidateGenerator());
    candidateGenerators.put(LoadCandidateGenerator.class, new LoadCandidateGenerator());
    candidateGenerators.put(LocalityBasedCandidateGenerator.class, localityCandidateGenerator);
    candidateGenerators.put(RegionReplicaCandidateGenerator.class,
      new RegionReplicaRackCandidateGenerator());
    for (RegionPlanConditional conditional : balancerConditionals.getConditionals()) {
      conditional.getCandidateGenerator().ifPresent(g -> candidateGenerators.put(g.getClass(), g));
    }
    return candidateGenerators;
  }

  protected List<CostFunction> createCostFunctions(Configuration conf) {
    List<CostFunction> costFunctions = new ArrayList<>();
    addCostFunction(costFunctions, new RegionCountSkewCostFunction(conf));
    addCostFunction(costFunctions, new PrimaryRegionCountSkewCostFunction(conf));
    addCostFunction(costFunctions, new MoveCostFunction(conf, provider));
    addCostFunction(costFunctions, localityCost);
    addCostFunction(costFunctions, rackLocalityCost);
    addCostFunction(costFunctions, new TableSkewCostFunction(conf));
    addCostFunction(costFunctions, regionReplicaHostCostFunction);
    addCostFunction(costFunctions, regionReplicaRackCostFunction);
    addCostFunction(costFunctions, new ReadRequestCostFunction(conf));
    addCostFunction(costFunctions, new CPRequestCostFunction(conf));
    addCostFunction(costFunctions, new WriteRequestCostFunction(conf));
    addCostFunction(costFunctions, new MemStoreSizeCostFunction(conf));
    addCostFunction(costFunctions, new StoreFileCostFunction(conf));
    return costFunctions;
  }

  @Override
  protected void loadConf(Configuration conf) {
    super.loadConf(conf);
    maxSteps = conf.getInt(MAX_STEPS_KEY, DEFAULT_MAX_STEPS);
    stepsPerRegion = conf.getInt(STEPS_PER_REGION_KEY, DEFAULT_STEPS_PER_REGION);
    maxRunningTime = conf.getLong(MAX_RUNNING_TIME_KEY, DEFAULT_MAX_RUNNING_TIME);
    runMaxSteps = conf.getBoolean(RUN_MAX_STEPS_KEY, DEFAULT_RUN_MAX_STEPS);

    numRegionLoadsToRemember = conf.getInt(KEEP_REGION_LOADS, DEFAULT_KEEP_REGION_LOADS);
    minCostNeedBalance = conf.getFloat(MIN_COST_NEED_BALANCE_KEY, DEFAULT_MIN_COST_NEED_BALANCE);
    localityCandidateGenerator = new LocalityBasedCandidateGenerator();
    localityCost = new ServerLocalityCostFunction(conf);
    rackLocalityCost = new RackLocalityCostFunction(conf);

    // Order is important here. We need to construct conditionals to load candidate generators
    balancerConditionals.loadConf(conf);
    this.candidateGenerators = createCandidateGenerators();

    regionReplicaHostCostFunction = new RegionReplicaHostCostFunction(conf);
    regionReplicaRackCostFunction = new RegionReplicaRackCostFunction(conf);
    this.costFunctions = createCostFunctions(conf);
    loadCustomCostFunctions(conf);

    curFunctionCosts = new double[costFunctions.size()];
    tempFunctionCosts = new double[costFunctions.size()];

    LOG.info("Loaded config; maxSteps=" + maxSteps + ", runMaxSteps=" + runMaxSteps
      + ", stepsPerRegion=" + stepsPerRegion + ", maxRunningTime=" + maxRunningTime + ", isByTable="
      + isByTable + ", CostFunctions=" + Arrays.toString(getCostFunctionNames())
      + " , sum of multiplier of cost functions = " + sumMultiplier + " etc.");
  }

  @Override
  public void updateClusterMetrics(ClusterMetrics st) {
    super.updateClusterMetrics(st);
    updateRegionLoad();

    // update metrics size
    try {
      // by-table or ensemble mode
      int tablesCount = isByTable ? provider.getNumberOfTables() : 1;
      int functionsCount = getCostFunctionNames().length;

      updateMetricsSize(tablesCount * (functionsCount + 1)); // +1 for overall
    } catch (Exception e) {
      LOG.error("failed to get the size of all tables", e);
    }
  }

  private void updateBalancerTableLoadInfo(TableName tableName,
    Map<ServerName, List<RegionInfo>> loadOfOneTable) {
    RegionHDFSBlockLocationFinder finder = null;
    if ((this.localityCost != null) || (this.rackLocalityCost != null)) {
      finder = this.regionFinder;
    }
    BalancerClusterState cluster =
      new BalancerClusterState(loadOfOneTable, loads, finder, rackManager);

    initCosts(cluster);
    curOverallCost = computeCost(cluster, Double.MAX_VALUE);
    System.arraycopy(tempFunctionCosts, 0, curFunctionCosts, 0, curFunctionCosts.length);
    updateStochasticCosts(tableName, curOverallCost, curFunctionCosts);
  }

  @Override
  public void
    updateBalancerLoadInfo(Map<TableName, Map<ServerName, List<RegionInfo>>> loadOfAllTable) {
    if (isByTable) {
      loadOfAllTable.forEach((tableName, loadOfOneTable) -> {
        updateBalancerTableLoadInfo(tableName, loadOfOneTable);
      });
    } else {
      updateBalancerTableLoadInfo(HConstants.ENSEMBLE_TABLE_NAME,
        toEnsumbleTableLoad(loadOfAllTable));
    }
  }

  /**
   * Update the number of metrics that are reported to JMX
   */
  @RestrictedApi(explanation = "Should only be called in tests", link = "",
      allowedOnPath = ".*(/src/test/.*|StochasticLoadBalancer).java")
  void updateMetricsSize(int size) {
    if (metricsBalancer instanceof MetricsStochasticBalancer) {
      ((MetricsStochasticBalancer) metricsBalancer).updateMetricsSize(size);
    }
  }

  private boolean areSomeRegionReplicasColocated(BalancerClusterState c) {
    if (balancerConditionals.isReplicaDistributionEnabled()) {
      // This check is unnecessary with conditional replica distribution
      // The balancer will auto-run if the replica distribution candidates are available
      return false;
    }
    regionReplicaHostCostFunction.prepare(c);
    return (Math.abs(regionReplicaHostCostFunction.cost()) > CostFunction.COST_EPSILON);
  }

  private String getBalanceReason(double total, double sumMultiplier) {
    if (total <= 0) {
      return "(cost1*multiplier1)+(cost2*multiplier2)+...+(costn*multipliern) = " + total + " <= 0";
    } else if (sumMultiplier <= 0) {
      return "sumMultiplier = " + sumMultiplier + " <= 0";
    } else if ((total / sumMultiplier) < minCostNeedBalance) {
      return "[(cost1*multiplier1)+(cost2*multiplier2)+...+(costn*multipliern)]/sumMultiplier = "
        + (total / sumMultiplier) + " <= minCostNeedBalance(" + minCostNeedBalance + ")";
    } else {
      return "";
    }
  }

  @RestrictedApi(explanation = "Should only be called in tests", link = "",
      allowedOnPath = ".*(/src/test/.*|StochasticLoadBalancer).java")
  boolean needsBalance(TableName tableName, BalancerClusterState cluster) {
    ClusterLoadState cs = new ClusterLoadState(cluster.clusterState);
    if (cs.getNumServers() < MIN_SERVER_BALANCE) {
      LOG.info(
        "Not running balancer because only " + cs.getNumServers() + " active regionserver(s)");
      sendRejectionReasonToRingBuffer(() -> "The number of RegionServers " + cs.getNumServers()
        + " < MIN_SERVER_BALANCE(" + MIN_SERVER_BALANCE + ")", null);
      return false;
    }
    if (areSomeRegionReplicasColocated(cluster)) {
      LOG.info("Running balancer because at least one server hosts replicas of the same region."
        + " function cost={}", functionCost());
      return true;
    }

    if (idleRegionServerExist(cluster)) {
      LOG.info("Running balancer because cluster has idle server(s)." + " function cost={}",
        functionCost());
      return true;
    }

    if (!balancerConditionals.shouldSkipSloppyServerEvaluation() && sloppyRegionServerExist(cs)) {
      LOG.info("Running balancer because cluster has sloppy server(s)." + " function cost={}",
        functionCost());
      return true;
    }

    if (balancerConditionals.shouldRunBalancer(cluster)) {
      LOG.info("Running balancer because conditional candidate generators have important moves");
      return true;
    }

    double total = 0.0;
    double multiplierTotal = 0; // this.sumMultiplier is not necessarily initialized at this point
    for (CostFunction c : costFunctions) {
      if (!c.isNeeded()) {
        LOG.trace("{} not needed", c.getClass().getSimpleName());
        continue;
      }
      total += c.cost() * c.getMultiplier();
      multiplierTotal += c.getMultiplier();
    }
    boolean balanced = (total / multiplierTotal < minCostNeedBalance);

    if (balanced) {
      final double calculatedTotal = total;
      sendRejectionReasonToRingBuffer(() -> getBalanceReason(calculatedTotal, sumMultiplier),
        costFunctions);
      LOG.info(
        "{} - skipping load balancing because weighted average imbalance={} <= "
          + "threshold({}) and conditionals do not have opinionated move candidates. "
          + "consecutive balancer runs. If you want more aggressive balancing, either lower "
          + "hbase.master.balancer.stochastic.minCostNeedBalance from {} or increase the relative "
          + "multiplier(s) of the specific cost function(s). functionCost={}",
        isByTable ? "Table specific (" + tableName + ")" : "Cluster wide", total / sumMultiplier,
        minCostNeedBalance, minCostNeedBalance, functionCost());
    } else {
      LOG.info("{} - Calculating plan. may take up to {}ms to complete.",
        isByTable ? "Table specific (" + tableName + ")" : "Cluster wide", maxRunningTime);
    }
    return !balanced;
  }

  @RestrictedApi(explanation = "Should only be called in tests", link = "",
      allowedOnPath = ".*(/src/test/.*|StochasticLoadBalancer).java")
  Pair<CandidateGenerator, BalanceAction> nextAction(BalancerClusterState cluster) {
    CandidateGenerator generator = getRandomGenerator();
    return Pair.newPair(generator, generator.generate(cluster));
  }

  /**
   * Select the candidate generator to use based on the cost of cost functions. The chance of
   * selecting a candidate generator is propotional to the share of cost of all cost functions among
   * all cost functions that benefit from it.
   */
  protected CandidateGenerator getRandomGenerator() {
    double sum = 0;
    for (Class<? extends CandidateGenerator> clazz : candidateGenerators.keySet()) {
      sum += weightsOfGenerators.getOrDefault(clazz, 0.0);
    }
    if (sum == 0) {
      return candidateGenerators.values().stream().findAny().orElseThrow();
    }

    for (Class<? extends CandidateGenerator> clazz : candidateGenerators.keySet()) {
      weightsOfGenerators.put(clazz,
        Math.min(CandidateGenerator.MAX_WEIGHT, weightsOfGenerators.get(clazz) / sum));
    }

    double rand = ThreadLocalRandom.current().nextDouble();
    Class<? extends CandidateGenerator> greatestWeightClazz = null;
    double greatestWeight = 0;
    for (Class<? extends CandidateGenerator> clazz : candidateGenerators.keySet()) {
      double generatorWeight = weightsOfGenerators.get(clazz);
      if (generatorWeight > greatestWeight) {
        greatestWeight = generatorWeight;
        greatestWeightClazz = clazz;
      }
      if (rand <= generatorWeight) {
        return candidateGenerators.get(clazz);
      }
    }

    return candidateGenerators.get(greatestWeightClazz);
  }

  @RestrictedApi(explanation = "Should only be called in tests", link = "",
      allowedOnPath = ".*/src/test/.*")
  void setRackManager(RackManager rackManager) {
    this.rackManager = rackManager;
  }

  private long calculateMaxSteps(BalancerClusterState cluster) {
    return (long) cluster.numRegions * (long) this.stepsPerRegion * (long) cluster.numServers;
  }

  /**
   * Given the cluster state this will try and approach an optimal balance. This should always
   * approach the optimal state given enough steps.
   */
  @Override
  protected List<RegionPlan> balanceTable(TableName tableName,
    Map<ServerName, List<RegionInfo>> loadOfOneTable) {
    // On clusters with lots of HFileLinks or lots of reference files,
    // instantiating the storefile infos can be quite expensive.
    // Allow turning this feature off if the locality cost is not going to
    // be used in any computations.
    RegionHDFSBlockLocationFinder finder = null;
    if ((this.localityCost != null) || (this.rackLocalityCost != null)) {
      finder = this.regionFinder;
    }

    // The clusterState that is given to this method contains the state
    // of all the regions in the table(s) (that's true today)
    // Keep track of servers to iterate through them.
    BalancerClusterState cluster = new BalancerClusterState(loadOfOneTable, loads, finder,
      rackManager, regionCacheRatioOnOldServerMap);

    long startTime = EnvironmentEdgeManager.currentTime();

    initCosts(cluster);
    balancerConditionals.loadClusterState(cluster);

    sumMultiplier = 0;
    for (CostFunction c : costFunctions) {
      if (c.isNeeded()) {
        sumMultiplier += c.getMultiplier();
      }
    }
    if (sumMultiplier <= 0) {
      LOG.error("At least one cost function needs a multiplier > 0. For example, set "
        + "hbase.master.balancer.stochastic.regionCountCost to a positive value or default");
      return null;
    }

    double currentCost = computeCost(cluster, Double.MAX_VALUE);
    curOverallCost = currentCost;
    System.arraycopy(tempFunctionCosts, 0, curFunctionCosts, 0, curFunctionCosts.length);
    updateStochasticCosts(tableName, curOverallCost, curFunctionCosts);
    double initCost = currentCost;
    double newCost;

    if (!needsBalance(tableName, cluster)) {
      return null;
    }

    long computedMaxSteps;
    if (runMaxSteps) {
      computedMaxSteps = Math.max(this.maxSteps, calculateMaxSteps(cluster));
    } else {
      long calculatedMaxSteps = calculateMaxSteps(cluster);
      computedMaxSteps = Math.min(this.maxSteps, calculatedMaxSteps);
      if (calculatedMaxSteps > maxSteps) {
        LOG.warn(
          "calculatedMaxSteps:{} for loadbalancer's stochastic walk is larger than "
            + "maxSteps:{}. Hence load balancing may not work well. Setting parameter "
            + "\"hbase.master.balancer.stochastic.runMaxSteps\" to true can overcome this issue."
            + "(This config change does not require service restart)",
          calculatedMaxSteps, maxSteps);
      }
    }

    LOG.info(
      "Start StochasticLoadBalancer.balancer, initial weighted average imbalance={}, "
        + "functionCost={} computedMaxSteps={}",
      currentCost / sumMultiplier, functionCost(), computedMaxSteps);

    final String initFunctionTotalCosts = totalCostsPerFunc();
    // Perform a stochastic walk to see if we can get a good fit.
    long step;

    boolean improvedConditionals = false;
    Map<CandidateGenerator, Long> generatorToStepCount = new HashMap<>();
    Map<CandidateGenerator, Long> generatorToApprovedActionCount = new HashMap<>();
    for (step = 0; step < computedMaxSteps; step++) {
      Pair<CandidateGenerator, BalanceAction> nextAction = nextAction(cluster);
      CandidateGenerator generator = nextAction.getFirst();
      BalanceAction action = nextAction.getSecond();

      if (action.getType() == BalanceAction.Type.NULL) {
        continue;
      }
      generatorToStepCount.merge(generator, action.getStepCount(), Long::sum);
      step += action.getStepCount() - 1;

      // Always accept a conditional generator output. Sometimes conditional generators
      // may need to make controversial moves in order to break what would otherwise
      // be a deadlocked situation.
      // Otherwise, for normal moves, evaluate the action.
      int conditionalViolationsChange;
      if (RegionPlanConditionalCandidateGenerator.class.isAssignableFrom(generator.getClass())) {
        conditionalViolationsChange = -1;
      } else {
        conditionalViolationsChange = balancerConditionals.isViolating(cluster, action);
      }

      // Change state and evaluate costs
      cluster.doAction(action);
      newCost = computeCost(cluster, currentCost);

      boolean conditionalsImproved = conditionalViolationsChange < 0;
      if (conditionalsImproved) {
        improvedConditionals = true;
      }
      boolean conditionalsSimilarCostsImproved =
        (newCost < currentCost && conditionalViolationsChange == 0);
      // Our first priority is to reduce conditional violations
      // Our second priority is to reduce balancer cost
      // change, regardless of cost change
      if (conditionalsImproved || conditionalsSimilarCostsImproved) {
        currentCost = newCost;
        generatorToApprovedActionCount.merge(generator, action.getStepCount(), Long::sum);
        balancerConditionals.loadClusterState(cluster);
        updateCostsAndWeightsWithAction(cluster, action);

        // save for JMX
        curOverallCost = currentCost;
        System.arraycopy(tempFunctionCosts, 0, curFunctionCosts, 0, curFunctionCosts.length);
      } else {
        // Put things back the way they were before.
        // TODO: undo by remembering old values
        BalanceAction undoAction = action.undoAction();
        cluster.doAction(undoAction);
        updateCostsAndWeightsWithAction(cluster, undoAction);
      }

      if (EnvironmentEdgeManager.currentTime() - startTime > maxRunningTime) {
        break;
      }
    }
    long endTime = EnvironmentEdgeManager.currentTime();

    // Build the log message
    StringBuilder logMessage = new StringBuilder("CandidateGenerator activity summary:\n");
    generatorToStepCount.forEach((generator, count) -> {
      long approvals = generatorToApprovedActionCount.getOrDefault(generator, 0L);
      logMessage.append(String.format(" - %s: %d steps, %d approvals\n",
        generator.getClass().getSimpleName(), count, approvals));
    });
    // Log the message
    LOG.info(logMessage.toString());

    metricsBalancer.balanceCluster(endTime - startTime);

    if (improvedConditionals || initCost > currentCost) {
      updateStochasticCosts(tableName, curOverallCost, curFunctionCosts);
      List<RegionPlan> plans = createRegionPlans(cluster);
      LOG.info(
        "Finished computing new moving plan. Computation took {} ms"
          + " to try {} different iterations.  Found a solution that moves "
          + "{} regions; Going from a computed imbalance of {}"
          + " to a new imbalance of {}. funtionCost={}",
        endTime - startTime, step, plans.size(), initCost / sumMultiplier,
        currentCost / sumMultiplier, functionCost());
      sendRegionPlansToRingBuffer(plans, currentCost, initCost, initFunctionTotalCosts, step);
      return plans;
    }
    LOG.info(
      "Could not find a better moving plan.  Tried {} different configurations in "
        + "{} ms, and did not find anything with an imbalance score less than {} "
        + "and could not improve conditional violations",
      step, endTime - startTime, initCost / sumMultiplier);
    return null;
  }

  protected void sendRejectionReasonToRingBuffer(Supplier<String> reason,
    List<CostFunction> costFunctions) {
    provider.recordBalancerRejection(() -> {
      BalancerRejection.Builder builder = new BalancerRejection.Builder().setReason(reason.get());
      if (costFunctions != null) {
        for (CostFunction c : costFunctions) {
          if (!c.isNeeded()) {
            continue;
          }
          builder.addCostFuncInfo(c.getClass().getName(), c.cost(), c.getMultiplier());
        }
      }
      return builder.build();
    });
  }

  private void sendRegionPlansToRingBuffer(List<RegionPlan> plans, double currentCost,
    double initCost, String initFunctionTotalCosts, long step) {
    provider.recordBalancerDecision(() -> {
      List<String> regionPlans = new ArrayList<>();
      for (RegionPlan plan : plans) {
        regionPlans
          .add("table: " + plan.getRegionInfo().getTable() + " , region: " + plan.getRegionName()
            + " , source: " + plan.getSource() + " , destination: " + plan.getDestination());
      }
      return new BalancerDecision.Builder().setInitTotalCost(initCost)
        .setInitialFunctionCosts(initFunctionTotalCosts).setComputedTotalCost(currentCost)
        .setFinalFunctionCosts(totalCostsPerFunc()).setComputedSteps(step)
        .setRegionPlans(regionPlans).build();
    });
  }

  /**
   * update costs to JMX
   */
  private void updateStochasticCosts(TableName tableName, double overall, double[] subCosts) {
    if (tableName == null) {
      return;
    }

    // check if the metricsBalancer is MetricsStochasticBalancer before casting
    if (metricsBalancer instanceof MetricsStochasticBalancer) {
      MetricsStochasticBalancer balancer = (MetricsStochasticBalancer) metricsBalancer;
      // overall cost
      balancer.updateStochasticCost(tableName.getNameAsString(), OVERALL_COST_FUNCTION_NAME,
        "Overall cost", overall);

      // each cost function
      for (int i = 0; i < costFunctions.size(); i++) {
        CostFunction costFunction = costFunctions.get(i);
        String costFunctionName = costFunction.getClass().getSimpleName();
        double costPercent = (overall == 0) ? 0 : (subCosts[i] / overall);
        // TODO: cost function may need a specific description
        balancer.updateStochasticCost(tableName.getNameAsString(), costFunctionName,
          "The percent of " + costFunctionName, costPercent);
      }
    }
  }

  private void addCostFunction(List<CostFunction> costFunctions, CostFunction costFunction) {
    float multiplier = costFunction.getMultiplier();
    if (multiplier > 0) {
      costFunctions.add(costFunction);
    }
  }

  protected String functionCost() {
    StringBuilder builder = new StringBuilder();
    for (CostFunction c : costFunctions) {
      builder.append(c.getClass().getSimpleName());
      builder.append(" : (");
      if (c.isNeeded()) {
        builder.append("multiplier=" + c.getMultiplier());
        builder.append(", ");
        double cost = c.cost();
        builder.append("imbalance=" + cost);
        if (cost >= minCostNeedBalance) {
          builder.append(", need balance");
        }
      } else {
        builder.append("not needed");
      }
      builder.append("); ");
    }
    return builder.toString();
  }

  @RestrictedApi(explanation = "Should only be called in tests", link = "",
      allowedOnPath = ".*(/src/test/.*|StochasticLoadBalancer).java")
  List<CostFunction> getCostFunctions() {
    return costFunctions;
  }

  private String totalCostsPerFunc() {
    StringBuilder builder = new StringBuilder();
    for (CostFunction c : costFunctions) {
      if (!c.isNeeded()) {
        continue;
      }
      double cost = c.getMultiplier() * c.cost();
      if (cost > 0.0) {
        builder.append(" ");
        builder.append(c.getClass().getSimpleName());
        builder.append(" : ");
        builder.append(cost);
        builder.append(";");
      }
    }
    if (builder.length() > 0) {
      builder.deleteCharAt(builder.length() - 1);
    }
    return builder.toString();
  }

  /**
   * Create all of the RegionPlan's needed to move from the initial cluster state to the desired
   * state.
   * @param cluster The state of the cluster
   * @return List of RegionPlan's that represent the moves needed to get to desired final state.
   */
  private List<RegionPlan> createRegionPlans(BalancerClusterState cluster) {
    List<RegionPlan> plans = new ArrayList<>();
    for (int regionIndex = 0; regionIndex
        < cluster.regionIndexToServerIndex.length; regionIndex++) {
      int initialServerIndex = cluster.initialRegionIndexToServerIndex[regionIndex];
      int newServerIndex = cluster.regionIndexToServerIndex[regionIndex];

      if (initialServerIndex != newServerIndex) {
        RegionInfo region = cluster.regions[regionIndex];
        ServerName initialServer = cluster.servers[initialServerIndex];
        ServerName newServer = cluster.servers[newServerIndex];

        if (LOG.isTraceEnabled()) {
          LOG.trace("Moving Region " + region.getEncodedName() + " from server "
            + initialServer.getHostname() + " to " + newServer.getHostname());
        }
        RegionPlan rp = new RegionPlan(region, initialServer, newServer);
        plans.add(rp);
      }
    }
    return plans;
  }

  /**
   * Store the current region loads.
   */
  private void updateRegionLoad() {
    // We create a new hashmap so that regions that are no longer there are removed.
    // However we temporarily need the old loads so we can use them to keep the rolling average.
    Map<String, Deque<BalancerRegionLoad>> oldLoads = loads;
    loads = new HashMap<>();

    clusterStatus.getLiveServerMetrics().forEach((ServerName sn, ServerMetrics sm) -> {
      sm.getRegionMetrics().forEach((byte[] regionName, RegionMetrics rm) -> {
        String regionNameAsString = RegionInfo.getRegionNameAsString(regionName);
        Deque<BalancerRegionLoad> rLoads = oldLoads.get(regionNameAsString);
        if (rLoads == null) {
          rLoads = new ArrayDeque<>(numRegionLoadsToRemember + 1);
        } else if (rLoads.size() >= numRegionLoadsToRemember) {
          rLoads.remove();
        }
        rLoads.add(new BalancerRegionLoad(rm));
        loads.put(regionNameAsString, rLoads);
      });
    });
  }

  @RestrictedApi(explanation = "Should only be called in tests", link = "",
      allowedOnPath = ".*(/src/test/.*|StochasticLoadBalancer).java")
  void initCosts(BalancerClusterState cluster) {
    // Initialize the weights of generator every time
    weightsOfGenerators = new HashMap<>(this.candidateGenerators.size());
    for (Class<? extends CandidateGenerator> clazz : candidateGenerators.keySet()) {
      weightsOfGenerators.put(clazz, 0.0);
    }
    for (CostFunction c : costFunctions) {
      c.prepare(cluster);
      c.updateWeight(weightsOfGenerators);
    }
    updateConditionalGeneratorWeights(cluster);
  }

  /**
   * Update both the costs of costfunctions and the weights of candidate generators
   */
  @RestrictedApi(explanation = "Should only be called in tests", link = "",
      allowedOnPath = ".*(/src/test/.*|StochasticLoadBalancer).java")
  void updateCostsAndWeightsWithAction(BalancerClusterState cluster, BalanceAction action) {
    // Reset all the weights to 0
    for (Class<? extends CandidateGenerator> clazz : candidateGenerators.keySet()) {
      weightsOfGenerators.put(clazz, 0.0);
    }
    for (CostFunction c : costFunctions) {
      if (c.isNeeded()) {
        c.postAction(action);
        c.updateWeight(weightsOfGenerators);
      }
    }
    updateConditionalGeneratorWeights(cluster);
  }

  private void updateConditionalGeneratorWeights(BalancerClusterState cluster) {
    for (RegionPlanConditional conditional : balancerConditionals.getConditionals()) {
      conditional.getCandidateGenerator().ifPresent(generator -> weightsOfGenerators
        .merge(generator.getClass(), generator.getWeight(cluster), Double::sum));
    }
  }

  /**
   * Get the names of the cost functions
   */
  @RestrictedApi(explanation = "Should only be called in tests", link = "",
      allowedOnPath = ".*(/src/test/.*|StochasticLoadBalancer).java")
  String[] getCostFunctionNames() {
    String[] ret = new String[costFunctions.size()];
    for (int i = 0; i < costFunctions.size(); i++) {
      CostFunction c = costFunctions.get(i);
      ret[i] = c.getClass().getSimpleName();
    }

    return ret;
  }

  /**
   * This is the main cost function. It will compute a cost associated with a proposed cluster
   * state. All different costs will be combined with their multipliers to produce a double cost.
   * @param cluster      The state of the cluster
   * @param previousCost the previous cost. This is used as an early out.
   * @return a double of a cost associated with the proposed cluster state. This cost is an
   *         aggregate of all individual cost functions.
   */
  @RestrictedApi(explanation = "Should only be called in tests", link = "",
      allowedOnPath = ".*(/src/test/.*|StochasticLoadBalancer).java")
  double computeCost(BalancerClusterState cluster, double previousCost) {
    double total = 0;

    for (int i = 0; i < costFunctions.size(); i++) {
      CostFunction c = costFunctions.get(i);
      this.tempFunctionCosts[i] = 0.0;

      if (!c.isNeeded()) {
        continue;
      }

      Float multiplier = c.getMultiplier();
      double cost = c.cost();

      this.tempFunctionCosts[i] = multiplier * cost;
      total += this.tempFunctionCosts[i];

      if (total > previousCost) {
        break;
      }
    }

    return total;
  }

  /**
   * A helper function to compose the attribute name from tablename and costfunction name
   */
  static String composeAttributeName(String tableName, String costFunctionName) {
    return tableName + TABLE_FUNCTION_SEP + costFunctionName;
  }
}
