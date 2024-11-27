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

import java.lang.reflect.Field;
import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.agrona.collections.Int2IntCounterMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hbase.thirdparty.com.google.common.primitives.Ints;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hbase.thirdparty.com.google.common.base.Preconditions;
import org.apache.hbase.thirdparty.com.google.common.collect.ImmutableMap;
import org.apache.hbase.thirdparty.com.google.common.collect.ImmutableMultimap;
import org.apache.hbase.thirdparty.com.google.common.collect.ImmutableSet;
import org.apache.hbase.thirdparty.com.google.common.primitives.Shorts;
import org.apache.hbase.thirdparty.com.google.gson.ExclusionStrategy;
import org.apache.hbase.thirdparty.com.google.gson.FieldAttributes;
import org.apache.hbase.thirdparty.com.google.gson.Gson;
import org.apache.hbase.thirdparty.com.google.gson.GsonBuilder;
import org.apache.hbase.thirdparty.com.google.gson.JsonArray;
import org.apache.hbase.thirdparty.com.google.gson.JsonDeserializationContext;
import org.apache.hbase.thirdparty.com.google.gson.JsonDeserializer;
import org.apache.hbase.thirdparty.com.google.gson.JsonElement;
import org.apache.hbase.thirdparty.com.google.gson.JsonObject;
import org.apache.hbase.thirdparty.com.google.gson.JsonParseException;
import org.apache.hbase.thirdparty.com.google.gson.JsonSerializationContext;
import org.apache.hbase.thirdparty.com.google.gson.JsonSerializer;

/**
 * HubSpot addition: Cost function for balancing regions based on their (reversed) cell prefix. This
 * should not be upstreamed, and our upstream solution should instead focus on introduction of
 * balancer conditionals; see
 * <a href="https://issues.apache.org/jira/browse/HBASE-28513">HBASE-28513</a>
 */
@InterfaceAudience.Private
public class HubSpotCellCostFunction extends CostFunction {

  private static final Logger LOG = LoggerFactory.getLogger(HubSpotCellCostFunction.class);
  private static final String HUBSPOT_CELL_COST_MULTIPLIER =
    "hbase.master.balancer.stochastic.hubspotCellCost";

  static class Int2IntCounterMapAdapter implements JsonSerializer<Int2IntCounterMap>, JsonDeserializer<Int2IntCounterMap> {
    @Override public JsonElement serialize(Int2IntCounterMap src, Type typeOfSrc,
      JsonSerializationContext context) {
      JsonObject obj = new JsonObject();

      obj.addProperty("loadFactor", src.loadFactor());
      obj.addProperty("initialValue", src.initialValue());
      obj.addProperty("resizeThreshold", src.resizeThreshold());
      obj.addProperty("size", src.size());

      Field entryField = null;
      try {
        entryField = Int2IntCounterMap.class.getDeclaredField("entries");
      } catch (NoSuchFieldException e) {
        throw new RuntimeException(e);
      }
      entryField.setAccessible(true);
      int[] entries = null;
      try {
        entries = (int[]) entryField.get(src);
      } catch (IllegalAccessException e) {
        throw new RuntimeException(e);
      }
      JsonArray entryArray = new JsonArray(entries.length);
      for (int entry : entries) {
        entryArray.add(entry);
      }
      obj.add("entries", entryArray);

      return obj;
    }

    @Override public Int2IntCounterMap deserialize(JsonElement json, Type typeOfT,
      JsonDeserializationContext context) throws JsonParseException {
      JsonObject obj = json.getAsJsonObject();

      float loadFactor = obj.get("loadFactor").getAsFloat();
      int initialValue = obj.get("initialValue").getAsInt();
      int resizeThreshold = obj.get("resizeThreshold").getAsInt();
      int size = obj.get("size").getAsInt();

      JsonArray entryArray = obj.get("entries").getAsJsonArray();
      int[] entries = new int[entryArray.size()];

      for (int i = 0; i < entryArray.size(); i++) {
        entries[i] = entryArray.get(i).getAsInt();
      }

      Int2IntCounterMap result = new Int2IntCounterMap(0, loadFactor, initialValue);

      Field resizeThresholdField = null;
      Field entryField = null;
      Field sizeField = null;

      try {
        resizeThresholdField = Int2IntCounterMap.class.getDeclaredField("resizeThreshold");
        entryField = Int2IntCounterMap.class.getDeclaredField("entries");
        sizeField = Int2IntCounterMap.class.getDeclaredField("size");
      } catch (NoSuchFieldException e) {
        throw new RuntimeException(e);
      }

      resizeThresholdField.setAccessible(true);
      entryField.setAccessible(true);
      sizeField.setAccessible(true);

      try {
        resizeThresholdField.set(result, resizeThreshold);
        entryField.set(result, entries);
        sizeField.set(result, size);
      } catch (IllegalAccessException e) {
        throw new RuntimeException(e);
      }

      return result;
    }
  }

  static final Gson OBJECT_MAPPER = new GsonBuilder()
    .excludeFieldsWithoutExposeAnnotation()
    .enableComplexMapKeySerialization()
    .registerTypeAdapter(Int2IntCounterMap.class, new Int2IntCounterMapAdapter())
    .registerTypeAdapter(RegionInfo.class, (JsonDeserializer) (json, typeOfT, context) -> {
      JsonObject obj = json.getAsJsonObject();

      boolean split = obj.get("split").getAsBoolean();
      long regionId = obj.get("regionId").getAsLong();
      int replicaId = obj.get("replicaId").getAsInt();
      JsonObject tableName = obj.get("tableName").getAsJsonObject();
      JsonArray startKey = obj.get("startKey").getAsJsonArray();
      JsonArray endKey = obj.get("endKey").getAsJsonArray();

      byte[] startKeyBytes = new byte[startKey.size()];
      byte[] endKeyBytes = new byte[endKey.size()];

      for (int i = 0; i < startKey.size(); i++) {
        startKeyBytes[i] = startKey.get(i).getAsByte();
      }
      for (int i = 0; i < endKey.size(); i++) {
        endKeyBytes[i] = endKey.get(i).getAsByte();
      }

      TableName tb = TableName.valueOf(
        tableName.get("namespaceAsString").getAsString(),
        tableName.get("qualifierAsString").getAsString()
      );

      RegionInfo result =
        RegionInfoBuilder.newBuilder(tb).setSplit(split).setRegionId(regionId)
          .setReplicaId(replicaId).setStartKey(startKeyBytes).setEndKey(endKeyBytes).build();
      return result;
    })
    .addDeserializationExclusionStrategy(new ExclusionStrategy() {
      @Override public boolean shouldSkipField(FieldAttributes f) {
        return f.getName().equals("serversToIndex")
          || f.getName().equals("regionsToIndex")
          || f.getName().equals("clusterState")
          ;
      }

      @Override public boolean shouldSkipClass(Class<?> clazz) {
        return false;
      }
    })
    .create();
  private static final float DEFAULT_HUBSPOT_CELL_COST = 0;
  // hack - hard code this for now
  static final short MAX_CELL_COUNT = 360;

  private int numServers;
  private short numCells;
  private ServerName[] servers;
  private RegionInfo[] regions;
  private int[] regionIndexToServerIndex;

  private boolean[][] serverHasCell;
  private Int2IntCounterMap regionCountByCell;
  private int bestCaseMaxCellsPerServer;
  private int numRegionCellsOverassigned;

  HubSpotCellCostFunction(Configuration conf) {
    this.setMultiplier(conf.getFloat(HUBSPOT_CELL_COST_MULTIPLIER, DEFAULT_HUBSPOT_CELL_COST));
  }

  @Override
  void prepare(BalancerClusterState cluster) {
    numServers = cluster.numServers;
    numCells = calcNumCells(cluster.regions, MAX_CELL_COUNT);
    regions = cluster.regions;
    regionIndexToServerIndex = cluster.regionIndexToServerIndex;
    servers = cluster.servers;
    super.prepare(cluster);

    if (LOG.isTraceEnabled()
      && cluster.tables.contains("objects-3")
      && cluster.regions != null
      && cluster.regions.length > 0
    ) {
      try {
        LOG.trace("{} cluster state:\n{}", cluster.tables, OBJECT_MAPPER.toJson(cluster));
      } catch (Exception ex) {
        LOG.error("Failed to write cluster state", ex);
      }
    }

    this.serverHasCell = new boolean[numServers][numCells];
    int balancedRegionsPerServer = Ints.checkedCast((long) Math.ceil((double) cluster.numRegions / cluster.numServers));
    this.regionCountByCell = new Int2IntCounterMap(MAX_CELL_COUNT, 0.5f, 0);
    Arrays.stream(cluster.regions)
      .forEach(r -> toCells(r.getStartKey(), r.getEndKey(), MAX_CELL_COUNT).forEach(cell -> regionCountByCell.addAndGet((int) cell, 1)));
    this.bestCaseMaxCellsPerServer = balancedRegionsPerServer;
    int numTimesCellRegionsFillAllServers = 0;
    for (int cell = 0; cell < MAX_CELL_COUNT; cell++) {
      int numRegionsForCell = regionCountByCell.get(cell);
      numTimesCellRegionsFillAllServers += Ints.checkedCast((long) Math.floor((double) numRegionsForCell / numServers));
    }

    this.bestCaseMaxCellsPerServer -= numTimesCellRegionsFillAllServers;

    this.numRegionCellsOverassigned =
      calculateCurrentCellCost(
        numCells,
        numServers,
        bestCaseMaxCellsPerServer,
        regions, regionIndexToServerIndex,
        serverHasCell,
        super.cluster::getRegionSizeMB
      );

    if (regions.length > 0
      && regions[0].getTable().getNamespaceAsString().equals("default")
      && LOG.isTraceEnabled()
    ) {
      LOG.trace("Evaluated (cost={}) {}", String.format("%d", numRegionCellsOverassigned), snapshotState());
    }
  }

  @Override boolean isNeeded() {
    return cluster.tables.stream().anyMatch(name -> name.contains("objects-3"));
  }

  @Override protected void regionMoved(int region, int oldServer, int newServer) {
    RegionInfo movingRegion = regions[region];

    if (!movingRegion.getTable().getNamespaceAsString().equals("default")) {
      return;
    }

    Set<Short> cellsOnRegion = toCells(movingRegion.getStartKey(), movingRegion.getEndKey(), numCells);

    if (LOG.isDebugEnabled()) {
      LOG.debug("Evaluating move of region {} [{}, {}). Cells are {}.",
        region,
        Bytes.toHex(movingRegion.getStartKey()),
        Bytes.toHex(movingRegion.getEndKey()),
        cellsOnRegion
      );
    }

    Map<Short, Integer> numRegionsForCellOnOldServer = computeCellFrequencyForServer(oldServer);
    Map<Short, Integer> numRegionsForCellOnNewServer = computeCellFrequencyForServer(newServer);

    int currentCellCountOldServer = numRegionsForCellOnOldServer.keySet().size();
    int currentCellCountNewServer = numRegionsForCellOnNewServer.keySet().size();

    if (LOG.isDebugEnabled()) {
      LOG.debug(
        "Old server {} [{}] has cell frequency of {}.\n\nNew server {} [{}] has cell frequency of {}.",
        oldServer,
        currentCellCountOldServer,
        numRegionsForCellOnOldServer,
        newServer,
        currentCellCountNewServer,
        numRegionsForCellOnNewServer
      );
    }

    int changeInOverassignedRegionCells = 0;
    for (short movingCell : cellsOnRegion) {
      // this is invoked AFTER the region has been moved
      boolean didMoveDecreaseCellsOnOldServer = !numRegionsForCellOnOldServer.containsKey(movingCell);
      boolean didMoveIncreaseCellsOnNewServer = numRegionsForCellOnNewServer.get(movingCell) == 1;

      if (didMoveDecreaseCellsOnOldServer) {
        changeInOverassignedRegionCells++;
        serverHasCell[oldServer][movingCell] = false;
      }

      if (didMoveIncreaseCellsOnNewServer) {
        changeInOverassignedRegionCells--;
        serverHasCell[newServer][movingCell] = true;
      }
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("Move cost delta for s{}.r{} --> s{} is {}", oldServer, region, newServer, changeInOverassignedRegionCells);
    }

    numRegionCellsOverassigned += changeInOverassignedRegionCells;
  }

  private Map<Short, Integer> computeCellFrequencyForServer(int server) {
    int[] regions = cluster.regionsPerServer[server];
    ImmutableMultimap.Builder<Short, Integer> regionsByCell = ImmutableMultimap.builder();
    for (int regionIndex : regions) {
      RegionInfo region = cluster.regions[regionIndex];
      Set<Short> cellsInRegion = toCells(region.getStartKey(), region.getEndKey(), numCells);
      cellsInRegion.forEach(cell -> regionsByCell.put(cell, regionIndex));
    }

    return regionsByCell.build()
      .asMap()
      .entrySet()
      .stream()
      .collect(ImmutableMap.toImmutableMap(Map.Entry::getKey, entry -> entry.getValue().size()));
  }

  private String snapshotState() {
    StringBuilder stateString = new StringBuilder();

    stateString.append("HubSpotCellCostFunction config for ")
      .append(Optional.ofNullable(regions[0]).map(RegionInfo::getTable)
        .map(TableName::getNameWithNamespaceInclAsString).orElseGet(() -> "N/A"))
      .append(":").append("\n\tnumServers=").append(numServers).append("\n\tnumCells=")
      .append(numCells).append("\n\tmultiplier=").append(String.format("%.3f", getMultiplier()))
      .append("\n\tregions=\n[");

    if (LOG.isDebugEnabled()) {
      int numAssigned = 0;
      int numUnassigned = 0;

      for (int i = 0; i < regions.length; i++) {
        RegionInfo region = regions[i];
        int location = this.regionIndexToServerIndex[i];
        Optional<ServerName> highestLocalityServerMaybe =
          Optional.ofNullable(location).filter(serverLocation -> serverLocation >= 0)
            .map(serverIndex -> this.servers[serverIndex]);

        if (location > 0) {
          numAssigned++;
        } else {
          numUnassigned++;
        }

        int regionSizeMb = super.cluster.getRegionSizeMB(i);
        String cellsInRegion =
          toCellSetString(toCells(region.getStartKey(), region.getEndKey(), numCells));

        stateString.append("\n\t").append(region.getShortNameToLog()).append("[")
          .append(Bytes.toHex(region.getStartKey())).append(", ")
          .append(Bytes.toHex(region.getEndKey())).append(") ").append(cellsInRegion).append(" [")
          .append(regionSizeMb).append(" mb] -> ")
          .append(highestLocalityServerMaybe.map(ServerName::getServerName).orElseGet(() -> "N/A"));
      }

      stateString.append("\n]\n\n\tAssigned regions: ").append(numAssigned)
        .append("\n\tUnassigned regions: ").append(numUnassigned).append("\n");
    } else {
      stateString.append("\n\t").append(regions.length).append(" regions\n]");
    }

    return stateString.toString();
  }

  private static String toCellSetString(Set<Short> cells) {
    return cells.stream().map(x -> Short.toString(x)).collect(Collectors.joining(", ", "{", "}"));
  }

  @Override
  protected double cost() {
    return numRegionCellsOverassigned;
  }

  static int calculateCurrentCellCost(
    short numCells,
    int numServers,
    int bestCaseMaxCellsPerServer,
    RegionInfo[] regions,
    int[] regionLocations,
    boolean[][] serverHasCell,
    Function<Integer, Integer> getRegionSizeMbFunc
  ) {

    Preconditions.checkState(bestCaseMaxCellsPerServer > 0,
      "Best case max cells per server must be > 0");

    if (LOG.isTraceEnabled()) {
      Set<String> tableAndNamespace = Arrays.stream(regions).map(RegionInfo::getTable)
        .map(table -> table.getNameAsString() + "." + table.getNamespaceAsString())
        .collect(Collectors.toSet());
      LOG.trace("Calculating current cell cost for {} regions from these tables {}", regions.length,
        tableAndNamespace);
    }

    if (regions.length > 0 && !regions[0].getTable().getNamespaceAsString().equals("default")) {
      if (LOG.isTraceEnabled()) {
        LOG.trace("Skipping cost calculation for non-default namespace on {}",
          regions[0].getTable().getNameWithNamespaceInclAsString());
      }
      return 0;
    }

    for (int i = 0; i < regions.length; i++) {
      if (regions[i] == null) {
        throw new IllegalStateException("No region available at index " + i);
      }

      if (regionLocations[i] == -1) {
        int regionSizeMb = getRegionSizeMbFunc.apply(i);
        if (regionSizeMb == 0 && LOG.isTraceEnabled()) {
          LOG.trace("{} ({} mb): no servers available, this IS an empty region",
            regions[i].getShortNameToLog(), regionSizeMb);
        } else {
          throw new IllegalStateException(
            "No server list available for region " + regions[i].getShortNameToLog());
        }
      }

      setCellsForServer(serverHasCell[regionLocations[i]], regions[i].getStartKey(),
        regions[i].getEndKey(), numCells);
    }

    int cost = 0;
    StringBuilder debugBuilder = new StringBuilder().append("[");
    for (int server = 0; server < numServers; server++) {
      int cellsOnThisServer = 0;
      for (int j = 0; j < numCells; j++) {
        if (serverHasCell[server][j]) {
          cellsOnThisServer++;
        }
      }

      int costForThisServer = Math.max(bestCaseMaxCellsPerServer - cellsOnThisServer, 0);
      if (LOG.isDebugEnabled()) {
        debugBuilder.append(server).append("=").append(costForThisServer).append(", ");
      }
      cost += costForThisServer;
    }

    if (LOG.isDebugEnabled()) {
      debugBuilder.append("]");
      LOG.debug("Cost {} from {}", cost, debugBuilder);
    }

    return cost;
  }

  private static void setCellsForServer(
    boolean[] serverHasCell,
    byte[] startKey,
    byte[] endKey,
    short numCells
  ) {
    short startCellId = (startKey == null || startKey.length == 0)
      ? 0
      : (startKey.length >= 2
        ? Bytes.toShort(startKey, 0, 2)
        : Bytes.toShort(new byte[] { 0, startKey[0] }));
    short stopCellId = (endKey == null || endKey.length == 0)
      ? (short) (numCells - 1)
      : (endKey.length >= 2
        ? Bytes.toShort(endKey, 0, 2)
        : Bytes.toShort(new byte[] { -1, endKey[0] }));

    if (stopCellId < 0 || stopCellId > numCells) {
      stopCellId = numCells;
    }

    if (startCellId == stopCellId) {
      serverHasCell[startCellId] = true;
      return;
    }

    for (short i = startCellId; i < stopCellId; i++) {
      serverHasCell[i] = true;
    }

    // if everything after the cell prefix is 0, this stop key is actually exclusive
    if (!isStopExclusive(endKey)) {
      serverHasCell[stopCellId] = true;
    }
  }

  static boolean isStopExclusive(byte[] endKey) {
    return endKey != null && endKey.length == 2 || (endKey.length > 2 && areSubsequentBytesAllZero(endKey, 2));
  }

  static short calcNumCells(RegionInfo[] regionInfos, short totalCellCount) {
    if (regionInfos == null || regionInfos.length == 0) {
      return 0;
    }

    Set<Short> cellsInRegions = Arrays.stream(regionInfos)
      .map(region -> toCells(region.getStartKey(), region.getEndKey(), totalCellCount))
      .flatMap(Set::stream).collect(Collectors.toSet());
    return Shorts.checkedCast(cellsInRegions.size());
  }

  static Set<Short> toCells(byte[] rawStart, byte[] rawStop, short numCells) {
    return range(padToTwoBytes(rawStart, (byte) 0), padToTwoBytes(rawStop, (byte) -1), numCells);
  }

  private static byte[] padToTwoBytes(byte[] key, byte pad) {
    if (key == null || key.length == 0) {
      return new byte[] { pad, pad };
    }

    if (key.length == 1) {
      return new byte[] { pad, key[0] };
    }

    return key;
  }

  private static Set<Short> range(byte[] start, byte[] stop, short numCells) {
    short stopCellId = toCell(stop);
    if (stopCellId < 0 || stopCellId > numCells) {
      stopCellId = numCells;
    }
    short startCellId = toCell(start);

    if (startCellId == stopCellId) {
      return ImmutableSet.of(startCellId);
    }

    // if everything after the cell prefix is 0, this stop key is actually exclusive
    boolean isStopExclusive = areSubsequentBytesAllZero(stop, 2);

    final IntStream cellStream;
    if (isStopExclusive) {
      cellStream = IntStream.range(startCellId, stopCellId);
    } else {
      // this is inclusive, but we have to make sure we include at least the startCellId,
      // even if stopCell = startCell + 1
      cellStream = IntStream.rangeClosed(startCellId, Math.max(stopCellId, startCellId + 1));
    }

    return cellStream.mapToObj(val -> (short) val).collect(Collectors.toSet());
  }

  private static boolean areSubsequentBytesAllZero(byte[] stop, int offset) {
    for (int i = offset; i < stop.length; i++) {
      if (stop[i] != (byte) 0) {
        return false;
      }
    }
    return true;
  }

  private static short toCell(byte[] key) {
    if (key == null || key.length < 2) {
      throw new IllegalArgumentException(
        "Key must be nonnull and at least 2 bytes long - passed " + Bytes.toHex(key));
    }

    return Bytes.toShort(key, 0, 2);
  }

  @Override
  public final void updateWeight(double[] weights) {
    weights[StochasticLoadBalancer.GeneratorType.HUBSPOT_CELL.ordinal()] += cost();
  }
}
