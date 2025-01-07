package org.apache.hadoop.hbase.hubspot;

import org.agrona.collections.Int2IntCounterMap;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hbase.thirdparty.com.google.common.collect.ImmutableSet;
import org.apache.hbase.thirdparty.com.google.common.primitives.Ints;
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
import org.apache.yetus.audience.InterfaceAudience;
import java.lang.reflect.Field;
import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@InterfaceAudience.Private
public final class HubSpotCellUtilities {
  // TODO: this should be dynamically configured, not hard-coded, but this dramatically simplifies the initial version
  public static final short MAX_CELL_COUNT = 360;
  private static final int TARGET_MAX_CELLS_PER_RS = 72;

  public static final Gson OBJECT_MAPPER = new GsonBuilder()
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

  public static final ImmutableSet<String> CELL_AWARE_TABLES = ImmutableSet.of("objects-3");

  private HubSpotCellUtilities() {}

  public static int getMaxCellsPerRs(int servers) {
    return Math.max(
      TARGET_MAX_CELLS_PER_RS,
      Ints.checkedCast( (long)Math.floor((double) MAX_CELL_COUNT / servers))
    );
  }

  public static String toCellSetString(Set<Short> cells) {
    return cells.stream().sorted().map(x -> Short.toString(x)).collect(Collectors.joining(", ", "{", "}"));
  }

  public static boolean isStopInclusive(byte[] endKey) {
    return (endKey == null || endKey.length != 2) && (endKey == null || endKey.length <= 2
      || !areSubsequentBytesAllZero(endKey, 2));
  }

  public static short calcNumCells(RegionInfo[] regionInfos, short totalCellCount) {
    if (regionInfos == null || regionInfos.length == 0) {
      return 0;
    }

    Set<Short> cellsInRegions = Arrays.stream(regionInfos)
      .map(region -> toCells(region.getStartKey(), region.getEndKey(), totalCellCount))
      .flatMap(Set::stream).collect(Collectors.toSet());
    return Shorts.checkedCast(cellsInRegions.size());
  }

  public static Set<Short> toCells(byte[] rawStart, byte[] rawStop, short numCells) {
    return range(padToTwoBytes(rawStart, (byte) 0), padToTwoBytes(rawStop, (byte) -1), numCells);
  }

  public static byte[] padToTwoBytes(byte[] key, byte pad) {
    if (key == null || key.length == 0) {
      return new byte[] { pad, pad };
    }

    if (key.length == 1) {
      return new byte[] { pad, key[0] };
    }

    return key;
  }

  public static Set<Short> range(byte[] start, byte[] stop) {
     return range(start, stop, MAX_CELL_COUNT);
  }

  public static Set<Short> range(byte[] start, byte[] stop, short numCells) {
    short stopCellId = toCell(stop, (byte) -1, (short) (numCells - 1));
    if (stopCellId < 0 || stopCellId > numCells) {
      stopCellId = numCells;
    }
    short startCellId = toCell(start, (byte) 0, (short) 0);

    if (startCellId == stopCellId) {
      return ImmutableSet.of(startCellId);
    }

    boolean isStopExclusive = areSubsequentBytesAllZero(stop, 2);

    final IntStream cellStream;
    if (isStopExclusive) {
      cellStream = IntStream.range(startCellId, stopCellId);
    } else {
      int stopCellIdForcedToIncludeStart = Math.max(stopCellId, startCellId + 1);
      cellStream = IntStream.rangeClosed(startCellId, stopCellIdForcedToIncludeStart);
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

  private static short toCell(byte[] key, byte pad, short ifAbsent) {
    if (key == null) {
      throw new IllegalArgumentException(
        "Key must be nonnull");
    }

    return key.length == 0
      ? ifAbsent
      : (key.length >= 2
      ? Bytes.toShort(key, 0, 2)
      : Bytes.toShort(new byte[] { pad, key[0] }));
  }

  static class Int2IntCounterMapAdapter implements JsonSerializer<Int2IntCounterMap>,
    JsonDeserializer<Int2IntCounterMap> {
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
}
