package com.cerner.bunsen.codes.broadcast;

import com.cerner.bunsen.codes.ConceptMaps;
import com.cerner.bunsen.codes.Mapping;
import java.io.Serializable;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.hl7.fhir.dstu3.model.ConceptMap;
import org.hl7.fhir.dstu3.model.ConceptMap.ConceptMapGroupUnmappedMode;

/**
 * Support to broadcast a collection of {@link BroadcastableConceptMap concept maps}
 * to map data transformations.
 */
public class BroadcastableMappings implements Serializable {
  /**
   * Map of concept map URIs to broadcastable maps.
   */
  private Map<String, BroadcastableConceptMap> conceptMaps;

  private BroadcastableMappings(Map<String, BroadcastableConceptMap> conceptMaps) {
    this.conceptMaps = conceptMaps;
  }

  /**
   * Returns a list of concept maps in the order they need to be loaded.
   */
  private static List<String> sortMapsToLoad(Set<String> mapsToLoad,
      Map<String,ConceptMap> allMaps) {

    Deque<String> pendingMaps = new ArrayDeque<>(mapsToLoad);

    List<String> loadOrder = new ArrayList<>();

    Set<String> loadedMaps = new HashSet<>();

    while (!pendingMaps.isEmpty()) {

      String nextMap = pendingMaps.peek();

      // If the map has already been loaded,
      // remove it and continue.
      if (loadedMaps.contains(nextMap)) {

        pendingMaps.pop();
        continue;
      }

      ConceptMap mapToLoad = allMaps.get(nextMap);

      if (mapToLoad == null) {
        throw new IllegalStateException("Concept map " + nextMap + " "
            + " is referenced but not in the collection of concept maps.");
      }

      // Get the set of children we need to load before the pending map.
      Set<String> childrenToLoad = getMapChildren(mapToLoad);

      childrenToLoad.removeAll(loadedMaps);

      // If the pending map has no children to load, we can
      // add it to our load order.
      if (childrenToLoad.isEmpty()) {

        loadedMaps.add(nextMap);
        loadOrder.add(nextMap);
        pendingMaps.pop();

      } else {

        // The pending map has children, so we need to load them first.
        for (String child: childrenToLoad) {
          pendingMaps.push(child);
        }
      }
    }

    return loadOrder;
  }

  /**
   * Returns the children of a given concept map to which the parent
   * may delegate mapped items.
   */
  private static Set<String> getMapChildren(ConceptMap map) {

    return map.getGroup()
        .stream()
        .filter(group -> group.getUnmapped() != null
            && group.getUnmapped().getMode() == ConceptMapGroupUnmappedMode.OTHERMAP)
        .map(group -> group.getUnmapped().getUrl())
        .collect(Collectors.toSet());
  }



  /**
   * Broadcasts the given set of concept maps.
   *
   * @param spark the spark session.
   * @param conceptMaps the FHIR ConceptMaps to broadcast
   * @return a broadcast variable containing a mappings object usable in UDFs.
   */
  public static Broadcast<BroadcastableMappings> broadcast(SparkSession spark,
      List<ConceptMap> conceptMaps) {

    ConceptMaps mapData = ConceptMaps.getEmpty(spark).withConceptMaps(conceptMaps);

    Map<String,String> conceptMapUris = conceptMaps.stream()
        .collect(Collectors.toMap(ConceptMap::getUrl, ConceptMap::getVersion));

    return broadcast(mapData, conceptMapUris);
  }

  /**
   * Broadcast mappings stored in the given conceptMaps instance that match the given
   * conceptMapUris.
   *
   * @param conceptMaps the {@link ConceptMaps} instance with the content to broadcast
   * @param conceptMapUris the URIs to broadcast.
   * @param includeExperimental flag to include experimental map versions in the broadcast.
   * @return a broadcast variable containing a mappings object usable in UDFs.
   */
  public static Broadcast<BroadcastableMappings> broadcast(ConceptMaps conceptMaps,
      Set<String> conceptMapUris,
      boolean includeExperimental) {

    // Load all maps because we must transitively pull in delegated maps, and since
    // there are relatively few maps we can simply do so in one pass.
    Map<String, String> latest = conceptMaps.getLatestVersions(includeExperimental);

    return broadcast(conceptMaps, latest);
  }

  /**
   * Broadcast mappings stored in the given conceptMaps instance that match the given
   * conceptMapUris.
   *
   * @param conceptMaps the {@link ConceptMaps} instance with the content to broadcast
   * @param conceptMapUriToVersion map of the concept map URIs to broadcast to their versions.
   * @return a broadcast variable containing a mappings object usable in UDFs.
   */
  public static Broadcast<BroadcastableMappings> broadcast(ConceptMaps conceptMaps,
      Map<String,String> conceptMapUriToVersion) {

    Map<String,ConceptMap> mapsToLoad = conceptMaps.getMaps()
        .collectAsList()
        .stream()
        .filter(conceptMap ->
            conceptMap.getVersion().equals(conceptMapUriToVersion.get(conceptMap.getUrl())))
        .collect(Collectors.toMap(ConceptMap::getUrl, Function.identity()));

    // Expand the concept maps to load and sort them so dependencies are before
    // their dependents in the list.
    List<String> sortedMapsToLoad = sortMapsToLoad(conceptMapUriToVersion.keySet(), mapsToLoad);

    // Since this is used to map from one system to another, we use only targets
    // that don't introduce inaccurate meanings. (For instance, we can't map
    // general condition code to a more specific type, since that is not
    // representative of the source data.)
    Dataset<Mapping> mappings = conceptMaps.getMappings(conceptMapUriToVersion)
        .filter("equivalence in ('equivalent', 'equals', 'wider', 'subsumes')");

    // Group mappings by their concept map URI
    Map<String, List<Mapping>> groupedMappings =  mappings
        .collectAsList()
        .stream()
        .collect(Collectors.groupingBy(Mapping::getConceptMapUri));

    Map<String, BroadcastableConceptMap> broadcastableMaps = new HashMap<>();

    for (String conceptMapUri: sortedMapsToLoad) {

      ConceptMap map = mapsToLoad.get(conceptMapUri);

      Set<String> children = getMapChildren(map);

      List<BroadcastableConceptMap> childMaps = children.stream()
          .map(child -> broadcastableMaps.get(child))
          .collect(Collectors.toList());

      BroadcastableConceptMap broadcastableConceptMap = new BroadcastableConceptMap(conceptMapUri,
          groupedMappings.getOrDefault(conceptMapUri, Collections.emptyList()),
          childMaps);

      broadcastableMaps.put(conceptMapUri, broadcastableConceptMap);
    }

    JavaSparkContext ctx = new JavaSparkContext(conceptMaps.getMaps()
        .sparkSession()
        .sparkContext());

    return ctx.broadcast(new BroadcastableMappings(broadcastableMaps));
  }

  /**
   * Returns the broadcastable concept map for the concept
   * map with the given URI.
   *
   * @param conceptMapUri URI of the concept map
   * @return the broadcastable concept map.
   */
  public BroadcastableConceptMap getBroadcastConceptMap(String conceptMapUri) {

    BroadcastableConceptMap map = conceptMaps.get(conceptMapUri);

    if (map == null) {
      BroadcastableConceptMap emptyMap = new BroadcastableConceptMap(
          conceptMapUri,
          Collections.emptyList());

      conceptMaps.put(conceptMapUri, emptyMap);

      map = emptyMap;
    }

    return map;
  }

}
