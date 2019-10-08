package feast.ingestion.utils;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import feast.core.FeatureSetProto.FeatureSetSpec;
import feast.core.StoreProto.Store;
import feast.core.StoreProto.Store.Subscription;
import java.util.ArrayList;
import java.util.List;

public class SpecUtil {
  /**
   * Get only feature set specs that matches the subscription
   *
   * @param subscriptions
   * @param featureSetSpecs
   * @return
   */
  public static List<FeatureSetSpec> getSubscribedFeatureSets(
      List<Subscription> subscriptions, List<FeatureSetSpec> featureSetSpecs) {
    List<FeatureSetSpec> subscribed = new ArrayList<>();
    for (FeatureSetSpec featureSet : featureSetSpecs) {
      for (Subscription sub : subscriptions) {
        if (!featureSet.getName().equals(sub.getName())) {
          continue;
        }

        // Supports only subscription version with number only or number with ">" sign
        if (sub.getVersion().startsWith(">") && sub.getVersion().length() > 1) {
          int lowerBoundIncl = Integer.parseInt(sub.getVersion().substring(1));
          if (featureSet.getVersion() >= lowerBoundIncl) {
            subscribed.add(featureSet);
            break;
          }
        } else {
          int version = Integer.parseInt(sub.getVersion());
          if (featureSet.getVersion() == version) {
            subscribed.add(featureSet);
            break;
          }
        }
      }
    }
    return subscribed;
  }

  public static List<FeatureSetSpec> parseFeatureSetSpecJsonList(List<String> jsonList)
      throws InvalidProtocolBufferException {
    List<FeatureSetSpec> featureSetSpecs = new ArrayList<>();
    for (String json : jsonList) {
      FeatureSetSpec.Builder builder = FeatureSetSpec.newBuilder();
      JsonFormat.parser().merge(json, builder);
      featureSetSpecs.add(builder.build());
    }
    return featureSetSpecs;
  }

  public static List<Store> parseStoreJsonList(List<String> jsonList)
      throws InvalidProtocolBufferException {
    List<Store> stores = new ArrayList<>();
    for (String json : jsonList) {
      Store.Builder builder = Store.newBuilder();
      JsonFormat.parser().merge(json, builder);
      stores.add(builder.build());
    }
    return stores;
  }
}
