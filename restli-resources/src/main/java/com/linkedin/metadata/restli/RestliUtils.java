package com.linkedin.metadata.restli;

import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.data.template.StringArray;
import com.linkedin.parseq.Task;
import com.linkedin.restli.common.HttpStatus;
import com.linkedin.restli.server.RestLiServiceException;
import java.util.Comparator;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;


public class RestliUtils {

  private RestliUtils() {
    // Utils class
  }

  /**
   * Executes the provided supplier and convert the results to a {@link Task}.
   * Exceptions thrown during the execution will be properly wrapped in {@link RestLiServiceException}.
   * @param supplier The supplier to execute
   * @return A parseq {@link Task}
   */
  @Nonnull
  public static <T> Task<T> toTask(@Nonnull Supplier<T> supplier) {
    try {
      return Task.value(supplier.get());
    } catch (Throwable throwable) {

      // Convert IllegalArgumentException to BAD REQUEST
      if (throwable instanceof IllegalArgumentException || throwable.getCause() instanceof IllegalArgumentException) {
        throwable = badRequestException(throwable.getMessage());
      }

      if (throwable instanceof RestLiServiceException) {
        throw (RestLiServiceException) throwable;
      }

      throw new RestLiServiceException(HttpStatus.S_500_INTERNAL_SERVER_ERROR, throwable);
    }
  }

  /**
   * Similar to {@link #toTask(Supplier)} but the supplier is expected to return an {@link Optional} instead.
   * A {@link RestLiServiceException} with 404 HTTP status code will be thrown if the optional is emtpy.
   * @param supplier The supplier to execute
   * @return A parseq {@link Task}
   */
  @Nonnull
  public static <T> Task<T> toTaskFromOptional(@Nonnull Supplier<Optional<T>> supplier) {
    return toTask(() -> supplier.get().orElseThrow(RestliUtils::resourceNotFoundException));
  }

  @Nonnull
  public static RestLiServiceException resourceNotFoundException() {
    return resourceNotFoundException(null);
  }

  @Nonnull
  public static RestLiServiceException resourceNotFoundException(@Nullable String message) {
    return new RestLiServiceException(HttpStatus.S_404_NOT_FOUND, message);
  }

  @Nonnull
  public static RestLiServiceException badRequestException(@Nullable String message) {
    return new RestLiServiceException(HttpStatus.S_400_BAD_REQUEST, message);
  }

  @Nonnull
  public static RestLiServiceException invalidArgumentsException(@Nullable String message) {
    return new RestLiServiceException(HttpStatus.S_412_PRECONDITION_FAILED, message);
  }

  @Nonnull
  public static <URN extends Urn> BackfillResult buildBackfillResult(
      @Nonnull Map<URN, Map<Class<? extends RecordTemplate>, Optional<? extends RecordTemplate>>> backfilledAspects) {

    final Set<URN> urns = new TreeSet<>(Comparator.comparing(Urn::toString));
    urns.addAll(backfilledAspects.keySet());
    return new BackfillResult().setEntities(new BackfillResultEntityArray(
        urns.stream().map(urn -> buildBackfillResultEntity(urn, backfilledAspects.get(urn)))
            .collect(Collectors.toList())));
  }

  private static <URN extends Urn> BackfillResultEntity buildBackfillResultEntity(@Nonnull URN urn,
      Map<Class<? extends RecordTemplate>, java.util.Optional<? extends RecordTemplate>> aspectMap) {

    return new BackfillResultEntity()
        .setUrn(urn)
        .setAspects(new StringArray(aspectMap.entrySet().stream()
            .filter(aspect -> aspect.getValue().isPresent())
            .map(aspect -> aspect.getKey().getCanonicalName())
            .collect(Collectors.toList()))
        );
  }
}
