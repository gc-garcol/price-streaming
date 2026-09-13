package gc.garcol.pricestreaming.stream;

/**
 * Push payload for a symbol pair that left the join, so a subscriber can drop the row instead of
 * waiting for its next reload. Shares the channel with {@link FullConfig}, and the {@code deleted}
 * flag is what tells the two apart.
 */
public record FullConfigDeletion(String symbolPair, boolean deleted) {

    public static FullConfigDeletion of(String symbolPair) {
        return new FullConfigDeletion(symbolPair, true);
    }
}
