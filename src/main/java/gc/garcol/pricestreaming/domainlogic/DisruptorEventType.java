package gc.garcol.pricestreaming.domainlogic;

public enum DisruptorEventType {

    NONE,
    PRICE_CONFIG, // load price only
    PRICE_CONFIG_CHANGED, // for update/created
    PRICE_CONFIG_DROP, // for delete
    SYMBOL_PRICE, // load symbol price only
    SYMBOL_PRICE_CHANGED, // one symbol price change
}
