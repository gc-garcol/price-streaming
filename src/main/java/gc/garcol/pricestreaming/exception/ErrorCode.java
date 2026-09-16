package gc.garcol.pricestreaming.exception;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;

@Getter
@RequiredArgsConstructor
public enum ErrorCode {

    PRICE_CONFIG_NOT_FOUND(HttpStatus.NOT_FOUND),
    DUPLICATE_SYMBOL_PAIR(HttpStatus.CONFLICT),
    VALIDATION_FAILED(HttpStatus.BAD_REQUEST),
    STREAM_STORE_UNAVAILABLE(HttpStatus.SERVICE_UNAVAILABLE),
    INTERNAL_ERROR(HttpStatus.INTERNAL_SERVER_ERROR);

    private final HttpStatus status;

    public String messageKey() {
        return "error." + name().toLowerCase();
    }
}
