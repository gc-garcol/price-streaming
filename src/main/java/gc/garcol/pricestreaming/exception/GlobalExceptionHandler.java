package gc.garcol.pricestreaming.exception;

import lombok.RequiredArgsConstructor;
import org.springframework.context.MessageSource;
import org.springframework.http.ProblemDetail;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

@RestControllerAdvice
@RequiredArgsConstructor
public class GlobalExceptionHandler {

    private final MessageSource messageSource;

    @ExceptionHandler(AppException.class)
    public ProblemDetail handleAppException(AppException exception, Locale locale) {
        return toProblemDetail(exception.getErrorCode(), exception.getArgs(), locale);
    }

    @ExceptionHandler(MethodArgumentNotValidException.class)
    public ProblemDetail handleValidation(MethodArgumentNotValidException exception, Locale locale) {
        ProblemDetail problemDetail = toProblemDetail(ErrorCode.VALIDATION_FAILED, new Object[0], locale);
        Map<String, String> fieldErrors = exception.getBindingResult().getFieldErrors().stream()
                .collect(Collectors.toMap(
                        error -> error.getField(),
                        error -> error.getDefaultMessage() == null ? "" : error.getDefaultMessage(),
                        (first, second) -> first));
        problemDetail.setProperty("errors", fieldErrors);
        return problemDetail;
    }

    private ProblemDetail toProblemDetail(ErrorCode errorCode, Object[] args, Locale locale) {
        String message = messageSource.getMessage(errorCode.messageKey(), args, errorCode.name(), locale);
        ProblemDetail problemDetail = ProblemDetail.forStatusAndDetail(errorCode.getStatus(), message);
        problemDetail.setProperty("error_code", errorCode.name());
        return problemDetail;
    }
}
