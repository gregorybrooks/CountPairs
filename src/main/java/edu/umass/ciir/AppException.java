package edu.umass.ciir;

public class AppException extends RuntimeException {
    AppException(String msg) {
        super(msg);
    }
    public AppException(Exception cause) {
        super(cause);
    }
}