package com.siwei.transfer;

public interface TaskProcessor {
    String direction();
    Result process(Task task) throws Exception;

    final class Result {
        public final String status, phase, errorType, errorDetail;
        public Result(String status, String phase, String errorType, String errorDetail) {
            this.status = status; this.phase = phase; this.errorType = errorType; this.errorDetail = errorDetail;
        }
    }
}
