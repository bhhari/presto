package com.facebook.presto.spi.connector;

import java.util.List;
import java.util.Map;

public class ConnectorCommitResult
{

    public Map<String, QueryCommitResult> getR()
    {
        return r;
    }

    private final Map<String, QueryCommitResult> r;

    public ConnectorCommitResult(Map<String, QueryCommitResult> r)
    {
        this.r = r;
    }

 public interface QueryCommitResult
 {

        List<Integer> getInputTimes();
        List<Integer> getOutputTimes();
 }
}
