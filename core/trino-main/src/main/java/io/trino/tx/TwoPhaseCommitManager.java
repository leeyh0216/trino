package io.trino.tx;

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class TwoPhaseCommitManager
{
    private final Set<Transaction> transactions;

    public TwoPhaseCommitManager()
    {
        this.transactions = Collections.newSetFromMap(new ConcurrentHashMap<>());
//        transactions = Collections.newSetFromMap(new ConcurrentHashMap<Transaction, Boolean>());
    }
}
