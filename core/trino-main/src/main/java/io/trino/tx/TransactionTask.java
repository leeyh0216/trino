package io.trino.tx;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

public class TransactionTask
{
    private final Map<Transaction.DistTx, DistTxResult> transactions = new ConcurrentHashMap<>();
    private final Function<Transaction.DistTx, ListenableFuture<Void>> distTxTask;
    private final ExecutorService taskThreadPool;
    private final CountDownLatch latch;
    private volatile Status status;

    public TransactionTask(List<Transaction.DistTx> transactions, Function<Transaction.DistTx, ListenableFuture<Void>> distTxTask, ExecutorService taskThreadPool)
    {
        for(Transaction.DistTx tx : transactions)
            this.transactions.put(tx, DistTxResult.createDefault());

        this.taskThreadPool = taskThreadPool;
        this.distTxTask = distTxTask;
        this.latch = new CountDownLatch(transactions.size());
        this.status = Status.INITED;
    }

    public Status doTask(long timeoutInMillis)
    {
        this.status = Status.RUNNING;
        submitDistTx();

        try {
            if(latch.await(timeoutInMillis, TimeUnit.MILLISECONDS))
            {
                return aggregateDistTxStatus();
            }
            else
            {
                return Status.RUNNING;
            }
        }
        catch(InterruptedException e)
        {
            return Status.FAILED;
        }
    }

    private Status aggregateDistTxStatus()
    {
        List<DistTxResult> results = new ArrayList<>(transactions.values());
        for(DistTxResult result : results)
        {
            if(result.status == DistTxResult.Status.FAILED)
                return Status.FAILED;
            else if(result.status == DistTxResult.Status.RUNNING || result.status == DistTxResult.Status.INITED)
                return Status.RUNNING;
        }
        return Status.SUCCESS;
    }
    private void submitDistTx()
    {
        for(Transaction.DistTx tx : transactions.keySet())
        {
            Futures.addCallback(distTxTask.apply(tx), new FutureCallback<>() {
                @Override
                public void onSuccess(Void result) {
                    transactions.put(tx, DistTxResult.createSuccess());
                    latch.countDown();
                }

                @Override
                public void onFailure(Throwable t) {
                    transactions.put(tx, DistTxResult.createFailed(t));
                    latch.countDown();
                }
            }, taskThreadPool);
        }
    }

    public static class DistTxResult
    {
        public enum Status
        {
            INITED, RUNNING, SUCCESS, FAILED;
        }

        private final Status status;
        private final Throwable exception;

        private DistTxResult(Status status)
        {
            this.status = status;
            this.exception = null;
        }

        private DistTxResult(Status status, Throwable e)
        {
            this.status = status;
            this.exception = e;
        }

        public static DistTxResult createDefault(){
            return new DistTxResult(Status.INITED);
        }

        public static DistTxResult createRunning(){
            return new DistTxResult(Status.RUNNING);
        }

        public static DistTxResult createSuccess(){
            return new DistTxResult(Status.SUCCESS);
        }

        public static DistTxResult createFailed(Throwable e)
        {
            return new DistTxResult(Status.FAILED, e);
        }
    }

    public enum Status
    {
        INITED, RUNNING, SUCCESS, FAILED;
    }
}
