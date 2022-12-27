package io.trino.tx;

import com.google.common.util.concurrent.ListenableFuture;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Supplier;

public class Transaction {

    public enum Status
    {
        INIT, PREPARING_COMMIT, COMMITTING, COMMITTED, ABORTING, ABORTED
    }

    private final String id;
    private final Status status;
    private final LocalDateTime created;
    private final List<DistTx> distTransactions = new ArrayList<>();
    private final ExecutorService txThreadPool = Executors.newFixedThreadPool(10);

    public Transaction()
    {
        this.id = UUID.randomUUID().toString();
        this.status = Status.INIT;
        this.created = LocalDateTime.now();
    }

    public String getId()
    {
        return id;
    }

    public Status getStatus()
    {
        return status;
    }

    public LocalDateTime getCreated() {
        return created;
    }

    public void addDistTx(DistTx tx)
    {
        this.distTransactions.add(tx);
    }

    public void start()
    {
        TransactionTask prepareCommitTask = new TransactionTask(distTransactions, distTx -> distTx.prepareCommit.get(), txThreadPool);
        if(prepareCommitTask.doTask(10000) == TransactionTask.Status.SUCCESS) {
            System.out.println("Global Commit");
            TransactionTask commitTask = new TransactionTask(distTransactions, distTx -> distTx.commit.get(), txThreadPool);
            commitTask.doTask(10000);
        }
        else {
            System.out.println("Global Rollback");
            TransactionTask rollbackTask = new TransactionTask(distTransactions, distTx -> distTx.rollback.get(), txThreadPool);
            rollbackTask.doTask(10000);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
        {
            return true;
        }
        if (o == null || getClass() != o.getClass())
        {
            return false;
        }

        Transaction that = (Transaction) o;
        return Objects.equals(id, that.id);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(id);
    }

    public static class DistTx
    {
        public enum Status
        {
            INIT, SUCCESS, FAIL
        }

        private final String nodeId;
        private Status status;

        Supplier<ListenableFuture<Void>> prepareCommit;
        Supplier<ListenableFuture<Void>> commit;
        Supplier<ListenableFuture<Void>> rollback;

        public DistTx(String nodeId, Supplier<ListenableFuture<Void>> prepareCommit, Supplier<ListenableFuture<Void>> commit, Supplier<ListenableFuture<Void>> rollback)
        {
            this.nodeId = nodeId;
            this.status = Status.INIT;
            this.prepareCommit = prepareCommit;
            this.commit = commit;
            this.rollback = rollback;
        }

        public String getNodeId()
        {
            return nodeId;
        }

        public Status getStatus()
        {
            return status;
        }
    }
}
