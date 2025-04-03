package com.scalar.db.datapopulator;

import com.scalar.db.api.DistributedTransaction;
import com.scalar.db.api.DistributedTransactionManager;
import com.scalar.db.api.Put;
import com.scalar.db.api.PutBuilder;
import com.scalar.db.io.BlobColumn;
import com.scalar.db.io.BooleanColumn;
import com.scalar.db.io.DateColumn;
import com.scalar.db.io.DoubleColumn;
import com.scalar.db.io.FloatColumn;
import com.scalar.db.io.IntColumn;
import com.scalar.db.io.Key;
import com.scalar.db.io.TextColumn;
import com.scalar.db.io.TimeColumn;
import com.scalar.db.io.TimestampColumn;
import com.scalar.db.io.TimestampTZColumn;
import com.scalar.db.service.TransactionFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class Main {
    public static void main(String[] args) throws IOException, InterruptedException {
        Path scalarDbPropertiesFilePath = Paths.get(args[0]);
        int recordCount = Integer.parseInt(args[1]);
        TransactionFactory transactionFactory =TransactionFactory.create(scalarDbPropertiesFilePath);
        DistributedTransactionManager transactionManager = transactionFactory.getTransactionManager();
        int coreCount = Runtime.getRuntime().availableProcessors();
        int batchSize = 10; // Number of Put operations per transaction
        ExecutorService executorService = Executors.newFixedThreadPool(coreCount);
        AtomicInteger insertedRecords = new AtomicInteger(0);
        for (int i = 1; i <= recordCount; i += batchSize) {
            int finalI = i;
            executorService.submit(
                    () -> {
                        try {
                            DistributedTransaction tx = transactionManager.start();
                            List<Put> puts = new ArrayList<>();
                            for (int j = finalI; j < finalI + batchSize && j <= recordCount; j++) {
                                Key partitionKey = Key.ofBigInt("col1", j);
                                Key clusteringKey =
                                        Key.newBuilder()
                                                .add(IntColumn.of("col2", j))
                                                .add(BooleanColumn.of("col3", true))
                                                .build();
                                PutBuilder.Buildable buildable =
                                        Put.newBuilder()
                                                .namespace("test")
                                                .table("all_columns")
                                                .partitionKey(partitionKey);
                                buildable.clusteringKey(clusteringKey);
                                buildable.value(FloatColumn.of("col4", Float.MIN_VALUE));
                                buildable.value(DoubleColumn.of("col5", Double.MIN_VALUE));
                                buildable.value(TextColumn.of("col6", "VALUEhhshgsdjgjasdgjhgasdbbnbfmsabmfjdlkjlksjakldhajgsdm!!s"));
                                buildable.value(
                                        BlobColumn.of("col7", "blob test value".getBytes(StandardCharsets.UTF_8)));
                                buildable.value(DateColumn.of("col8", LocalDate.of(2000, 1, 1)));
                                buildable.value(TimeColumn.of("col9", LocalTime.of(1, 1, 1)));
                                buildable.value(TimestampColumn.of("col10", LocalDateTime.of(2000, 1, 1, 1, 1)));
                                buildable.value(TimestampTZColumn.of("col11", Instant.ofEpochMilli(1740041740)));
                                puts.add(buildable.build());
                            }

                            for (Put put : puts) {
                                tx.put(put);
                            }
                            tx.commit();

                            int currentInsertedRecords = insertedRecords.addAndGet(batchSize);
                            if (currentInsertedRecords % 10000 == 0) {
                                System.out.println("Total records inserted: " + currentInsertedRecords);
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    });
        }
        executorService.shutdown();
        executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);

    }
}