package com.scalar.db.dataloader.core.dataexport;

import com.scalar.db.api.DistributedTransaction;
import com.scalar.db.api.TransactionCrudOperable;
import lombok.Value;

@Value
public class ScannerWithTransaction {
  DistributedTransaction transaction;
  TransactionCrudOperable.Scanner scanner;
}
