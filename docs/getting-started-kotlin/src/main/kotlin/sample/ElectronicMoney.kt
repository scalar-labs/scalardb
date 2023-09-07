package sample

import com.scalar.db.api.DistributedTransactionManager
import com.scalar.db.api.Get
import com.scalar.db.api.Put
import com.scalar.db.exception.transaction.TransactionException
import com.scalar.db.io.Key
import com.scalar.db.service.TransactionFactory

class ElectronicMoney(scalarDBProperties: String) {
    companion object {
        private const val NAMESPACE = "emoney"
        private const val TABLENAME = "account"
        private const val ID = "id"
        private const val BALANCE = "balance"
    }

    private val manager: DistributedTransactionManager

    init {
        val factory = TransactionFactory.create(scalarDBProperties)
        manager = factory.transactionManager
    }

    @Throws(TransactionException::class)
    fun charge(id: String, amount: Int) {
        // Start a transaction
        val tx = manager.start()
        try {
            // Retrieve the current balance for id
            val get = Get.newBuilder()
                .namespace(NAMESPACE)
                .table(TABLENAME)
                .partitionKey(Key.ofText(ID, id))
                .build()
            val result = tx.get(get)

            // Calculate the balance
            var balance = amount
            if (result.isPresent) {
                val current = result.get().getInt(BALANCE)
                balance += current
            }

            // Update the balance
            val put = Put.newBuilder()
                .namespace(NAMESPACE)
                .table(TABLENAME)
                .partitionKey(Key.ofText(ID, id))
                .intValue(BALANCE, balance)
                .build()
            tx.put(put)

            // Commit the transaction (records are automatically recovered in case of failure)
            tx.commit()
        } catch (e: Exception) {
            tx.abort()
            throw e
        }
    }

    @Throws(TransactionException::class)
    fun pay(fromId: String, toId: String, amount: Int) {
        // Start a transaction
        val tx = manager.start()
        try {
            // Retrieve the current balances for ids
            val fromGet = Get.newBuilder()
                .namespace(NAMESPACE)
                .table(TABLENAME)
                .partitionKey(Key.ofText(ID, fromId))
                .build()
            val toGet = Get.newBuilder()
                .namespace(NAMESPACE)
                .table(TABLENAME)
                .partitionKey(Key.ofText(ID, toId))
                .build()
            val fromResult = tx.get(fromGet)
            val toResult = tx.get(toGet)

            // Calculate the balances (it assumes that both accounts exist)
            val newFromBalance = fromResult.get().getInt(BALANCE) - amount
            val newToBalance = toResult.get().getInt(BALANCE) + amount
            if (newFromBalance < 0) {
                throw RuntimeException("$fromId doesn't have enough balance.")
            }

            // Update the balances
            val fromPut = Put.newBuilder()
                .namespace(NAMESPACE)
                .table(TABLENAME)
                .partitionKey(Key.ofText(ID, fromId))
                .intValue(BALANCE, newFromBalance)
                .build()
            val toPut = Put.newBuilder()
                .namespace(NAMESPACE)
                .table(TABLENAME)
                .partitionKey(Key.ofText(ID, toId))
                .intValue(BALANCE, newToBalance)
                .build()
            tx.put(fromPut)
            tx.put(toPut)

            // Commit the transaction (records are automatically recovered in case of failure)
            tx.commit()
        } catch (e: Exception) {
            tx.abort()
            throw e
        }
    }

    @Throws(TransactionException::class)
    fun getBalance(id: String): Int {
        // Start a transaction
        val tx = manager.start()
        return try {
            // Retrieve the current balances for id
            val get = Get.newBuilder()
                .namespace(NAMESPACE)
                .table(TABLENAME)
                .partitionKey(Key.ofText(ID, id))
                .build()
            val result = tx.get(get)
            var balance = -1
            if (result.isPresent) {
                balance = result.get().getInt(BALANCE)
            }

            // Commit the transaction
            tx.commit()
            balance
        } catch (e: Exception) {
            tx.abort()
            throw e
        }
    }

    fun close() {
        manager.close()
    }
}
