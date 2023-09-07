import sample.ElectronicMoney
import java.io.File
import kotlin.system.exitProcess

fun main(args: Array<String>) {
    var action: String? = null
    var amount = 0
    var to: String? = null
    var from: String? = null
    var id: String? = null
    var scalarDBProperties: String? = null
    var i = 0
    while (i < args.size) {
        if ("-action" == args[i]) {
            action = args[++i]
        } else if ("-amount" == args[i]) {
            amount = args[++i].toInt()
        } else if ("-to" == args[i]) {
            to = args[++i]
        } else if ("-from" == args[i]) {
            from = args[++i]
        } else if ("-id" == args[i]) {
            id = args[++i]
        } else if ("-config" == args[i]) {
            scalarDBProperties = args[++i]
        } else if ("-help" == args[i]) {
            printUsageAndExit()
            return
        }
        ++i
    }
    if (action == null) {
        printUsageAndExit()
        return
    }
    val eMoney = ElectronicMoney(
        scalarDBProperties ?: (System.getProperty("user.dir") + File.separator + "scalardb.properties")
    )
    if (action.equals("charge", ignoreCase = true)) {
        if (to == null || amount < 0) {
            printUsageAndExit()
            return
        }
        eMoney.charge(to, amount)
    } else if (action.equals("pay", ignoreCase = true)) {
        if (to == null || amount < 0 || from == null) {
            printUsageAndExit()
            return
        }
        eMoney.pay(from, to, amount)
    } else if (action.equals("getBalance", ignoreCase = true)) {
        if (id == null) {
            printUsageAndExit()
            return
        }
        val balance = eMoney.getBalance(id)
        println("The balance for $id is $balance")
    }
    eMoney.close()
}

fun printUsageAndExit() {
    System.err.println(
        "ElectronicMoneyMain -action charge/pay/getBalance [-amount number (needed for charge and pay)] [-to id (needed for charge and pay)] [-from id (needed for pay)] [-id id (needed for getBalance)]"
    )
    exitProcess(1)
}
