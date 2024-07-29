--------------------- MODULE CoordinatorWithGroupCommit ---------------------
EXTENDS Integers, Sequences, TLC
CONSTANT Null

(* --algorithm CoordinatorWithGroupCommit 
variables
    normal_group_key = "p0",
    normal_group_slots = {
        [client_id |-> "c1", tx_child_id |-> "tx1", state |-> "not_ready"],
        [client_id |-> "c2", tx_child_id |-> "tx2", state |-> "not_ready"],
        [client_id |-> "c3", tx_child_id |-> "tx3", state |-> "not_ready"]
    },
    delayed_groups = {},
    coordinator_table = {},
    response_to_clients = {}
    ;

define
    TransactionIsValid(tx) ==
        <<tx["client_id"], tx["tx_child_id"]>> \in {
            <<"c1", "tx1">>,
            <<"c2", "tx2">>,
            <<"c3", "tx3">>
        }
        /\ tx["state"] \in {"ready", "not_ready"}

    TransactionsInNormalGroupAreValid ==
        \A tx \in normal_group_slots : TransactionIsValid(tx)

    TransactionsInDelayedGroupsAreValid ==
        \A tx \in delayed_groups : TransactionIsValid(tx)

    AllTransactionsAreValid == TransactionsInNormalGroupAreValid /\ TransactionsInDelayedGroupsAreValid

    ResponseToClientIsValid(client) ==
        client["client_id"] \in {"c1", "c2", "c3"} /\ client["state"] \in {"committed", "aborted"}

    AllResponseToClientsAreValid == \A client \in response_to_clients : ResponseToClientIsValid(client)

    CoordinatorRecordIsValid(record) ==
        (record.tx_id = <<normal_group_key>> /\ record.state = "committed" /\ record.tx_child_ids \ {"tx1", "tx2", "tx3"} = {})
        \/ (record.tx_id = <<normal_group_key>> /\ record.state = "aborted" /\ record.tx_child_ids = {})
        \/ (record.tx_id = <<normal_group_key, "tx1">> /\ record.state \in {"committed", "aborted"} /\ record.tx_child_ids = {})
        \/ (record.tx_id = <<normal_group_key, "tx2">> /\ record.state \in {"committed", "aborted"} /\ record.tx_child_ids = {})
        \/ (record.tx_id = <<normal_group_key, "tx3">> /\ record.state \in {"committed", "aborted"} /\ record.tx_child_ids = {})

    AllCoordinatorRecordsAreValid == \A record \in coordinator_table : CoordinatorRecordIsValid(record)

    Invariant == AllTransactionsAreValid /\ AllResponseToClientsAreValid /\ AllCoordinatorRecordsAreValid

    CoordinatorStateIsSameAs(tx_child_id, expected_state) ==
        (\E record \in coordinator_table : record["tx_id"] = <<normal_group_key>>
        /\ tx_child_id \in record["tx_child_ids"]
        /\ record["state"] = expected_state)
        \/
        (\E record \in coordinator_table : record["tx_id"] = <<normal_group_key, tx_child_id>>
        /\ record["tx_child_ids"] = {}
        /\ record["state"] = expected_state)

    CoordinatorState(tx_child_id) ==
        IF \E record \in coordinator_table : record["tx_id"] = <<normal_group_key>> /\ tx_child_id \in record["tx_child_ids"] THEN
            LET
                record == CHOOSE record \in coordinator_table : record["tx_id"] = <<normal_group_key>> /\ tx_child_id \in record["tx_child_ids"]
            IN
                record["state"]
        ELSE IF \E record \in coordinator_table : record["tx_id"] = <<normal_group_key, tx_child_id>> /\ record["tx_child_ids"] = {} THEN
            LET
                record == CHOOSE record \in coordinator_table : record["tx_id"] = <<normal_group_key, tx_child_id>> /\ record["tx_child_ids"] = {}
            IN
                record["state"]
        ELSE
            Null

    CoordinatorStateShouldContainAnyRecord(tx_child_id) ==
        (\E record \in coordinator_table : record["tx_id"] = <<normal_group_key>> /\ tx_child_id \in record["tx_child_ids"])
        \/ \E record \in coordinator_table : record["tx_id"] = <<normal_group_key, tx_child_id>>

    CoordinatorStateShouldNotContainMultipleRecords(tx_child_id) ==
        ~(
            (\E record \in coordinator_table : record["tx_id"] = <<normal_group_key>> /\ tx_child_id \in record["tx_child_ids"])
            /\ \E record \in coordinator_table : record["tx_id"] = <<normal_group_key, tx_child_id>>
        )

    StateInResponseToClient(tx_child_id) ==
        IF \E client \in response_to_clients : client["tx_child_id"] = tx_child_id THEN
            LET 
                client == CHOOSE client \in response_to_clients : client["tx_child_id"] = tx_child_id
            IN
                client["state"]
        ELSE
            Null

    ExpectedState ==
        normal_group_slots = {}
        /\ delayed_groups = {}
        /\ (\A tx_child_id \in {"tx1", "tx2", "tx3"} :
                CoordinatorStateShouldContainAnyRecord(tx_child_id)
                /\ CoordinatorStateShouldNotContainMultipleRecords(tx_child_id))
                \* TODO: Check response_to_clients.

    EventualExpectedState == <>[]ExpectedState
end define;

fair process Client = "client"
begin Client:
    while normal_group_slots /= {} \/ delayed_groups /= {} do
        \* Make transactions ready.
        either
            with ready_tx \in normal_group_slots do
                normal_group_slots := {IF tx.tx_child_id = ready_tx.tx_child_id THEN [tx EXCEPT !.state = "ready"] ELSE tx : tx \in normal_group_slots};
            end with;
        or
            with ready_tx \in delayed_groups do
                delayed_groups := {IF tx.tx_child_id = ready_tx.tx_child_id THEN [tx EXCEPT !.state = "ready"] ELSE tx : tx \in delayed_groups};
            end with;
        or
            skip;
        end either;
    end while;
end process;

fair process GroupCommitter = "group-committer"
variable
    tmp_tx = Null
begin GroupCommit:
    while normal_group_slots /= {} \/ delayed_groups /= {} do
        either
            \* Move delayed transactions to the delayed groups.
            with tx \in normal_group_slots do
                if tx["state"] = "not_ready" then
                    delayed_groups := delayed_groups \union {tx};
                    normal_group_slots := normal_group_slots \ {tx};
                end if;
            end with;
        or
            \* Commit ready transactions in the normal group.
            if normal_group_slots /= {} /\ \A tx \in normal_group_slots: tx["state"] = "ready" then
                if \E ctx \in coordinator_table : ctx["tx_id"] = <<normal_group_key>> then
                    \* The existing record must be created by lazy-recovery.
                    assert ~(\E ctx \in coordinator_table : ctx["tx_id"] = <<normal_group_key>> /\ ctx["tx_child_ids"] /= {});
                    assert ~(\E ctx \in coordinator_table : ctx["tx_id"] = <<normal_group_key>> /\ ctx["state"] /= "aborted");
                    \* The client must not have received any response.
                    assert ~({tx["client_id"] : tx \in normal_group_slots} \subseteq {client["client_id"] : client \in response_to_clients});

                    response_to_clients := response_to_clients \union {
                        [client_id |-> tx["client_id"], tx_child_id |-> tx["tx_child_id"], state |-> "aborted"] : tx \in normal_group_slots
                    };
                    normal_group_slots := {};
                else
                    coordinator_table := coordinator_table \union {[
                        tx_id |-> <<normal_group_key>>,
                        tx_child_ids |-> {tx["tx_child_id"] : tx \in normal_group_slots},
                        state |-> "committed"
                    ]};

                    SendCommitToClientForNormalGroupCommit:
                    either
                        response_to_clients := response_to_clients \union {
                            [client_id |-> tx["client_id"], tx_child_id |-> tx["tx_child_id"], state |-> "committed"] : tx \in normal_group_slots
                        };
                    or
                        skip;
                    end either;
                    normal_group_slots := {};
                end if;
            end if;
        or
            \* Commit ready transactions in the delayed groups.
            with tx \in delayed_groups do
                tmp_tx := tx;
            end with;

            if tmp_tx["state"] = "ready" then
                if \E ctx \in coordinator_table : ctx["tx_id"] = <<normal_group_key, tmp_tx["tx_child_id"]>> then
                    \* The client must not have received any response.
                    assert ~({tmp_tx["client_id"]} \subseteq {client["client_id"] : client \in response_to_clients});

                    response_to_clients := response_to_clients \union {
                        [client_id |-> tmp_tx["client_id"], tx_child_id |-> tmp_tx["tx_child_id"], state |-> "aborted"]
                    };
                    delayed_groups := delayed_groups \ {tmp_tx};
                else
                    coordinator_table := coordinator_table \union {[
                        tx_id |-> <<normal_group_key, tmp_tx["tx_child_id"]>>,
                        tx_child_ids |-> {},
                        state |-> "committed"
                    ]};

                    SendCommitToClientForDelayedGroupCommit:
                    either
                        response_to_clients := response_to_clients \union {
                            [client_id |-> tmp_tx["client_id"], tx_child_id |-> tmp_tx["tx_child_id"], state |-> "committed"]
                        };
                    or
                        skip;
                    end either;

                    delayed_groups := delayed_groups \ {tmp_tx};
                end if;
            end if;
        or
            skip;
        end either;
    end while;
end process;

fair process LazyRecovery = "lazy-recovery"
variable
    tx_child_ids_in_prepared_records = {"tx1", "tx2", "tx3"},
    tmp_tx_child_id = Null,
    already_committed = FALSE;
begin LazyRecovery:
    \* Make transactions abort.
    while ~ExpectedState do
        with tx_child_id \in tx_child_ids_in_prepared_records do
            tmp_tx_child_id := tx_child_id;
        end with;

        \* First, abort with the parent ID.
        if \E ctx \in coordinator_table : ctx["tx_id"] = <<normal_group_key>> then
            if \E ctx \in coordinator_table : ctx["tx_id"] = <<normal_group_key>> /\ ctx["state"] = "committed" /\ tmp_tx_child_id \in ctx["tx_child_ids"] then
                \* This transaction is already committed.
                already_committed := TRUE;
            elsif \E ctx \in coordinator_table : ctx["tx_id"] = <<normal_group_key>> /\ ctx["state"] = "committed" /\ tmp_tx_child_id \notin ctx["tx_child_ids"] then
                \* The normal group was committed, but this transaction isn't committed.
                already_committed := FALSE;
            elsif \E ctx \in coordinator_table : ctx["tx_id"] = <<normal_group_key>> /\ ctx["state"] = "aborted" /\ ctx["tx_child_ids"] = {} then
                \* The normal group was aborted by other lazy recovery.
                already_committed := FALSE;
            else
                assert FALSE;
            end if;
        else
            coordinator_table := coordinator_table \union {[
                tx_id |-> <<normal_group_key>>,
                tx_child_ids |-> {},
                state |-> "aborted"
            ]};
        end if;

        \* Then, abort with the full ID.
        AbortWithFullId:
        if already_committed \/ (\E ctx \in coordinator_table : ctx["tx_id"] = <<normal_group_key, tmp_tx_child_id>>) then
            \* This transaction is already committed or aborted.
        else
            coordinator_table := coordinator_table \union {[
                tx_id |-> <<normal_group_key, tmp_tx_child_id>>,
                tx_child_ids |-> {},
                state |-> "aborted"
            ]};
        end if;
        tx_child_ids_in_prepared_records := tx_child_ids_in_prepared_records \ {tmp_tx_child_id};
    end while;
end process;

end algorithm *)
\* BEGIN TRANSLATION (chksum(pcal) = "e260ebf3" /\ chksum(tla) = "b1c46d7a")
\* Label Client of process Client at line 106 col 5 changed to Client_
\* Label LazyRecovery of process LazyRecovery at line 214 col 5 changed to LazyRecovery_
VARIABLES normal_group_key, normal_group_slots, delayed_groups, 
          coordinator_table, response_to_clients, pc

(* define statement *)
TransactionIsValid(tx) ==
    <<tx["client_id"], tx["tx_child_id"]>> \in {
        <<"c1", "tx1">>,
        <<"c2", "tx2">>,
        <<"c3", "tx3">>
    }
    /\ tx["state"] \in {"ready", "not_ready"}

TransactionsInNormalGroupAreValid ==
    \A tx \in normal_group_slots : TransactionIsValid(tx)

TransactionsInDelayedGroupsAreValid ==
    \A tx \in delayed_groups : TransactionIsValid(tx)

AllTransactionsAreValid == TransactionsInNormalGroupAreValid /\ TransactionsInDelayedGroupsAreValid

ResponseToClientIsValid(client) ==
    client["client_id"] \in {"c1", "c2", "c3"} /\ client["state"] \in {"committed", "aborted"}

AllResponseToClientsAreValid == \A client \in response_to_clients : ResponseToClientIsValid(client)

CoordinatorRecordIsValid(record) ==
    (record.tx_id = <<normal_group_key>> /\ record.state = "committed" /\ record.tx_child_ids \ {"tx1", "tx2", "tx3"} = {})
    \/ (record.tx_id = <<normal_group_key>> /\ record.state = "aborted" /\ record.tx_child_ids = {})
    \/ (record.tx_id = <<normal_group_key, "tx1">> /\ record.state \in {"committed", "aborted"} /\ record.tx_child_ids = {})
    \/ (record.tx_id = <<normal_group_key, "tx2">> /\ record.state \in {"committed", "aborted"} /\ record.tx_child_ids = {})
    \/ (record.tx_id = <<normal_group_key, "tx3">> /\ record.state \in {"committed", "aborted"} /\ record.tx_child_ids = {})

AllCoordinatorRecordsAreValid == \A record \in coordinator_table : CoordinatorRecordIsValid(record)

Invariant == AllTransactionsAreValid /\ AllResponseToClientsAreValid /\ AllCoordinatorRecordsAreValid

CoordinatorStateIsSameAs(tx_child_id, expected_state) ==
    (\E record \in coordinator_table : record["tx_id"] = <<normal_group_key>>
    /\ tx_child_id \in record["tx_child_ids"]
    /\ record["state"] = expected_state)
    \/
    (\E record \in coordinator_table : record["tx_id"] = <<normal_group_key, tx_child_id>>
    /\ record["tx_child_ids"] = {}
    /\ record["state"] = expected_state)

CoordinatorState(tx_child_id) ==
    IF \E record \in coordinator_table : record["tx_id"] = <<normal_group_key>> /\ tx_child_id \in record["tx_child_ids"] THEN
        LET
            record == CHOOSE record \in coordinator_table : record["tx_id"] = <<normal_group_key>> /\ tx_child_id \in record["tx_child_ids"]
        IN
            record["state"]
    ELSE IF \E record \in coordinator_table : record["tx_id"] = <<normal_group_key, tx_child_id>> /\ record["tx_child_ids"] = {} THEN
        LET
            record == CHOOSE record \in coordinator_table : record["tx_id"] = <<normal_group_key, tx_child_id>> /\ record["tx_child_ids"] = {}
        IN
            record["state"]
    ELSE
        Null

CoordinatorStateShouldContainAnyRecord(tx_child_id) ==
    (\E record \in coordinator_table : record["tx_id"] = <<normal_group_key>> /\ tx_child_id \in record["tx_child_ids"])
    \/ \E record \in coordinator_table : record["tx_id"] = <<normal_group_key, tx_child_id>>

CoordinatorStateShouldNotContainMultipleRecords(tx_child_id) ==
    ~(
        (\E record \in coordinator_table : record["tx_id"] = <<normal_group_key>> /\ tx_child_id \in record["tx_child_ids"])
        /\ \E record \in coordinator_table : record["tx_id"] = <<normal_group_key, tx_child_id>>
    )

StateInResponseToClient(tx_child_id) ==
    IF \E client \in response_to_clients : client["tx_child_id"] = tx_child_id THEN
        LET
            client == CHOOSE client \in response_to_clients : client["tx_child_id"] = tx_child_id
        IN
            client["state"]
    ELSE
        Null

ExpectedState ==
    normal_group_slots = {}
    /\ delayed_groups = {}
    /\ (\A tx_child_id \in {"tx1", "tx2", "tx3"} :
            CoordinatorStateShouldContainAnyRecord(tx_child_id)
            /\ CoordinatorStateShouldNotContainMultipleRecords(tx_child_id))


EventualExpectedState == <>[]ExpectedState

VARIABLES tmp_tx, tx_child_ids_in_prepared_records, tmp_tx_child_id, 
          already_committed

vars == << normal_group_key, normal_group_slots, delayed_groups, 
           coordinator_table, response_to_clients, pc, tmp_tx, 
           tx_child_ids_in_prepared_records, tmp_tx_child_id, 
           already_committed >>

ProcSet == {"client"} \cup {"group-committer"} \cup {"lazy-recovery"}

Init == (* Global variables *)
        /\ normal_group_key = "p0"
        /\ normal_group_slots =                      {
                                    [client_id |-> "c1", tx_child_id |-> "tx1", state |-> "not_ready"],
                                    [client_id |-> "c2", tx_child_id |-> "tx2", state |-> "not_ready"],
                                    [client_id |-> "c3", tx_child_id |-> "tx3", state |-> "not_ready"]
                                }
        /\ delayed_groups = {}
        /\ coordinator_table = {}
        /\ response_to_clients = {}
        (* Process GroupCommitter *)
        /\ tmp_tx = Null
        (* Process LazyRecovery *)
        /\ tx_child_ids_in_prepared_records = {"tx1", "tx2", "tx3"}
        /\ tmp_tx_child_id = Null
        /\ already_committed = FALSE
        /\ pc = [self \in ProcSet |-> CASE self = "client" -> "Client_"
                                        [] self = "group-committer" -> "GroupCommit"
                                        [] self = "lazy-recovery" -> "LazyRecovery_"]

Client_ == /\ pc["client"] = "Client_"
           /\ IF normal_group_slots /= {} \/ delayed_groups /= {}
                 THEN /\ \/ /\ \E ready_tx \in normal_group_slots:
                                 normal_group_slots' = {IF tx.tx_child_id = ready_tx.tx_child_id THEN [tx EXCEPT !.state = "ready"] ELSE tx : tx \in normal_group_slots}
                            /\ UNCHANGED delayed_groups
                         \/ /\ \E ready_tx \in delayed_groups:
                                 delayed_groups' = {IF tx.tx_child_id = ready_tx.tx_child_id THEN [tx EXCEPT !.state = "ready"] ELSE tx : tx \in delayed_groups}
                            /\ UNCHANGED normal_group_slots
                         \/ /\ TRUE
                            /\ UNCHANGED <<normal_group_slots, delayed_groups>>
                      /\ pc' = [pc EXCEPT !["client"] = "Client_"]
                 ELSE /\ pc' = [pc EXCEPT !["client"] = "Done"]
                      /\ UNCHANGED << normal_group_slots, delayed_groups >>
           /\ UNCHANGED << normal_group_key, coordinator_table, 
                           response_to_clients, tmp_tx, 
                           tx_child_ids_in_prepared_records, tmp_tx_child_id, 
                           already_committed >>

Client == Client_

GroupCommit == /\ pc["group-committer"] = "GroupCommit"
               /\ IF normal_group_slots /= {} \/ delayed_groups /= {}
                     THEN /\ \/ /\ \E tx \in normal_group_slots:
                                     IF tx["state"] = "not_ready"
                                        THEN /\ delayed_groups' = (delayed_groups \union {tx})
                                             /\ normal_group_slots' = normal_group_slots \ {tx}
                                        ELSE /\ TRUE
                                             /\ UNCHANGED << normal_group_slots, 
                                                             delayed_groups >>
                                /\ pc' = [pc EXCEPT !["group-committer"] = "GroupCommit"]
                                /\ UNCHANGED <<coordinator_table, response_to_clients, tmp_tx>>
                             \/ /\ IF normal_group_slots /= {} /\ \A tx \in normal_group_slots: tx["state"] = "ready"
                                      THEN /\ IF \E ctx \in coordinator_table : ctx["tx_id"] = <<normal_group_key>>
                                                 THEN /\ Assert(~(\E ctx \in coordinator_table : ctx["tx_id"] = <<normal_group_key>> /\ ctx["tx_child_ids"] /= {}), 
                                                                "Failure of assertion at line 140, column 21.")
                                                      /\ Assert(~(\E ctx \in coordinator_table : ctx["tx_id"] = <<normal_group_key>> /\ ctx["state"] /= "aborted"), 
                                                                "Failure of assertion at line 141, column 21.")
                                                      /\ Assert(~({tx["client_id"] : tx \in normal_group_slots} \subseteq {client["client_id"] : client \in response_to_clients}), 
                                                                "Failure of assertion at line 143, column 21.")
                                                      /\ response_to_clients' = (                       response_to_clients \union {
                                                                                     [client_id |-> tx["client_id"], tx_child_id |-> tx["tx_child_id"], state |-> "aborted"] : tx \in normal_group_slots
                                                                                 })
                                                      /\ normal_group_slots' = {}
                                                      /\ pc' = [pc EXCEPT !["group-committer"] = "GroupCommit"]
                                                      /\ UNCHANGED coordinator_table
                                                 ELSE /\ coordinator_table' = (                     coordinator_table \union {[
                                                                                   tx_id |-> <<normal_group_key>>,
                                                                                   tx_child_ids |-> {tx["tx_child_id"] : tx \in normal_group_slots},
                                                                                   state |-> "committed"
                                                                               ]})
                                                      /\ pc' = [pc EXCEPT !["group-committer"] = "SendCommitToClientForNormalGroupCommit"]
                                                      /\ UNCHANGED << normal_group_slots, 
                                                                      response_to_clients >>
                                      ELSE /\ pc' = [pc EXCEPT !["group-committer"] = "GroupCommit"]
                                           /\ UNCHANGED << normal_group_slots, 
                                                           coordinator_table, 
                                                           response_to_clients >>
                                /\ UNCHANGED <<delayed_groups, tmp_tx>>
                             \/ /\ \E tx \in delayed_groups:
                                     tmp_tx' = tx
                                /\ IF tmp_tx'["state"] = "ready"
                                      THEN /\ IF \E ctx \in coordinator_table : ctx["tx_id"] = <<normal_group_key, tmp_tx'["tx_child_id"]>>
                                                 THEN /\ Assert(~({tmp_tx'["client_id"]} \subseteq {client["client_id"] : client \in response_to_clients}), 
                                                                "Failure of assertion at line 176, column 21.")
                                                      /\ response_to_clients' = (                       response_to_clients \union {
                                                                                     [client_id |-> tmp_tx'["client_id"], tx_child_id |-> tmp_tx'["tx_child_id"], state |-> "aborted"]
                                                                                 })
                                                      /\ delayed_groups' = delayed_groups \ {tmp_tx'}
                                                      /\ pc' = [pc EXCEPT !["group-committer"] = "GroupCommit"]
                                                      /\ UNCHANGED coordinator_table
                                                 ELSE /\ coordinator_table' = (                     coordinator_table \union {[
                                                                                   tx_id |-> <<normal_group_key, tmp_tx'["tx_child_id"]>>,
                                                                                   tx_child_ids |-> {},
                                                                                   state |-> "committed"
                                                                               ]})
                                                      /\ pc' = [pc EXCEPT !["group-committer"] = "SendCommitToClientForDelayedGroupCommit"]
                                                      /\ UNCHANGED << delayed_groups, 
                                                                      response_to_clients >>
                                      ELSE /\ pc' = [pc EXCEPT !["group-committer"] = "GroupCommit"]
                                           /\ UNCHANGED << delayed_groups, 
                                                           coordinator_table, 
                                                           response_to_clients >>
                                /\ UNCHANGED normal_group_slots
                             \/ /\ TRUE
                                /\ pc' = [pc EXCEPT !["group-committer"] = "GroupCommit"]
                                /\ UNCHANGED <<normal_group_slots, delayed_groups, coordinator_table, response_to_clients, tmp_tx>>
                     ELSE /\ pc' = [pc EXCEPT !["group-committer"] = "Done"]
                          /\ UNCHANGED << normal_group_slots, delayed_groups, 
                                          coordinator_table, 
                                          response_to_clients, tmp_tx >>
               /\ UNCHANGED << normal_group_key, 
                               tx_child_ids_in_prepared_records, 
                               tmp_tx_child_id, already_committed >>

SendCommitToClientForNormalGroupCommit == /\ pc["group-committer"] = "SendCommitToClientForNormalGroupCommit"
                                          /\ \/ /\ response_to_clients' = (                       response_to_clients \union {
                                                                               [client_id |-> tx["client_id"], tx_child_id |-> tx["tx_child_id"], state |-> "committed"] : tx \in normal_group_slots
                                                                           })
                                             \/ /\ TRUE
                                                /\ UNCHANGED response_to_clients
                                          /\ normal_group_slots' = {}
                                          /\ pc' = [pc EXCEPT !["group-committer"] = "GroupCommit"]
                                          /\ UNCHANGED << normal_group_key, 
                                                          delayed_groups, 
                                                          coordinator_table, 
                                                          tmp_tx, 
                                                          tx_child_ids_in_prepared_records, 
                                                          tmp_tx_child_id, 
                                                          already_committed >>

SendCommitToClientForDelayedGroupCommit == /\ pc["group-committer"] = "SendCommitToClientForDelayedGroupCommit"
                                           /\ \/ /\ response_to_clients' = (                       response_to_clients \union {
                                                                                [client_id |-> tmp_tx["client_id"], tx_child_id |-> tmp_tx["tx_child_id"], state |-> "committed"]
                                                                            })
                                              \/ /\ TRUE
                                                 /\ UNCHANGED response_to_clients
                                           /\ delayed_groups' = delayed_groups \ {tmp_tx}
                                           /\ pc' = [pc EXCEPT !["group-committer"] = "GroupCommit"]
                                           /\ UNCHANGED << normal_group_key, 
                                                           normal_group_slots, 
                                                           coordinator_table, 
                                                           tmp_tx, 
                                                           tx_child_ids_in_prepared_records, 
                                                           tmp_tx_child_id, 
                                                           already_committed >>

GroupCommitter == GroupCommit \/ SendCommitToClientForNormalGroupCommit
                     \/ SendCommitToClientForDelayedGroupCommit

LazyRecovery_ == /\ pc["lazy-recovery"] = "LazyRecovery_"
                 /\ IF ~ExpectedState
                       THEN /\ \E tx_child_id \in tx_child_ids_in_prepared_records:
                                 tmp_tx_child_id' = tx_child_id
                            /\ IF \E ctx \in coordinator_table : ctx["tx_id"] = <<normal_group_key>>
                                  THEN /\ IF \E ctx \in coordinator_table : ctx["tx_id"] = <<normal_group_key>> /\ ctx["state"] = "committed" /\ tmp_tx_child_id' \in ctx["tx_child_ids"]
                                             THEN /\ already_committed' = TRUE
                                             ELSE /\ IF \E ctx \in coordinator_table : ctx["tx_id"] = <<normal_group_key>> /\ ctx["state"] = "committed" /\ tmp_tx_child_id' \notin ctx["tx_child_ids"]
                                                        THEN /\ already_committed' = FALSE
                                                        ELSE /\ IF \E ctx \in coordinator_table : ctx["tx_id"] = <<normal_group_key>> /\ ctx["state"] = "aborted" /\ ctx["tx_child_ids"] = {}
                                                                   THEN /\ already_committed' = FALSE
                                                                   ELSE /\ Assert(FALSE, 
                                                                                  "Failure of assertion at line 231, column 17.")
                                                                        /\ UNCHANGED already_committed
                                       /\ UNCHANGED coordinator_table
                                  ELSE /\ coordinator_table' = (                     coordinator_table \union {[
                                                                    tx_id |-> <<normal_group_key>>,
                                                                    tx_child_ids |-> {},
                                                                    state |-> "aborted"
                                                                ]})
                                       /\ UNCHANGED already_committed
                            /\ pc' = [pc EXCEPT !["lazy-recovery"] = "AbortWithFullId"]
                       ELSE /\ pc' = [pc EXCEPT !["lazy-recovery"] = "Done"]
                            /\ UNCHANGED << coordinator_table, tmp_tx_child_id, 
                                            already_committed >>
                 /\ UNCHANGED << normal_group_key, normal_group_slots, 
                                 delayed_groups, response_to_clients, tmp_tx, 
                                 tx_child_ids_in_prepared_records >>

AbortWithFullId == /\ pc["lazy-recovery"] = "AbortWithFullId"
                   /\ IF already_committed \/ (\E ctx \in coordinator_table : ctx["tx_id"] = <<normal_group_key, tmp_tx_child_id>>)
                         THEN /\ UNCHANGED coordinator_table
                         ELSE /\ coordinator_table' = (                     coordinator_table \union {[
                                                           tx_id |-> <<normal_group_key, tmp_tx_child_id>>,
                                                           tx_child_ids |-> {},
                                                           state |-> "aborted"
                                                       ]})
                   /\ tx_child_ids_in_prepared_records' = tx_child_ids_in_prepared_records \ {tmp_tx_child_id}
                   /\ pc' = [pc EXCEPT !["lazy-recovery"] = "LazyRecovery_"]
                   /\ UNCHANGED << normal_group_key, normal_group_slots, 
                                   delayed_groups, response_to_clients, tmp_tx, 
                                   tmp_tx_child_id, already_committed >>

LazyRecovery == LazyRecovery_ \/ AbortWithFullId

(* Allow infinite stuttering to prevent deadlock on termination. *)
Terminating == /\ \A self \in ProcSet: pc[self] = "Done"
               /\ UNCHANGED vars

Next == Client \/ GroupCommitter \/ LazyRecovery
           \/ Terminating

Spec == /\ Init /\ [][Next]_vars
        /\ WF_vars(Client)
        /\ WF_vars(GroupCommitter)
        /\ WF_vars(LazyRecovery)

Termination == <>(\A self \in ProcSet: pc[self] = "Done")

\* END TRANSLATION 

=============================================================================
\* Modification History
\* Last modified Mon Jul 29 18:41:30 JST 2024 by mitsunorikomatsu
\* Last modified Fri Jul 26 23:38:27 JST 2024 by komamitsu
\* Created Thu Jul 25 17:04:35 JST 2024 by mitsunorikomatsu
