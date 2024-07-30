--------------------- MODULE CoordinatorWithGroupCommit ---------------------
EXTENDS Integers, Sequences, TLC
CONSTANT Null

(* --algorithm CoordinatorWithGroupCommit 
variables
    all_tx_child_ids = {"tx1", "tx2", "tx3"},
    normal_group_key = "p0",
    normal_group_slots = {[tx_child_id |-> tx_child_id, state |-> "not_ready"] : tx_child_id \in all_tx_child_ids},
    delayed_groups = {},
    coordinator_table = {},
    response_to_clients = {}
    ;

define
    TransactionIsValid(tx) ==
        tx["tx_child_id"] \in all_tx_child_ids /\ tx["state"] \in {"ready", "not_ready"}

    TransactionsInNormalGroupAreValid ==
        \A tx \in normal_group_slots : TransactionIsValid(tx)

    TransactionsInDelayedGroupsAreValid ==
        \A tx \in delayed_groups : TransactionIsValid(tx)

    AllTransactionsAreValid == TransactionsInNormalGroupAreValid /\ TransactionsInDelayedGroupsAreValid

    ResponseToClientIsValid(client) ==
        client["tx_child_id"] \in all_tx_child_ids /\ client["state"] \in {"committed", "aborted"}

    AllResponseToClientsAreValid == \A client \in response_to_clients : ResponseToClientIsValid(client)

    CoordinatorStateIsValid(coord_state) ==
        (coord_state["tx_id"] = <<normal_group_key>> /\ coord_state["state"] = "committed" /\ coord_state["tx_child_ids"] \ all_tx_child_ids = {})
        \/ (coord_state["tx_id"] = <<normal_group_key>> /\ coord_state["state"] = "aborted" /\ coord_state["tx_child_ids"] = {})
        \/ (coord_state["tx_id"] = <<normal_group_key, "tx1">> /\ coord_state["state"] \in {"committed", "aborted"} /\ coord_state["tx_child_ids"] = {})
        \/ (coord_state["tx_id"] = <<normal_group_key, "tx2">> /\ coord_state["state"] \in {"committed", "aborted"} /\ coord_state["tx_child_ids"] = {})
        \/ (coord_state["tx_id"] = <<normal_group_key, "tx3">> /\ coord_state["state"] \in {"committed", "aborted"} /\ coord_state["tx_child_ids"] = {})

    AllCoordinatorRecordsAreValid == \A coord_state \in coordinator_table : CoordinatorStateIsValid(coord_state)

    CoordinatorStateIsSameAs(tx_child_id, expected_state) ==
        (\E coord_state \in coordinator_table : coord_state["tx_id"] = <<normal_group_key>>
        /\ tx_child_id \in coord_state["tx_child_ids"]
        /\ coord_state["state"] = expected_state)
        \/
        (\E coord_state \in coordinator_table : coord_state["tx_id"] = <<normal_group_key, tx_child_id>>
        /\ coord_state["tx_child_ids"] = {}
        /\ coord_state["state"] = expected_state)

    StateInCoordinatorState(tx_child_id) ==
        IF \E coord_state \in coordinator_table : coord_state["tx_id"] = <<normal_group_key, tx_child_id>> /\ coord_state["tx_child_ids"] = {} THEN
            LET
                coord_state == CHOOSE coord_state \in coordinator_table : coord_state["tx_id"] = <<normal_group_key, tx_child_id>> /\ coord_state["tx_child_ids"] = {}
            IN
                coord_state["state"]
        ELSE IF \E coord_state \in coordinator_table : coord_state["tx_id"] = <<normal_group_key>> /\ tx_child_id \in coord_state["tx_child_ids"] THEN
            LET
                coord_state == CHOOSE coord_state \in coordinator_table : coord_state["tx_id"] = <<normal_group_key>> /\ tx_child_id \in coord_state["tx_child_ids"]
            IN
                coord_state["state"]
        ELSE
            Null

    CoordinatorStateShouldContainAnyRecord(tx_child_id) ==
        (\E coord_state \in coordinator_table : coord_state["tx_id"] = <<normal_group_key>> /\ tx_child_id \in coord_state["tx_child_ids"])
        \/ \E coord_state \in coordinator_table : coord_state["tx_id"] = <<normal_group_key, tx_child_id>>

    CoordinatorStateShouldNotContainMultipleRecords(tx_child_id) ==
        ~(
            (\E coord_state \in coordinator_table : coord_state["tx_id"] = <<normal_group_key>> /\ tx_child_id \in coord_state["tx_child_ids"])
            /\ \E coord_state \in coordinator_table : coord_state["tx_id"] = <<normal_group_key, tx_child_id>>
        )

    StateInResponseToClient(tx_child_id) ==
        IF \E client \in response_to_clients : client["tx_child_id"] = tx_child_id THEN
            LET 
                client == CHOOSE client \in response_to_clients : client["tx_child_id"] = tx_child_id
            IN
                client["state"]
        ELSE
            Null

    CoordinatorStateAndResponseToClient(tx_child_id) ==
        LET
            state_in_coordinator_state == StateInCoordinatorState(tx_child_id)
            state_in_response_to_client == StateInResponseToClient(tx_child_id)
        IN
            IF state_in_coordinator_state = Null \/ state_in_response_to_client = Null THEN
                \* This can happen. (e.g., failure of sending response to the client / failure of inserting a coordinator state)
                TRUE
            ELSE IF state_in_coordinator_state = state_in_response_to_client THEN
                TRUE
            ELSE
                FALSE

    Invariant == AllTransactionsAreValid /\ AllResponseToClientsAreValid /\ AllCoordinatorRecordsAreValid
                    /\ \A tx_child_id \in all_tx_child_ids : CoordinatorStateAndResponseToClient(tx_child_id)

    ExpectedState ==
        normal_group_slots = {}
        /\ delayed_groups = {}
        /\ (\A tx_child_id \in {"tx1", "tx2", "tx3"} :
                CoordinatorStateShouldContainAnyRecord(tx_child_id)
                /\ CoordinatorStateShouldNotContainMultipleRecords(tx_child_id))

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
                    \* The existing coordinator state must be created by lazy-recovery.
                    assert ~(\E ctx \in coordinator_table : ctx["tx_id"] = <<normal_group_key>> /\ ctx["tx_child_ids"] /= {});
                    assert ~(\E ctx \in coordinator_table : ctx["tx_id"] = <<normal_group_key>> /\ ctx["state"] /= "aborted");
                    \* The client must not have received any response.
                    assert ~({tx["tx_child_id"] : tx \in normal_group_slots} \subseteq {client["tx_child_id"] : client \in response_to_clients});

                    response_to_clients := response_to_clients \union {
                        [tx_child_id |-> tx["tx_child_id"], state |-> "aborted"] : tx \in normal_group_slots
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
                            [tx_child_id |-> tx["tx_child_id"], state |-> "committed"] : tx \in normal_group_slots
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
                    assert ~({tmp_tx["tx_child_id"]} \subseteq {client["tx_child_id"] : client \in response_to_clients});

                    response_to_clients := response_to_clients \union {
                        [tx_child_id |-> tmp_tx["tx_child_id"], state |-> "aborted"]
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
                            [tx_child_id |-> tmp_tx["tx_child_id"], state |-> "committed"]
                        };
                    or
                        skip;
                    end either;

                    delayed_groups := delayed_groups \ {tmp_tx};
                end if;
            end if;
        or
            \* Emulate a crash.
            normal_group_slots := {};
            delayed_groups := {};
        end either;
    end while;
end process;

fair process LazyRecovery = "lazy-recovery"
variable
    tx_child_ids_in_prepared_records = all_tx_child_ids,
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
            elsif \E ctx \in coordinator_table : ctx["tx_id"] = <<normal_group_key>> /\ ctx["state"] = "committed" then
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
\* BEGIN TRANSLATION (chksum(pcal) = "eee4969c" /\ chksum(tla) = "5170139a")
\* Label Client of process Client at line 111 col 5 changed to Client_
\* Label LazyRecovery of process LazyRecovery at line 219 col 5 changed to LazyRecovery_
VARIABLES all_tx_child_ids, normal_group_key, normal_group_slots, 
          delayed_groups, coordinator_table, response_to_clients, pc

(* define statement *)
TransactionIsValid(tx) ==
    tx["tx_child_id"] \in all_tx_child_ids /\ tx["state"] \in {"ready", "not_ready"}

TransactionsInNormalGroupAreValid ==
    \A tx \in normal_group_slots : TransactionIsValid(tx)

TransactionsInDelayedGroupsAreValid ==
    \A tx \in delayed_groups : TransactionIsValid(tx)

AllTransactionsAreValid == TransactionsInNormalGroupAreValid /\ TransactionsInDelayedGroupsAreValid

ResponseToClientIsValid(client) ==
    client["tx_child_id"] \in all_tx_child_ids /\ client["state"] \in {"committed", "aborted"}

AllResponseToClientsAreValid == \A client \in response_to_clients : ResponseToClientIsValid(client)

CoordinatorStateIsValid(coord_state) ==
    (coord_state["tx_id"] = <<normal_group_key>> /\ coord_state["state"] = "committed" /\ coord_state["tx_child_ids"] \ all_tx_child_ids = {})
    \/ (coord_state["tx_id"] = <<normal_group_key>> /\ coord_state["state"] = "aborted" /\ coord_state["tx_child_ids"] = {})
    \/ (coord_state["tx_id"] = <<normal_group_key, "tx1">> /\ coord_state["state"] \in {"committed", "aborted"} /\ coord_state["tx_child_ids"] = {})
    \/ (coord_state["tx_id"] = <<normal_group_key, "tx2">> /\ coord_state["state"] \in {"committed", "aborted"} /\ coord_state["tx_child_ids"] = {})
    \/ (coord_state["tx_id"] = <<normal_group_key, "tx3">> /\ coord_state["state"] \in {"committed", "aborted"} /\ coord_state["tx_child_ids"] = {})

AllCoordinatorRecordsAreValid == \A coord_state \in coordinator_table : CoordinatorStateIsValid(coord_state)

CoordinatorStateIsSameAs(tx_child_id, expected_state) ==
    (\E coord_state \in coordinator_table : coord_state["tx_id"] = <<normal_group_key>>
    /\ tx_child_id \in coord_state["tx_child_ids"]
    /\ coord_state["state"] = expected_state)
    \/
    (\E coord_state \in coordinator_table : coord_state["tx_id"] = <<normal_group_key, tx_child_id>>
    /\ coord_state["tx_child_ids"] = {}
    /\ coord_state["state"] = expected_state)

StateInCoordinatorState(tx_child_id) ==
    IF \E coord_state \in coordinator_table : coord_state["tx_id"] = <<normal_group_key, tx_child_id>> /\ coord_state["tx_child_ids"] = {} THEN
        LET
            coord_state == CHOOSE coord_state \in coordinator_table : coord_state["tx_id"] = <<normal_group_key, tx_child_id>> /\ coord_state["tx_child_ids"] = {}
        IN
            coord_state["state"]
    ELSE IF \E coord_state \in coordinator_table : coord_state["tx_id"] = <<normal_group_key>> /\ tx_child_id \in coord_state["tx_child_ids"] THEN
        LET
            coord_state == CHOOSE coord_state \in coordinator_table : coord_state["tx_id"] = <<normal_group_key>> /\ tx_child_id \in coord_state["tx_child_ids"]
        IN
            coord_state["state"]
    ELSE
        Null

CoordinatorStateShouldContainAnyRecord(tx_child_id) ==
    (\E coord_state \in coordinator_table : coord_state["tx_id"] = <<normal_group_key>> /\ tx_child_id \in coord_state["tx_child_ids"])
    \/ \E coord_state \in coordinator_table : coord_state["tx_id"] = <<normal_group_key, tx_child_id>>

CoordinatorStateShouldNotContainMultipleRecords(tx_child_id) ==
    ~(
        (\E coord_state \in coordinator_table : coord_state["tx_id"] = <<normal_group_key>> /\ tx_child_id \in coord_state["tx_child_ids"])
        /\ \E coord_state \in coordinator_table : coord_state["tx_id"] = <<normal_group_key, tx_child_id>>
    )

StateInResponseToClient(tx_child_id) ==
    IF \E client \in response_to_clients : client["tx_child_id"] = tx_child_id THEN
        LET
            client == CHOOSE client \in response_to_clients : client["tx_child_id"] = tx_child_id
        IN
            client["state"]
    ELSE
        Null

CoordinatorStateAndResponseToClient(tx_child_id) ==
    LET
        state_in_coordinator_state == StateInCoordinatorState(tx_child_id)
        state_in_response_to_client == StateInResponseToClient(tx_child_id)
    IN
        IF state_in_coordinator_state = Null \/ state_in_response_to_client = Null THEN

            TRUE
        ELSE IF state_in_coordinator_state = state_in_response_to_client THEN
            TRUE
        ELSE
            FALSE

Invariant == AllTransactionsAreValid /\ AllResponseToClientsAreValid /\ AllCoordinatorRecordsAreValid
                /\ \A tx_child_id \in all_tx_child_ids : CoordinatorStateAndResponseToClient(tx_child_id)

ExpectedState ==
    normal_group_slots = {}
    /\ delayed_groups = {}
    /\ (\A tx_child_id \in {"tx1", "tx2", "tx3"} :
            CoordinatorStateShouldContainAnyRecord(tx_child_id)
            /\ CoordinatorStateShouldNotContainMultipleRecords(tx_child_id))

EventualExpectedState == <>[]ExpectedState

VARIABLES tmp_tx, tx_child_ids_in_prepared_records, tmp_tx_child_id, 
          already_committed

vars == << all_tx_child_ids, normal_group_key, normal_group_slots, 
           delayed_groups, coordinator_table, response_to_clients, pc, tmp_tx, 
           tx_child_ids_in_prepared_records, tmp_tx_child_id, 
           already_committed >>

ProcSet == {"client"} \cup {"group-committer"} \cup {"lazy-recovery"}

Init == (* Global variables *)
        /\ all_tx_child_ids = {"tx1", "tx2", "tx3"}
        /\ normal_group_key = "p0"
        /\ normal_group_slots = {[tx_child_id |-> tx_child_id, state |-> "not_ready"] : tx_child_id \in all_tx_child_ids}
        /\ delayed_groups = {}
        /\ coordinator_table = {}
        /\ response_to_clients = {}
        (* Process GroupCommitter *)
        /\ tmp_tx = Null
        (* Process LazyRecovery *)
        /\ tx_child_ids_in_prepared_records = all_tx_child_ids
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
                      /\ pc' = [pc EXCEPT !["client"] = "Client_"]
                 ELSE /\ pc' = [pc EXCEPT !["client"] = "Done"]
                      /\ UNCHANGED << normal_group_slots, delayed_groups >>
           /\ UNCHANGED << all_tx_child_ids, normal_group_key, 
                           coordinator_table, response_to_clients, tmp_tx, 
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
                                                                "Failure of assertion at line 143, column 21.")
                                                      /\ Assert(~(\E ctx \in coordinator_table : ctx["tx_id"] = <<normal_group_key>> /\ ctx["state"] /= "aborted"), 
                                                                "Failure of assertion at line 144, column 21.")
                                                      /\ Assert(~({tx["tx_child_id"] : tx \in normal_group_slots} \subseteq {client["tx_child_id"] : client \in response_to_clients}), 
                                                                "Failure of assertion at line 146, column 21.")
                                                      /\ response_to_clients' = (                       response_to_clients \union {
                                                                                     [tx_child_id |-> tx["tx_child_id"], state |-> "aborted"] : tx \in normal_group_slots
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
                                                 THEN /\ Assert(~({tmp_tx'["tx_child_id"]} \subseteq {client["tx_child_id"] : client \in response_to_clients}), 
                                                                "Failure of assertion at line 179, column 21.")
                                                      /\ response_to_clients' = (                       response_to_clients \union {
                                                                                     [tx_child_id |-> tmp_tx'["tx_child_id"], state |-> "aborted"]
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
                             \/ /\ normal_group_slots' = {}
                                /\ delayed_groups' = {}
                                /\ pc' = [pc EXCEPT !["group-committer"] = "GroupCommit"]
                                /\ UNCHANGED <<coordinator_table, response_to_clients, tmp_tx>>
                     ELSE /\ pc' = [pc EXCEPT !["group-committer"] = "Done"]
                          /\ UNCHANGED << normal_group_slots, delayed_groups, 
                                          coordinator_table, 
                                          response_to_clients, tmp_tx >>
               /\ UNCHANGED << all_tx_child_ids, normal_group_key, 
                               tx_child_ids_in_prepared_records, 
                               tmp_tx_child_id, already_committed >>

SendCommitToClientForNormalGroupCommit == /\ pc["group-committer"] = "SendCommitToClientForNormalGroupCommit"
                                          /\ \/ /\ response_to_clients' = (                       response_to_clients \union {
                                                                               [tx_child_id |-> tx["tx_child_id"], state |-> "committed"] : tx \in normal_group_slots
                                                                           })
                                             \/ /\ TRUE
                                                /\ UNCHANGED response_to_clients
                                          /\ normal_group_slots' = {}
                                          /\ pc' = [pc EXCEPT !["group-committer"] = "GroupCommit"]
                                          /\ UNCHANGED << all_tx_child_ids, 
                                                          normal_group_key, 
                                                          delayed_groups, 
                                                          coordinator_table, 
                                                          tmp_tx, 
                                                          tx_child_ids_in_prepared_records, 
                                                          tmp_tx_child_id, 
                                                          already_committed >>

SendCommitToClientForDelayedGroupCommit == /\ pc["group-committer"] = "SendCommitToClientForDelayedGroupCommit"
                                           /\ \/ /\ response_to_clients' = (                       response_to_clients \union {
                                                                                [tx_child_id |-> tmp_tx["tx_child_id"], state |-> "committed"]
                                                                            })
                                              \/ /\ TRUE
                                                 /\ UNCHANGED response_to_clients
                                           /\ delayed_groups' = delayed_groups \ {tmp_tx}
                                           /\ pc' = [pc EXCEPT !["group-committer"] = "GroupCommit"]
                                           /\ UNCHANGED << all_tx_child_ids, 
                                                           normal_group_key, 
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
                                             ELSE /\ IF \E ctx \in coordinator_table : ctx["tx_id"] = <<normal_group_key>> /\ ctx["state"] = "committed"
                                                        THEN /\ already_committed' = FALSE
                                                        ELSE /\ IF \E ctx \in coordinator_table : ctx["tx_id"] = <<normal_group_key>> /\ ctx["state"] = "aborted" /\ ctx["tx_child_ids"] = {}
                                                                   THEN /\ already_committed' = FALSE
                                                                   ELSE /\ Assert(FALSE, 
                                                                                  "Failure of assertion at line 236, column 17.")
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
                 /\ UNCHANGED << all_tx_child_ids, normal_group_key, 
                                 normal_group_slots, delayed_groups, 
                                 response_to_clients, tmp_tx, 
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
                   /\ UNCHANGED << all_tx_child_ids, normal_group_key, 
                                   normal_group_slots, delayed_groups, 
                                   response_to_clients, tmp_tx, 
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
\* Last modified Tue Jul 30 11:25:19 JST 2024 by mitsunorikomatsu
\* Created Thu Jul 25 17:04:35 JST 2024 by mitsunorikomatsu
