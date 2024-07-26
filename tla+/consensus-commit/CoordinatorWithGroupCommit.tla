--------------------- MODULE CoordinatorWithGroupCommit ---------------------
EXTENDS Integers, Sequences, TLC
CONSTANT Null

(* --algorithm CoordinatorWithGroupCommit 
variables
  normal_group_key = "p0",
  normal_group_slots = {
    [ client_id |-> "c1", tx_child_id |-> "tx1", state |-> "not_ready" ],
    [ client_id |-> "c2", tx_child_id |-> "tx2", state |-> "not_ready" ],
    [ client_id |-> "c3", tx_child_id |-> "tx3", state |-> "not_ready" ]
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
    \/ (record.tx_id = <<normal_group_key, "tx1">> /\ record.state \in {"committed", "abortted"} /\ record.tx_child_ids = {})
    \/ (record.tx_id = <<normal_group_key, "tx2">> /\ record.state \in {"committed", "abortted"} /\ record.tx_child_ids = {})
    \/ (record.tx_id = <<normal_group_key, "tx3">> /\ record.state \in {"committed", "abortted"} /\ record.tx_child_ids = {})

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

  StateInResponseToClient(tx_child_id) ==
    LET 
      client == CHOOSE client \in response_to_clients : client["tx_child_id"] = tx_child_id
    IN
      client["state"]

  ExpectedState == <>[]
    (normal_group_slots = {}
      /\ delayed_groups = {}
      /\ {client["client_id"] : client \in response_to_clients} = {"c1", "c2", "c3"}
      /\ \A tx_child_id \in {"tx1", "tx2", "tx3"} : CoordinatorStateIsSameAs(tx_child_id, StateInResponseToClient(tx_child_id))
    )

end define;

fair process Client = "client"
begin Client:
  while normal_group_slots /= {} \/ delayed_groups /= {} do
    (* Make transactions ready *)
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
begin GroupCommit:
  while normal_group_slots /= {} \/ delayed_groups /= {} do
    either
      (* Move delayed transactions to the delayed groups *)
      with tx \in normal_group_slots do
        if tx["state"] = "not_ready" then
          delayed_groups := delayed_groups \union { tx };
          normal_group_slots := normal_group_slots \ { tx };
        end if;
      end with;
    or
      (* Commit ready transactions in the normal group *)
      if normal_group_slots /= {} /\ \A tx \in normal_group_slots: tx["state"] = "ready" then
        if \E ctx \in coordinator_table : ctx["tx_id"] = <<normal_group_key>> then
          (* The existing record must be created by lazy-recovery. *)
          assert ~(\E ctx \in coordinator_table : ctx["tx_id"] = <<normal_group_key>> \/ ctx["tx_child_ids"] /= {});
          assert ~(\E ctx \in coordinator_table : ctx["tx_id"] = <<normal_group_key>> \/ ctx["state"] /= "aborted");
          assert ~({ tx["client_id"] : tx \in normal_group_slots } \subseteq { client["client_id"] : client \in response_to_clients });
          response_to_clients := response_to_clients \union {
            [ client_id |-> tx["client_id"], tx_child_id |-> tx["tx_child_id"], state |-> "aborted" ] : tx \in normal_group_slots
          };
        else
          coordinator_table := coordinator_table \union {[
            tx_id |-> <<normal_group_key>>,
            tx_child_ids |-> { tx["tx_child_id"] : tx \in normal_group_slots },
            state |-> "committed"
          ]};
          response_to_clients := response_to_clients \union {
            [ client_id |-> tx["client_id"], tx_child_id |-> tx["tx_child_id"], state |-> "committed" ] : tx \in normal_group_slots
          };
        end if;
        normal_group_slots := {};
      end if;
    or
      (* Commit ready transactions in the delayed groups *)
      with tx \in delayed_groups do
        if tx["state"] = "ready" then
          if \E ctx \in coordinator_table : ctx["tx_id"] = <<normal_group_key, tx["tx_child_id"]>> then
            (* The existing record must be created by lazy-recovery. *)
            assert ~(\E ctx \in coordinator_table : ctx["tx_id"] = <<normal_group_key>> \/ ctx["tx_child_ids"] /= {});
            assert ~(\E ctx \in coordinator_table : ctx["tx_id"] = <<normal_group_key>> \/ ctx["state"] /= "aborted");
            assert ~({ tx["client_id"] } \subseteq { client["client_id"] : client \in response_to_clients });
            response_to_clients := response_to_clients \union {
              [ client_id |-> tx["client_id"], tx_child_id |-> tx["tx_child_id"], state |-> "aborted" ]
            };
          else
            coordinator_table := coordinator_table \union {[
                tx_id |-> <<normal_group_key, tx["tx_child_id"]>>,
                tx_child_ids |-> {},
                state |-> "committed"
            ]};
            response_to_clients := response_to_clients \union {
              [ client_id |-> tx["client_id"], tx_child_id |-> tx["tx_child_id"], state |-> "committed" ]
            };
          end if;
          delayed_groups := delayed_groups \ { tx };
        end if;
      end with;
    or
      skip;
    end either;
  end while;
end process;

(*
fair process LazyRecovery = "lazy-recovery"
begin Client:
  while normal_group_slots /= {} \/ delayed_groups /= {} do
    ( Make transactions abort )
    either
      with tx \in normal_group_slots do
        if \E ctx \in coordinator_table : ctx["tx_id"] = <<normal_group_key>> then
          \* Failed to abort.
        end if;

        if \E ctx \in coordinator_table : ctx["tx_id"] = <<normal_group_key, tx["tx_child_id"]>> then
        end if;
        normal_group_slots := normal_group_slots \ { tx };
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
*)

end algorithm *)
\* BEGIN TRANSLATION (chksum(pcal) = "b91210c8" /\ chksum(tla) = "f06cd42")
\* Label Client of process Client at line 77 col 3 changed to Client_
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
  \/ (record.tx_id = <<normal_group_key, "tx1">> /\ record.state \in {"committed", "abortted"} /\ record.tx_child_ids = {})
  \/ (record.tx_id = <<normal_group_key, "tx2">> /\ record.state \in {"committed", "abortted"} /\ record.tx_child_ids = {})
  \/ (record.tx_id = <<normal_group_key, "tx3">> /\ record.state \in {"committed", "abortted"} /\ record.tx_child_ids = {})

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

StateInResponseToClient(tx_child_id) ==
  LET
    client == CHOOSE client \in response_to_clients : client["tx_child_id"] = tx_child_id
  IN
    client["state"]

ExpectedState == <>[]
  (normal_group_slots = {}
    /\ delayed_groups = {}
    /\ {client["client_id"] : client \in response_to_clients} = {"c1", "c2", "c3"}
    /\ \A tx_child_id \in {"tx1", "tx2", "tx3"} : CoordinatorStateIsSameAs(tx_child_id, StateInResponseToClient(tx_child_id))
  )


vars == << normal_group_key, normal_group_slots, delayed_groups, 
           coordinator_table, response_to_clients, pc >>

ProcSet == {"client"} \cup {"group-committer"}

Init == (* Global variables *)
        /\ normal_group_key = "p0"
        /\ normal_group_slots =                      {
                                  [ client_id |-> "c1", tx_child_id |-> "tx1", state |-> "not_ready" ],
                                  [ client_id |-> "c2", tx_child_id |-> "tx2", state |-> "not_ready" ],
                                  [ client_id |-> "c3", tx_child_id |-> "tx3", state |-> "not_ready" ]
                                }
        /\ delayed_groups = {}
        /\ coordinator_table = {}
        /\ response_to_clients = {}
        /\ pc = [self \in ProcSet |-> CASE self = "client" -> "Client_"
                                        [] self = "group-committer" -> "GroupCommit"]

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
                           response_to_clients >>

Client == Client_

GroupCommit == /\ pc["group-committer"] = "GroupCommit"
               /\ IF normal_group_slots /= {} \/ delayed_groups /= {}
                     THEN /\ \/ /\ \E tx \in normal_group_slots:
                                     IF tx["state"] = "not_ready"
                                        THEN /\ delayed_groups' = (delayed_groups \union { tx })
                                             /\ normal_group_slots' = normal_group_slots \ { tx }
                                        ELSE /\ TRUE
                                             /\ UNCHANGED << normal_group_slots, 
                                                             delayed_groups >>
                                /\ UNCHANGED <<coordinator_table, response_to_clients>>
                             \/ /\ IF normal_group_slots /= {} /\ \A tx \in normal_group_slots: tx["state"] = "ready"
                                      THEN /\ IF \E ctx \in coordinator_table : ctx["tx_id"] = <<normal_group_key>>
                                                 THEN /\ Assert(~(\E ctx \in coordinator_table : ctx["tx_id"] = <<normal_group_key>> \/ ctx["tx_child_ids"] /= {}), 
                                                                "Failure of assertion at line 109, column 11.")
                                                      /\ Assert(~(\E ctx \in coordinator_table : ctx["tx_id"] = <<normal_group_key>> \/ ctx["state"] /= "aborted"), 
                                                                "Failure of assertion at line 110, column 11.")
                                                      /\ Assert(~({ tx["client_id"] : tx \in normal_group_slots } \subseteq { client["client_id"] : client \in response_to_clients }), 
                                                                "Failure of assertion at line 111, column 11.")
                                                      /\ response_to_clients' = (                       response_to_clients \union {
                                                                                   [ client_id |-> tx["client_id"], tx_child_id |-> tx["tx_child_id"], state |-> "aborted" ] : tx \in normal_group_slots
                                                                                 })
                                                      /\ UNCHANGED coordinator_table
                                                 ELSE /\ coordinator_table' = (                     coordinator_table \union {[
                                                                                 tx_id |-> <<normal_group_key>>,
                                                                                 tx_child_ids |-> { tx["tx_child_id"] : tx \in normal_group_slots },
                                                                                 state |-> "committed"
                                                                               ]})
                                                      /\ response_to_clients' = (                       response_to_clients \union {
                                                                                   [ client_id |-> tx["client_id"], tx_child_id |-> tx["tx_child_id"], state |-> "committed" ] : tx \in normal_group_slots
                                                                                 })
                                           /\ normal_group_slots' = {}
                                      ELSE /\ TRUE
                                           /\ UNCHANGED << normal_group_slots, 
                                                           coordinator_table, 
                                                           response_to_clients >>
                                /\ UNCHANGED delayed_groups
                             \/ /\ \E tx \in delayed_groups:
                                     IF tx["state"] = "ready"
                                        THEN /\ IF \E ctx \in coordinator_table : ctx["tx_id"] = <<normal_group_key, tx["tx_child_id"]>>
                                                   THEN /\ Assert(~(\E ctx \in coordinator_table : ctx["tx_id"] = <<normal_group_key>> \/ ctx["tx_child_ids"] /= {}), 
                                                                  "Failure of assertion at line 133, column 13.")
                                                        /\ Assert(~(\E ctx \in coordinator_table : ctx["tx_id"] = <<normal_group_key>> \/ ctx["state"] /= "aborted"), 
                                                                  "Failure of assertion at line 134, column 13.")
                                                        /\ Assert(~({ tx["client_id"] } \subseteq { client["client_id"] : client \in response_to_clients }), 
                                                                  "Failure of assertion at line 135, column 13.")
                                                        /\ response_to_clients' = (                       response_to_clients \union {
                                                                                     [ client_id |-> tx["client_id"], tx_child_id |-> tx["tx_child_id"], state |-> "aborted" ]
                                                                                   })
                                                        /\ UNCHANGED coordinator_table
                                                   ELSE /\ coordinator_table' = (                     coordinator_table \union {[
                                                                                     tx_id |-> <<normal_group_key, tx["tx_child_id"]>>,
                                                                                     tx_child_ids |-> {},
                                                                                     state |-> "committed"
                                                                                 ]})
                                                        /\ response_to_clients' = (                       response_to_clients \union {
                                                                                     [ client_id |-> tx["client_id"], tx_child_id |-> tx["tx_child_id"], state |-> "committed" ]
                                                                                   })
                                             /\ delayed_groups' = delayed_groups \ { tx }
                                        ELSE /\ TRUE
                                             /\ UNCHANGED << delayed_groups, 
                                                             coordinator_table, 
                                                             response_to_clients >>
                                /\ UNCHANGED normal_group_slots
                             \/ /\ TRUE
                                /\ UNCHANGED <<normal_group_slots, delayed_groups, coordinator_table, response_to_clients>>
                          /\ pc' = [pc EXCEPT !["group-committer"] = "GroupCommit"]
                     ELSE /\ pc' = [pc EXCEPT !["group-committer"] = "Done"]
                          /\ UNCHANGED << normal_group_slots, delayed_groups, 
                                          coordinator_table, 
                                          response_to_clients >>
               /\ UNCHANGED normal_group_key

GroupCommitter == GroupCommit

(* Allow infinite stuttering to prevent deadlock on termination. *)
Terminating == /\ \A self \in ProcSet: pc[self] = "Done"
               /\ UNCHANGED vars

Next == Client \/ GroupCommitter
           \/ Terminating

Spec == /\ Init /\ [][Next]_vars
        /\ WF_vars(Client)
        /\ WF_vars(GroupCommitter)

Termination == <>(\A self \in ProcSet: pc[self] = "Done")

\* END TRANSLATION 

=============================================================================
\* Modification History
\* Last modified Fri Jul 26 21:53:41 JST 2024 by mitsunorikomatsu
\* Created Thu Jul 25 17:04:35 JST 2024 by mitsunorikomatsu
