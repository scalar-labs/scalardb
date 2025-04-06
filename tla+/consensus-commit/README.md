## TLA+ verification of the Consensus Commit protocol in ScalarDB

## Overview of the protocol

A quick overview of the protocol. Note: to use Consensus Commit will require a schema change to maintain metadata (txId, old data, record state) and also a coordinator table.

The general idea is as follows:
1. There is a write set W and a coordinator C. We say that all writes and also the coordinator start out in an "initialized" state.
2. Create a new transaction id (uuid) and mark every write in W with it and also as "prepared"
3. (Prepare phase) CAS every (prepared) write and store the old value.
4. If every CAS operation succeeds that we say the transaction is "prepared". If any fails, abort the transaction, and rollback.
5. (Commit phase) CAS the transaction id to the coordinator table. (Commit phase)
6. If this succeeds then the transaction is committed; in this case we also say the coordinator is "committed". If this fails then the transaction fails, rollback.
7. Update all the writes in W to be marked "committed". Can be done lazily.

Failure handling
- If the client fails before prepare phase (3), simply clear the tx out of memory.
- If the client fails after the prepare phase (3) and before the commit phase (5) (No status in the coordinator table). Then a next transaction will notice prepared records, but not status, so it will abort the previous transaction and roll it back.
- If the client fails after the commit phase (5), then the next transaction will commit the records (7) (rollforward) on behalf of the transaction.

## TLA+ protocol specification

The specification of the protocol may be found in `CCSpec.tla`. It is specified as a state machine. The specific start state is designated by the `Init` formula. Then the valid transitions that the state machine can make are describe by the `Next` formula. For example the transition
```
rCommit(r) == /\ rState[r] = "prepared"
              /\ cState = "committed"
              /\ rState' = [rState EXCEPT ![r] = "committed"]
              /\ cState' = cState
```
says that if a resource `r` is in the "prepared" state and the coordinator is in the "committed" state, then `r` can move into a "committed" state while the coordinator state does not change. The last line is absolutely neccessary as each state transition must describe the state of the "world".

If the protocol is correct, then the the formula `Consistent` should be true at every state. This formula consists of a conjunction of two conditions:
1. If the coordinator is "committed" then no resource should be in the "initialized" or "aborted" state.
2. If any resource is "committed" then the coordinator should not be "aborted".

`Spec == Init /\ [][Next]_<<rState, cState>>` is a temporal formula asserting that all behaviours must initial satisfy `Init` and have all next steps be `Next`.

`THEOREM Spec => [](TypeOK /\ Consistent)` asserts that behaviour satisfying `Spec` will always satisfy `TypeOK` and `Consistent`.

## TLA+ implementation specification

The implementation of the protocol may be found in `CC.tla`. `CCSpec.tla` specifies the protocol itself, whereas `CC.tla` specifies an implementation of the protocol.

The implementation is done in message passing style. All messages are sent on the `msg` channel and all participants can send and recieve messages on that channel.

## How to run the model checker

### CCSpec

1. Open the spec and click on "TLC Model Checker -> New Model".
2. In "What is the behavior spec?" select "Temporal formula" and enter "Spec".
3. In "What is the model?" enter "R <- r1, r2, r3" and select "Set of model values" and also "Symmetry set". This means that r1, r2, and r3 are interchangable and will make model checking much faster.
4. Make sure "Deadlock" is _not_ selected.
5. Under "Invariants" you can enter "TypeOK".
6. Under "Properties" enter "Spec".
7. Now you should be able to run the model checker. Push the little green arrow at the top.
8. The number in "States Found" and "Distinct States" should give you a good idea if the model checker is working correctly or not.

### CC

1. Open the spec and click on "TLC Model Checker -> New Model".
2. In "What is the behavior spec?" select "Temporal formula" and enter "Spec".
3. In "What is the model?" enter "R <- r1, r2, r3" and select "Set of model values" and also "Symmetry set". This means that r1, r2, and r3 are interchangable and will make model checking much faster.
4. Make sure "Deadlock" is _not_ selected.
5. Under "Invariants" you can enter "TypeOK", "CCS!TypeOK" and "CCS!Consistent".
6. Under "Properties" enter "Spec" and "CCS!Spec".
7. Now you should be able to run the model checker. Push the little green arrow at the top.
8. The number in "States Found" and "Distinct States" should give you a good idea if the model checker is working correctly or not.
