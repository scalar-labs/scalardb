------------------------------ MODULE CCSpec ------------------------------
CONSTANT R          \* The set of participating records
VARIABLES rState,   \* rState[r] is the state of the record r
          cState    \* cState is the state of the coordinator

---------------------------------------------------------------------------
TypeOK == /\ rState \in [R -> {"initialized", "prepared", "committed", "aborted"}]
          /\ cState \in {"initialized", "committed", "aborted"}

Init == /\ rState = [r \in R |-> "initialized"]
        /\ cState = "initialized"

---------------------------------------------------------------------------
rPrepare(r) == /\ rState[r] = "initialized"
               /\ rState' = [rState EXCEPT ![r] = "prepared"]
               /\ cState' = cState

\* A record may abort as long as the coordinator has not committed
rAbort(r) == /\ rState[r] \in {"initialized", "prepared"}
             /\ cState # "committed"
             /\ rState' = [rState EXCEPT ![r] = "aborted"]
             /\ cState' = cState

\* A record may commit only after the coordinator has committed
rCommit(r) == /\ rState[r] = "prepared"
              /\ cState = "committed"
              /\ rState' = [rState EXCEPT ![r] = "committed"]
              /\ cState' = cState
              
\* Coordinator can commit only after all records are prepared
cCommit == /\ \A r \in R : rState[r] = "prepared"
           /\ cState = "initialized"
           /\ cState' = "committed"
           /\ rState' = rState

\* Coordinator can abort if no record has committed
cAbort == /\ \A r \in R : rState[r] # "committed"
          /\ cState = "initialized"
          /\ cState' = "aborted"
          /\ rState' = rState

---------------------------------------------------------------------------
Next == \/ \E r \in R : rPrepare(r) \/ rAbort(r) \/ rCommit(r)
        \/ cCommit
        \/ cAbort

Consistent == ~(\/ \E r \in R : rState[r] \in {"initialized", "aborted"} /\ cState = "committed"
                \/ \E r \in R : rState[r] = "committed" /\ cState = "aborted")
               
Spec == Init /\ [][Next]_<<rState, cState>>

THEOREM Spec => [](TypeOK /\ Consistent)

=============================================================================
