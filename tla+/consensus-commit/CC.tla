--------------------------------- MODULE CC ---------------------------------
CONSTANT R  \* The set of records

VARIABLES
  rState,       \* rState[r] is the state of the record r
  cState,       \* The state of the coordinator
  rPrepared,    \* The set of records from which the client has received "Prepared"
  msgs          \* The set of sent messages

vars == <<rState, cState, rPrepared, msgs>>

\* [type |-> "Prepared", from |-> r] and [type |-> "Aborted", from |-> r] are from the records
\* [type |-> "Committed"] and [type |-> "Aborted"] are from the coordinator
\* [type |-> "CoordinatorCommit"], [type |-> "RecordsCommit"] and [type |-> "AllAbort"] are from the client
Messages ==
  [type : {"Prepared", "Aborted"}, from : R] \cup [type : {"Committed", "Aborted", "CoordinatorCommit", "RecordsCommit", "AllAbort"}]

TypeOK ==  
  /\ rState \in [R -> {"initialized", "prepared", "committed", "aborted"}]
  /\ cState \in {"initialized", "committed", "aborted"}
  /\ rPrepared \subseteq R
  /\ msgs \subseteq Messages

Init ==   
  /\ rState = [r \in R |-> "initialized"]
  /\ cState = "initialized"
  /\ rPrepared = {}
  /\ msgs = {}

-----------------------------------------------------------------------------

\* A record tries to prepare; it may end up prepared or aborted
rPrepare(r) ==
  \/ /\ rState[r] = "initialized"
     /\ rState' = [rState EXCEPT ![r] = "prepared"]
     /\ UNCHANGED <<cState, rPrepared, msgs>>
  \/ /\ rState[r] = "initialized"
     /\ rState' = [rState EXCEPT ![r] = "aborted"]
     /\ UNCHANGED <<cState, rPrepared, msgs>>

rSendPrepare(r) == 
  /\ rState[r] = "prepared"
  /\ msgs' = msgs \cup {[type |-> "Prepared", from |-> r]}
  /\ UNCHANGED <<rState, cState, rPrepared>>
 
rSendAbort(r) == 
  /\ rState[r] = "aborted"
  /\ msgs' = msgs \cup {[type |-> "Aborted", from |-> r]}
  /\ UNCHANGED <<rState, cState, rPrepared>>

\* A record can commit if it is in the prepared state and receives a commit message
\* This will never fail
rRcvCommit(r) == 
  /\ rState[r] = "prepared"
  /\ [type |-> "RecordsCommit"] \in msgs
  /\ rState' = [rState EXCEPT ![r] = "committed"]
  /\ UNCHANGED <<cState, rPrepared, msgs>>

rRcvAbort(r) ==
  /\ rState[r] # "committed"
  /\ [type |-> "AllAbort"] \in msgs
  /\ rState' = [rState EXCEPT ![r] = "aborted"]  
  /\ UNCHANGED <<cState, rPrepared, msgs>>
-----------------------------------------------------------------------------

\* Coordinator recieves a commit message and either commits or aborts
cRcvCommit ==
  \/ /\ cState = "initialized"
     /\ [type |-> "CoordinatorCommit"] \in msgs
     /\ cState' = "committed"
     /\ UNCHANGED <<rState, rPrepared, msgs>>
  \/ /\ cState = "initialized"
     /\ [type |-> "CoordinatorCommit"] \in msgs
     /\ cState' = "aborted"
     /\ UNCHANGED <<rState, rPrepared, msgs>>

cSendCommitted ==
  /\ cState = "committed"
  /\ msgs' = msgs \cup {[type |-> "Committed"]}
  /\ UNCHANGED <<rState, cState, rPrepared>>

cSendAborted ==
  /\ cState = "aborted"
  /\ msgs' = msgs \cup {[type |-> "Aborted"]}
  /\ UNCHANGED <<rState, cState, rPrepared>>

cRcvAbort ==
  /\ cState # "committed"
  /\ [type |-> "AllAbort"] \in msgs
  /\ cState' = "aborted"  
  /\ UNCHANGED <<rState, rPrepared, msgs>>
-----------------------------------------------------------------------------

clientRcvPrepared(r) ==
  /\ [type |-> "Prepared", from |-> r] \in msgs
  /\ rPrepared' = rPrepared \cup {r}
  /\ UNCHANGED <<rState, cState, msgs>>

clientSendCoordinatorCommit ==
  /\ rPrepared = R
  /\ msgs' = msgs \cup {[type |-> "CoordinatorCommit"]}
  /\ UNCHANGED <<rState, cState, rPrepared>>

clientRcvCommitted ==  \* from coordinator
  /\ [type |-> "Committed"] \in msgs
  /\ msgs' = msgs \cup {[type |-> "RecordsCommit"]}
  /\ UNCHANGED <<rState, cState, rPrepared>>

clientRcvAbortFromWrite(r) ==
  /\ [type |-> "Aborted", from |-> r] \in msgs
  /\ msgs' = msgs \cup {[type |-> "AllAbort"]}
  /\ UNCHANGED <<rState, cState, rPrepared>>

clientRcvAbortFromCoordinator ==
  /\ [type |-> "Aborted"] \in msgs
  /\ msgs' = msgs \cup {[type |-> "AllAbort"]}
  /\ UNCHANGED <<rState, cState, rPrepared>>

-----------------------------------------------------------------------------

Next ==
  \/ \E r \in R : rPrepare(r) \/ rSendPrepare(r) \/ rSendAbort(r) \/ rRcvCommit(r) \/ rRcvAbort(r)
  \/ cRcvCommit \/ cSendCommitted \/ cSendAborted \/ cRcvAbort
  \/ clientSendCoordinatorCommit \/ clientRcvCommitted \/ clientRcvAbortFromCoordinator \/ \E r \in R : clientRcvPrepared(r) \/ clientRcvAbortFromWrite(r)

Spec == Init /\ [][Next]_vars
THEOREM Spec => []TypeOK

-----------------------------------------------------------------------------

CCS == INSTANCE CCSpec
THEOREM Spec => CCS!Spec
\* Put "CCS!Spec" into the Properties section of the model to check it

=============================================================================
