---- MODULE MC ----
EXTENDS CoordinatorWithGroupCommit, TLC

\* MV CONSTANT declarations@modelParameterConstants
CONSTANTS
tx1, tx2, tx3
----

\* MV CONSTANT definitions AllTxChildIds
const_1722309678035200000 == 
{tx1, tx2, tx3}
----

\* SYMMETRY definition
symm_1722309678035201000 == 
Permutations(const_1722309678035200000)
----

=============================================================================
\* Modification History
\* Created Tue Jul 30 12:21:18 JST 2024 by mitsunorikomatsu
