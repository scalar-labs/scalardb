---- MODULE MC ----
EXTENDS CC, TLC

\* MV CONSTANT declarations@modelParameterConstants
CONSTANTS
r1, r2, r3
----

\* MV CONSTANT definitions R
const_157432435185431000 == 
{r1, r2, r3}
----

\* SYMMETRY definition
symm_157432435185432000 == 
Permutations(const_157432435185431000)
----

\* INVARIANT definition @modelCorrectnessInvariants:0
inv_157432435185433000 ==
CCS!TypeOK
----
\* INVARIANT definition @modelCorrectnessInvariants:1
inv_157432435185434000 ==
CCS!Consistent
----
\* PROPERTY definition @modelCorrectnessProperties:0
prop_157432435185435000 ==
CCS!Spec
----
