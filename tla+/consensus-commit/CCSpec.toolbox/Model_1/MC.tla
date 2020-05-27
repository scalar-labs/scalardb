---- MODULE MC ----
EXTENDS CCSpec, TLC

\* MV CONSTANT declarations@modelParameterConstants
CONSTANTS
r1, r2, r3
----

\* MV CONSTANT definitions R
const_157432426674921000 == 
{r1, r2, r3}
----

\* SYMMETRY definition
symm_157432426674922000 == 
Permutations(const_157432426674921000)
----

