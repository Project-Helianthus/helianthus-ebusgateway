## Leave a pending operator identity link pending when a child is withdrawn

A withdrawal of a child in the logger-attached PV family carries no pending operator identity link for a binding it does not observe. The declaration stays pending, the child's withdrawn records still commit, and the link of the replacement binding keeps naming the replacement inverter. A declaration that names a resource the asset does not hold leaves that same withdrawal able to commit. A batch that carries an operator link set other than the pending set is still refused (#1666).
