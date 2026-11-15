## Remove the retired client-certificate flags of the M2M listener

The gateway no longer defines `-m2m-graphql-client-ca` and `-m2m-graphql-denied-principals`. Both were accepted and ignored since the M2M listener stopped asking for client certificates; a command line that still passes one now fails at start. Server TLS of the listener is unchanged. The gateway binary changes.
