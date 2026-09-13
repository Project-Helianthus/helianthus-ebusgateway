package catalogv1

// This file deliberately keeps admission on the immutable catalog interfaces.
// Gateway adapters must rerun SemReg, binding, and DriverManager checks before
// their Invoker performs any native operation; this package never owns a route.
