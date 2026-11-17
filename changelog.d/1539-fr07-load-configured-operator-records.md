## Load configured operator records and accept an identity link

Startup loads the operator-record section the configuration already holds, including an identity link that names the home asset, the native resource and the configuration basis class. An empty section still admits nothing. A configuration-only home is declared for the configuration provider; a home a driver already owns is not given a contributor declaration. The provider still does not publish the identity link. (#1539)
