package graphql

type ServiceStatus struct {
	Status           string
	FirmwareVersion  string
	UpdatesAvailable bool
	InitiatorAddress string
}

type StatusProvider interface {
	DaemonStatus() ServiceStatus
	AdapterStatus() ServiceStatus
}

type staticStatusProvider struct{}

func (staticStatusProvider) DaemonStatus() ServiceStatus {
	return ServiceStatus{
		Status:           "running",
		FirmwareVersion:  "",
		UpdatesAvailable: false,
		InitiatorAddress: "",
	}
}

func (staticStatusProvider) AdapterStatus() ServiceStatus {
	return ServiceStatus{
		Status:           "unknown",
		FirmwareVersion:  "",
		UpdatesAvailable: false,
	}
}
