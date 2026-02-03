//go:build tinygo

package mdns

type unsupportedRegistrar struct{}

func defaultRegistrar() registrar {
	return unsupportedRegistrar{}
}

func (unsupportedRegistrar) Register(service Service) (Advertiser, error) {
	return nil, ErrUnsupported
}
