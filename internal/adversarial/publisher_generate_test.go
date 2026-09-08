package adversarial

import (
	"testing"
	"time"
)

func TestGeneratePublishedReports(t *testing.T) {
	publisher, err := NewPublisher()
	if err != nil {
		t.Fatal(err)
	}
	startedAt := time.Date(2026, 9, 7, 12, 0, 0, 0, time.UTC)
	for _, name := range []string{"evaluated-fail", "execution-error", "infrastructure-block", "offline-all-pass"} {
		driver, err := fixtureFiles.ReadFile("fixtures/v1/inputs/" + name + ".json")
		if err != nil {
			t.Fatal(err)
		}
		report, err := publisher.NewExecutor(startedAt, nil, nil).Run(driver)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if err := publisher.WriteReport(report, name+".json"); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
	}
}
