package elasticsearch

import "fmt"

type FmtLogger struct{
	Debug bool
}

func (f *FmtLogger) Infof(format string, a ...interface{}) {
	fmt.Printf(format, a...)
}

func (f *FmtLogger) Debugf(format string, a ...interface{}) {
	if f.Debug {
		fmt.Printf(format, a...)
	}
}

func (f *FmtLogger) DebugMode() bool {
	return f.Debug
}

func ExampleSetLogger() {
	log.Infof("Hello, this message will be discarded.\n")
	log.Debugf("DEBUG: Hello, this message will be discarded.\n")
	f := &FmtLogger{}
	SetLogger(f)
	log.Infof("Hello, this message will be shown.\n")
	log.Debugf("DEBUG: Hello, this message will be discarded.\n")
	f.Debug = true
	log.Infof("Hello, this message will be shown.\n")
	log.Debugf("DEBUG: Hello, this message will be shown.\n")
	// Output: Hello, this message will be shown.
	// Hello, this message will be shown.
	// DEBUG: Hello, this message will be shown.
}
