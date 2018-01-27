package elasticsearch

/*
 * Logger defines, that the logger for this package needs to bring
 * the function 'Printf' with it.
 */
type Logger interface {
	Infof(format string, a ...interface{})
	Debugf(format string, a ...interface{})
	DebugMode() bool
}

/*
 * SetLogger changes the logger for the elasticsearch package.
 * By default, all messages are discarded.
 */
func SetLogger(l Logger) {
	log = l
}

/*
 * discard will be used as default logger and will discard all messages.
 */
type discard struct{}

/*
 * Infof will do nothing.
 */
func (d *discard) Infof(_ string, _ ...interface{}) {}

/*
 * Debugf will do nothing.
 */
func (d *discard) Debugf(_ string, _ ...interface{}) {}

/*
 * DebugMode will return always false.
 */
func (d *discard) DebugMode() bool {return false}

/*
 * log can be used to print messages. By default, discard all messages.
 */
var log Logger = &discard{}
