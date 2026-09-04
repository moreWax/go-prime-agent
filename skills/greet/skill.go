// Package greet is the smoke-test Go skill.
package greet

// Hello returns a greeting for name.
func Hello(name string) string {
	if name == "" {
		name = "world"
	}
	return "hello, " + name
}
