package validation_test

import (
	"errors"
	"fmt"

	"github.com/zanvd/slumber-mill/src/validation"
)

func ExampleCollector() {
	var v validation.Collector
	v.Add("title", "is required")
	v.Add("file", "is empty")
	err := fmt.Errorf("error creating asset: %w", v.Err())

	if verr, ok := errors.AsType[*validation.Error](err); ok {
		for _, f := range verr.Fields() {
			fmt.Printf("%s: %s\n", f.Name, f.Message)
		}
	}
	fmt.Println(err)
	// Output:
	// title: is required
	// file: is empty
	// error creating asset: invalid input: title: is required; file: is empty
}
