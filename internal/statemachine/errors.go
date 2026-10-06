package statemachine

import "errors"

// ErrSuspended indicates a batch folds into a period where inspection is
// suspended; a resume event must precede it on the timeline.
var ErrSuspended = errors.New("inspection is suspended")
