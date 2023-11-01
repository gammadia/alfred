package scheduler

import (
	"fmt"
	"sync/atomic"

	"github.com/gammadia/alfred/proto"
)

type Job struct {
	*proto.Job

	id string

	tasksCompleted atomic.Uint32
}

func (j *Job) FQN() string {
	return fmt.Sprintf("%s-%s", j.Name, j.id)
}
