// console
package console

import (
	"bufio"
	"code.google.com/p/go.net/context"
	log "github.com/cihub/seelog"
	"strings"
)

type ConsoleManager struct {
	ctx context.Context

	reader *bufio.Reader
	writer *bufio.Writer

	handlers map[string]func([]string)
}

func Init(ctx context.Context, reader *bufio.Reader, writer *bufio.Writer) (cm *ConsoleManager) {
	cm = &ConsoleManager{ctx, reader, writer, make(map[string]func([]string))}
	return
}

func (cm ConsoleManager) AddHandler(name string, handler func([]string)) {
	cm.handlers[name] = handler
	log.Tracef("Add handler for %s(%d) = %v", name, len(name), cm.handlers[name])
}

func (cm ConsoleManager) Start() {
	log.Trace("Start ConsoleManager")
	for {
		line, err := cm.reader.ReadString('\n')
		if err != nil {
			log.Error(err)
			continue
		}

		args := strings.Split(line[0:len(line)-2], " ")
		if len(args) > 0 {
			var name string
			name = args[0]

			handler, ok := cm.handlers[name]
			log.Tracef("Handler for %s (%d) = %v", name, len(name), handler)
			if ok {
				handler(args)
			}
		}

		select {
		case <-cm.ctx.Done():
			return
		}

	}
}
