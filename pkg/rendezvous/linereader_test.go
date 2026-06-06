package rendezvous

import (
	"bufio"
	"net"
	"strings"
)

type lineReader struct{ r *bufio.Reader }

func newLineReader(c net.Conn) *lineReader { return &lineReader{r: bufio.NewReader(c)} }

func (l *lineReader) line() (string, error) {
	s, err := l.r.ReadString('\n')
	return strings.TrimRight(s, "\r\n"), err
}
