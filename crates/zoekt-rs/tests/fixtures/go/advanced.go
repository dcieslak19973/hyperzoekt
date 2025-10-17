package sample

// unexported type with method receiver
type node struct{}

func (n *node) doIt() {}

// unexported top-level function
func helper() {}

// private method on existing Server type
func (s *Server) startPrivate() {}
