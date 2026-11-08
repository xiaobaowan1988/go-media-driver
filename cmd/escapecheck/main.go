// Command escapecheck enforces the "zero heap allocation on the hot path"
// rule statically:
//
//  1. AST check: hot functions must not contain make/new/append, &T{...},
//     slice/map literals, closures, type assertions or type switches.
//  2. Escape-analysis check: `go build -gcflags=-m` must report no
//     "escapes to heap" / "moved to heap" inside a hot function body.
//
// Run from the repository root: go run ./cmd/escapecheck
package main

import (
	"bufio"
	"bytes"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

// hot lists "importpath-dir:Receiver.Func" (Receiver without '*').
var hot = map[string][]string{
	"logbuffer": {"LogBuffer.TryClaim", "BufferClaim.Commit", "LogBuffer.Offer", "LogBuffer.Poll", "LogBuffer.ScanBlock",
		"LogBuffer.Insert", "LogBuffer.ContiguousEnd", "LogBuffer.GapEnd", "LogBuffer.CleanConsumed", "LogBuffer.rotate",
		"LogBuffer.writePad", "LogBuffer.Tail"},
	"udp": {"Socket.Add", "Socket.Flush", "Socket.Recv", "Socket.Send", "Socket.SendTo", "Socket.Reset"},
	"driver": {"Sender.DoWork", "Sender.Run", "NetworkPublication.doWork", "NetworkPublication.send",
		"NetworkPublication.pollControl", "NetworkPublication.onSM", "NetworkPublication.onNak",
		"NetworkPublication.retransmitOne", "NetworkPublication.heartbeat",
		"Receiver.DoWork", "Receiver.Run", "NetworkSubscription.onDatagram", "NetworkSubscription.housekeeping"},
}

type span struct {
	file       string
	start, end int
	name       string
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Println("escapecheck: OK (no allocating constructs or heap escapes in hot functions)")
}

func run() error {
	var spans []span
	var problems []string
	for dir, names := range hot {
		want := map[string]bool{}
		for _, n := range names {
			want[n] = true
		}
		fset := token.NewFileSet()
		pkgs, err := parser.ParseDir(fset, dir, func(fi os.FileInfo) bool { return !strings.HasSuffix(fi.Name(), "_test.go") }, 0)
		if err != nil {
			return err
		}
		for _, pkg := range pkgs {
			for _, f := range pkg.Files {
				for _, d := range f.Decls {
					fd, ok := d.(*ast.FuncDecl)
					if !ok || fd.Body == nil {
						continue
					}
					name := fd.Name.Name
					if fd.Recv != nil && len(fd.Recv.List) == 1 {
						t := fd.Recv.List[0].Type
						if s, ok := t.(*ast.StarExpr); ok {
							t = s.X
						}
						if id, ok := t.(*ast.Ident); ok {
							name = id.Name + "." + name
						}
					}
					if !want[name] {
						continue
					}
					delete(want, name)
					p := fset.Position(fd.Pos())
					e := fset.Position(fd.End())
					spans = append(spans, span{filepath.ToSlash(p.Filename), p.Line, e.Line, dir + "." + name})
					problems = append(problems, astCheck(fset, fd, dir+"."+name)...)
				}
			}
		}
		for n := range want {
			problems = append(problems, fmt.Sprintf("hot function %s.%s not found (update cmd/escapecheck)", dir, n))
		}
	}

	out, _ := exec.Command("go", "build", "-gcflags=-m", "./logbuffer", "./udp", "./driver").CombinedOutput()
	re := regexp.MustCompile(`^(\S+\.go):(\d+):\d+: (.*(?:escapes to heap|moved to heap).*)$`)
	sc := bufio.NewScanner(bytes.NewReader(out))
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	for sc.Scan() {
		m := re.FindStringSubmatch(strings.TrimPrefix(sc.Text(), "./"))
		if m == nil {
			continue
		}
		line, _ := strconv.Atoi(m[2])
		file := filepath.ToSlash(m[1])
		for _, s := range spans {
			if strings.HasSuffix(s.file, file) && line >= s.start && line <= s.end {
				problems = append(problems, fmt.Sprintf("%s:%d: %s (in %s)", file, line, m[3], s.name))
			}
		}
	}
	if len(problems) > 0 {
		sort.Strings(problems)
		return fmt.Errorf("escapecheck: %d problem(s):\n  %s", len(problems), strings.Join(problems, "\n  "))
	}
	return nil
}

func astCheck(fset *token.FileSet, fd *ast.FuncDecl, name string) []string {
	var out []string
	add := func(n ast.Node, what string) {
		out = append(out, fmt.Sprintf("%s: %s in hot function %s", fset.Position(n.Pos()), what, name))
	}
	ast.Inspect(fd.Body, func(n ast.Node) bool {
		switch x := n.(type) {
		case *ast.CallExpr:
			if id, ok := x.Fun.(*ast.Ident); ok && (id.Name == "make" || id.Name == "new" || id.Name == "append") {
				add(x, id.Name+"()")
			}
		case *ast.TypeAssertExpr:
			add(x, "type assertion")
		case *ast.TypeSwitchStmt:
			add(x, "type switch")
		case *ast.FuncLit:
			add(x, "closure")
		case *ast.UnaryExpr:
			if _, ok := x.X.(*ast.CompositeLit); ok && x.Op == token.AND {
				add(x, "&T{}")
			}
		case *ast.CompositeLit:
			switch t := x.Type.(type) {
			case *ast.MapType:
				add(x, "map literal")
			case *ast.ArrayType:
				if t.Len == nil {
					add(x, "slice literal")
				}
			}
		}
		return true
	})
	return out
}
