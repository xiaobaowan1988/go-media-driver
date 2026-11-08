package protocol

import "testing"

func TestCommandRoundTrip(t *testing.T) {
	in := AddPublication{CorrelationID: 7, SessionID: -1, StreamID: 2, RemoteIP: [4]byte{10, 1, 2, 3}, RemotePort: 40123,
		InitialTermID: 9, TermLength: 1 << 20, MTU: 1408, InitialWindow: 4096, RespPath: "/dev/shm/x.resp"}
	b := in.Marshal()
	if typ, corr, err := Header(b); err != nil || typ != CmdAddPublication || corr != 7 {
		t.Fatalf("header %d %d %v", typ, corr, err)
	}
	var out AddPublication
	if err := out.Unmarshal(b); err != nil || out != in {
		t.Fatalf("%+v %v", out, err)
	}
	if err := out.Unmarshal(b[:len(b)-3]); err == nil {
		t.Fatal("truncated command accepted")
	}
	r := Response{Type: RspReady, CorrelationID: 3, Text: "5|/x/y"}
	var r2 Response
	if err := r2.Unmarshal(r.Marshal()); err != nil || r2 != r {
		t.Fatalf("%+v %v", r2, err)
	}
}
