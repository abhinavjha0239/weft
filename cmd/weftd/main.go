// Command weftd is the server binary. M0 scope: prove the spine —
// event log, gateway with resume, auth, one message.created flow.
// See MILESTONES.md in the design repo.
package main

import (
	"fmt"
	"os"

	"github.com/abhinavjha0239/weft/internal/brand"
)

// version is stamped by the build (-ldflags "-X main.version=...").
var version = "0.0.1-dev"

func main() {
	if len(os.Args) > 1 && os.Args[1] == "version" {
		fmt.Printf("%s %s\n", brand.Slug, version)
		return
	}
	fmt.Printf("%s v%s — %s\n", brand.Name, version, brand.Tagline)
	fmt.Println("server not yet implemented — M0 in progress")
}
