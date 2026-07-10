package rest

import (
	"net/http"
	"strconv"

	"github.com/abhinavjha0239/weft/internal/auth"
	"github.com/abhinavjha0239/weft/internal/domain/search"
)

func (a *api) handleSearch(w http.ResponseWriter, r *http.Request, id auth.Identity) {
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	results, err := a.search.Search(r.Context(), id, r.URL.Query().Get("q"), limit)
	if err != nil {
		writeDomainError(w, a.Log, r, err)
		return
	}
	if results == nil {
		results = []search.Result{}
	}
	writeJSON(w, http.StatusOK, map[string]any{"results": results})
}
