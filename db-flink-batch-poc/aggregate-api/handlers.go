package main

import (
	"encoding/json"
	"net/http"
)

type Handler struct {
	store Store
}

func NewHandler(s Store) *Handler {
	return &Handler{store: s}
}

func (h *Handler) Health(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("OK"))
}

func (h *Handler) TopSalesByCity(w http.ResponseWriter, r *http.Request) {
	data, err := h.store.TopSalesByCity()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(data)
}

func (h *Handler) TopSalesman(w http.ResponseWriter, r *http.Request) {
	data, err := h.store.TopSalesman()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(data)
}