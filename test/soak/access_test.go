package soak_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func issueSoakSubscriber(t *testing.T, endpoint *httptest.Server, id, owner string) string {
	t.Helper()
	request, err := http.NewRequest(http.MethodPost, endpoint.URL+"/v1/sessions/"+id+"/subscribe", bytes.NewBufferString(`{"bus_id":"application"}`))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Authorization", "Bearer "+owner)
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("subscriber issuance status=%d", response.StatusCode)
	}
	var result struct {
		Token string `json:"subscriber_token"`
	}
	if err = json.NewDecoder(response.Body).Decode(&result); err != nil {
		t.Fatal(err)
	}
	return result.Token
}
