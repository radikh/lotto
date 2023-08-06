package rwio

import (
	"fmt"
	"log"
	"testing"

	"github.com/dgrijalva/jwt-go"
)

func TestJWT(t *testing.T) {
	// decode jwt

	// decode segment 1
	seg1, err := jwt.DecodeSegment("eyJhbGciOiJSUzI1NiIsImtpZCI6IjJkOWE1ZWY1YjEyNjIzYzkxNjcxYTcwOTNjYjMyMzMzM2NkMDdkMDkiLCJ0eXAiOiJKV1QifQ")
	if err != nil {
		log.Fatal(err)
	}

	// decode segment 2

	seg2, err := jwt.DecodeSegment("eyJpc3MiOiJodHRwczovL2FjY291bnRzLmdvb2dsZS5jb20iLCJhenAiOiIzMjU1NTk0MDU1OS5hcHBzLmdvb2dsZXVzZXJjb250ZW50LmNvbSIsImF1ZCI6IjMyNTU1OTQwNTU5LmFwcHMuZ29vZ2xldXNlcmNvbnRlbnQuY29tIiwic3ViIjoiMTA2MTU0OTYyMDAwMjM0NzkxNTQ3IiwiZW1haWwiOiJyYWRpa2hzQGdtYWlsLmNvbSIsImVtYWlsX3ZlcmlmaWVkIjp0cnVlLCJhdF9oYXNoIjoieHZ2dGZCbVZDckpLeVV6c0lZSDUxdyIsImlhdCI6MTY4NTI3OTI5MSwiZXhwIjoxNjg1MjgyODkxfQ")
	if err != nil {
		log.Fatal(err)
	}

	// decode segment 3

	seg3, err := jwt.DecodeSegment("oDdd09lmD284u6s911B2LevDzM2LTJG_uEWNE5e_QQZ8vEOBXvW3hcBq8q7owCOQVgsBXaPZr4DP3P-5mApjn5wNYcQfsZkKeIdx9eh-wtS9Z8lmeClsFxa_52OpgUExIYzl7so2dup_MK5oB0O9fwM1wumYZqpWw9LT-hn4wxG5sQayBnVvyb50wurRnwSV6juqRixl0ds12RHMaQOGMuDZR6DqTIp9tbOStV2nT9ETlNeLgKJ5NqXgZ7CtzanSmoFr_Mv8IlSRPc5xgc9mDAru3ZVr9F0Rj4zgfki8XAYSXvY9bjkjNdWO-mOY1O8SymSl2XmtP6TGTu9PDrmWdg")
	if err != nil {
		log.Fatal(err)
	}

	// prrint segments

	fmt.Println(string(seg1))
	fmt.Println(string(seg2))
	fmt.Println(string(seg3))
}
