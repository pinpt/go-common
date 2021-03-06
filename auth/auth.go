package auth

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"net/http"
	"os"

	"golang.org/x/crypto/pbkdf2"
)

const (
	algorithmNonceSize int = 12
	algorithmKeySize   int = 32
	pbkdf2SaltSize     int = 16
	pbkdf2Iterations   int = 32767
)

// WithOption allows options to be customized
type WithOption struct {
	URLSafe bool
}

// WithOptionFunc is a function for receiving options
type WithOptionFunc func(opts *WithOption)

// WithURLSafeOption will return a url safe encryption key
func WithURLSafeOption() WithOptionFunc {
	return func(opts *WithOption) {
		opts.URLSafe = true
	}
}

// EncryptString will encrypt plaintext using password
func EncryptString(plaintext, password string, options ...WithOptionFunc) (string, error) {
	// Generate a 128-bit salt using a CSPRNG.
	salt := make([]byte, pbkdf2SaltSize)
	_, err := rand.Read(salt)
	if err != nil {
		return "", err
	}

	// Derive a key using PBKDF2.
	key := pbkdf2.Key([]byte(password), salt, pbkdf2Iterations, algorithmKeySize, sha256.New)

	// Encrypt and prepend salt.
	ciphertextAndNonce, err := encrypt([]byte(plaintext), key)
	if err != nil {
		return "", err
	}

	ciphertextAndNonceAndSalt := make([]byte, 0)
	ciphertextAndNonceAndSalt = append(ciphertextAndNonceAndSalt, salt...)
	ciphertextAndNonceAndSalt = append(ciphertextAndNonceAndSalt, ciphertextAndNonce...)

	opts := &WithOption{}
	for _, opt := range options {
		opt(opts)
	}

	if opts.URLSafe {
		return base64.URLEncoding.EncodeToString(ciphertextAndNonceAndSalt), nil
	}

	// Return as base64 string.
	return base64.StdEncoding.EncodeToString(ciphertextAndNonceAndSalt), nil
}

// DecryptString will decrypt the encoded value with password
func DecryptString(base64CiphertextAndNonceAndSalt, password string, options ...WithOptionFunc) (string, error) {
	// Decode the base64.

	opts := &WithOption{}
	for _, opt := range options {
		opt(opts)
	}

	var err error
	var ciphertextAndNonceAndSalt []byte

	if opts.URLSafe {
		ciphertextAndNonceAndSalt, err = base64.URLEncoding.DecodeString(base64CiphertextAndNonceAndSalt)
	} else {
		ciphertextAndNonceAndSalt, err = base64.StdEncoding.DecodeString(base64CiphertextAndNonceAndSalt)
	}
	if err != nil {
		return "", err
	}

	// Create slices pointing to the salt and ciphertextAndNonce.
	salt := ciphertextAndNonceAndSalt[:pbkdf2SaltSize]
	ciphertextAndNonce := ciphertextAndNonceAndSalt[pbkdf2SaltSize:]

	// Derive the key using PBKDF2.
	key := pbkdf2.Key([]byte(password), salt, pbkdf2Iterations, algorithmKeySize, sha256.New)

	// Decrypt and return result.
	plaintext, err := decrypt(ciphertextAndNonce, key)
	if err != nil {
		return "", err
	}

	return string(plaintext), nil
}

func encrypt(plaintext, key []byte) ([]byte, error) {
	// Generate a 96-bit nonce using a CSPRNG.
	nonce := make([]byte, algorithmNonceSize)
	_, err := rand.Read(nonce)
	if err != nil {
		return nil, err
	}

	// Create the cipher and block.
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}

	cipher, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}

	// Encrypt and prepend nonce.
	ciphertext := cipher.Seal(nil, nonce, plaintext, nil)
	ciphertextAndNonce := make([]byte, 0)

	ciphertextAndNonce = append(ciphertextAndNonce, nonce...)
	ciphertextAndNonce = append(ciphertextAndNonce, ciphertext...)

	return ciphertextAndNonce, nil
}

func decrypt(ciphertextAndNonce, key []byte) ([]byte, error) {
	// Create slices pointing to the ciphertext and nonce.
	nonce := ciphertextAndNonce[:algorithmNonceSize]
	ciphertext := ciphertextAndNonce[algorithmNonceSize:]

	// Create the cipher and block.
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}

	cipher, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}

	// Decrypt and return result.
	plaintext, err := cipher.Open(nil, nonce, ciphertext, nil)
	if err != nil {
		return nil, err
	}

	return plaintext, nil
}

// Info contains authentication information about the current request.
type Info struct {
	Authenticated bool
	Internal      bool // Internal is not implemented yet
	CustomerID    string
	UserID        string
}

// GetAuthInfo will extract pinpoint auth info from a request
func GetAuthInfo(r *http.Request) Info {
	customerID := r.Header.Get("pinpt-customer-id")
	if customerID == "" {
		customerID = os.Getenv("PP_CUSTOMER_ID")
	}
	userID := r.Header.Get("pinpt-user-id")
	if userID == "" {
		userID = os.Getenv("PP_USER_ID")
	}
	return Info{
		Authenticated: customerID != "" && userID != "",
		CustomerID:    customerID,
		UserID:        userID,
	}
}
