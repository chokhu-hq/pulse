package auth

import (
	"crypto/hmac"
	"crypto/md5"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"
)

type Service struct {
	appKey    string
	appSecret string
}

func NewService(appKey, appSecret string) *Service {
	return &Service{
		appKey:    appKey,
		appSecret: appSecret,
	}
}

func (s *Service) GenerateSignature(socketID, channel string, channelData *string) string {
	var stringToSign string
	if channelData != nil {
		stringToSign = fmt.Sprintf("%s:%s:%s", socketID, channel, *channelData)
	} else {
		stringToSign = fmt.Sprintf("%s:%s", socketID, channel)
	}

	h := hmac.New(sha256.New, []byte(s.appSecret))
	h.Write([]byte(stringToSign))
	signature := hex.EncodeToString(h.Sum(nil))

	return signature
}

func (s *Service) GenerateAuthString(socketID, channel string, channelData *string) string {
	signature := s.GenerateSignature(socketID, channel, channelData)
	return fmt.Sprintf("%s:%s", s.appKey, signature)
}

func (s *Service) ValidateAuth(authString, socketID, channel string, channelData *string) bool {
	expectedAuth := s.GenerateAuthString(socketID, channel, channelData)
	return hmac.Equal([]byte(authString), []byte(expectedAuth))
}

func (s *Service) ValidateUserAuth(authString, socketID, userData string) bool {
	h := hmac.New(sha256.New, []byte(s.appSecret))
	h.Write([]byte(socketID + "::user::" + userData))
	expectedAuth := s.appKey + ":" + hex.EncodeToString(h.Sum(nil))
	return hmac.Equal([]byte(authString), []byte(expectedAuth))
}

func ParseAuthString(authString string) (appKey, signature string, err error) {
	parts := strings.SplitN(authString, ":", 2)
	if len(parts) != 2 {
		return "", "", fmt.Errorf("invalid auth string format")
	}
	return parts[0], parts[1], nil
}

func (s *Service) ValidateHTTPRequest(method, path string, queryParams url.Values, body []byte) error {
	authKey := queryParams.Get("auth_key")
	authTimestamp := queryParams.Get("auth_timestamp")
	authVersion := queryParams.Get("auth_version")
	authSignature := queryParams.Get("auth_signature")
	bodyMD5 := queryParams.Get("body_md5")

	if authKey == "" {
		return fmt.Errorf("missing auth_key")
	}
	if authTimestamp == "" {
		return fmt.Errorf("missing auth_timestamp")
	}
	if authVersion == "" {
		return fmt.Errorf("missing auth_version")
	}
	if authSignature == "" {
		return fmt.Errorf("missing auth_signature")
	}

	if authKey != s.appKey {
		return fmt.Errorf("invalid auth_key")
	}

	if authVersion != "1.0" {
		return fmt.Errorf("unsupported auth_version: %s", authVersion)
	}

	timestamp, err := strconv.ParseInt(authTimestamp, 10, 64)
	if err != nil {
		return fmt.Errorf("invalid auth_timestamp")
	}
	now := time.Now().Unix()
	if now-timestamp > 600 || timestamp-now > 600 {
		return fmt.Errorf("auth_timestamp too old or in future")
	}

	if len(body) > 0 {
		if bodyMD5 == "" {
			return fmt.Errorf("missing body_md5 for non-empty body")
		}
		expectedMD5 := fmt.Sprintf("%x", md5.Sum(body))
		if bodyMD5 != expectedMD5 {
			return fmt.Errorf("body_md5 mismatch")
		}
	}

	expectedSignature := s.GenerateHTTPSignature(method, path, queryParams)

	if !hmac.Equal([]byte(authSignature), []byte(expectedSignature)) {
		return fmt.Errorf("invalid auth_signature")
	}

	return nil
}

func (s *Service) GenerateHTTPSignature(method, path string, queryParams url.Values) string {
	params := url.Values{}
	for k, v := range queryParams {
		if k != "auth_signature" {
			params[k] = v
		}
	}

	keys := make([]string, 0, len(params))
	for k := range params {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	queryParts := make([]string, 0, len(keys))
	for _, k := range keys {
		for _, v := range params[k] {
			queryParts = append(queryParts, fmt.Sprintf("%s=%s", k, v))
		}
	}
	queryString := strings.Join(queryParts, "&")

	stringToSign := fmt.Sprintf("%s\n%s\n%s", strings.ToUpper(method), path, queryString)

	h := hmac.New(sha256.New, []byte(s.appSecret))
	h.Write([]byte(stringToSign))
	signature := hex.EncodeToString(h.Sum(nil))

	return signature
}
