package main

import (
	"bytes"
	"text/template"
)

func renderPlainTemplate(name, body string, data map[string]string) (string, error) {
	t, err := template.New(name).Option("missingkey=zero").Parse(body)
	if err != nil {
		return "", err
	}
	var buf bytes.Buffer
	if err := t.Execute(&buf, data); err != nil {
		return "", err
	}
	return buf.String(), nil
}
