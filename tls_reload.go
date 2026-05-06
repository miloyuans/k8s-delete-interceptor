package main

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"sync"
	"time"

	"k8s.io/klog/v2"
)

type dynamicTLSCertificateLoader struct {
	certPath string
	keyPath  string

	mu       sync.RWMutex
	cert     *tls.Certificate
	certInfo fileSnapshot
	keyInfo  fileSnapshot
}

type fileSnapshot struct {
	modTime time.Time
	size    int64
}

func newDynamicTLSCertificateLoader(certPath string, keyPath string) (*dynamicTLSCertificateLoader, error) {
	loader := &dynamicTLSCertificateLoader{
		certPath: certPath,
		keyPath:  keyPath,
	}

	if err := loader.reloadLocked(); err != nil {
		return nil, err
	}
	return loader, nil
}

func (l *dynamicTLSCertificateLoader) TLSConfig() *tls.Config {
	return &tls.Config{
		MinVersion:     tls.VersionTLS12,
		GetCertificate: l.GetCertificate,
	}
}

func (l *dynamicTLSCertificateLoader) GetCertificate(_ *tls.ClientHelloInfo) (*tls.Certificate, error) {
	if err := l.reloadIfChanged(); err != nil {
		klog.Errorf("Failed to reload TLS certificate from '%s' and '%s': %v", l.certPath, l.keyPath, err)
	}

	l.mu.RLock()
	defer l.mu.RUnlock()
	if l.cert == nil {
		return nil, fmt.Errorf("TLS certificate is not loaded")
	}
	return l.cert, nil
}

func (l *dynamicTLSCertificateLoader) reloadIfChanged() error {
	certInfo, err := snapshotFile(l.certPath)
	if err != nil {
		return err
	}
	keyInfo, err := snapshotFile(l.keyPath)
	if err != nil {
		return err
	}

	l.mu.RLock()
	unchanged := l.cert != nil && l.certInfo == certInfo && l.keyInfo == keyInfo
	l.mu.RUnlock()
	if unchanged {
		return nil
	}

	l.mu.Lock()
	defer l.mu.Unlock()
	certInfo, err = snapshotFile(l.certPath)
	if err != nil {
		return err
	}
	keyInfo, err = snapshotFile(l.keyPath)
	if err != nil {
		return err
	}
	if l.cert != nil && l.certInfo == certInfo && l.keyInfo == keyInfo {
		return nil
	}
	return l.reloadWithSnapshotsLocked(certInfo, keyInfo)
}

func (l *dynamicTLSCertificateLoader) reloadLocked() error {
	certInfo, err := snapshotFile(l.certPath)
	if err != nil {
		return err
	}
	keyInfo, err := snapshotFile(l.keyPath)
	if err != nil {
		return err
	}
	return l.reloadWithSnapshotsLocked(certInfo, keyInfo)
}

func (l *dynamicTLSCertificateLoader) reloadWithSnapshotsLocked(certInfo fileSnapshot, keyInfo fileSnapshot) error {
	cert, err := tls.LoadX509KeyPair(l.certPath, l.keyPath)
	if err != nil {
		return err
	}

	if len(cert.Certificate) > 0 {
		leaf, err := x509.ParseCertificate(cert.Certificate[0])
		if err == nil {
			cert.Leaf = leaf
			klog.Infof("Loaded TLS serving certificate. Subject=%s, NotAfter=%s, DNSNames=%v", leaf.Subject.String(), leaf.NotAfter.Format(time.RFC3339), leaf.DNSNames)
		} else {
			klog.Warningf("Loaded TLS certificate but failed to parse leaf certificate: %v", err)
		}
	}

	l.cert = &cert
	l.certInfo = certInfo
	l.keyInfo = keyInfo
	return nil
}

func snapshotFile(path string) (fileSnapshot, error) {
	info, err := os.Stat(path)
	if err != nil {
		return fileSnapshot{}, err
	}
	return fileSnapshot{
		modTime: info.ModTime(),
		size:    info.Size(),
	}, nil
}
