package volsync

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	kerrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

// validateSecretAndAddOwnerRef validates that a secret exists and adds owner reference
// The secret must be pre-created by the user
func (v *VSHandler) validateSecretAndAddOwnerRef(secretName, secretNamespace string) (bool, error) {
	secret := &corev1.Secret{}

	err := v.client.Get(v.ctx,
		types.NamespacedName{
			Name:      secretName,
			Namespace: secretNamespace,
		}, secret)
	if err != nil {
		if kerrors.IsNotFound(err) {
			v.log.Error(err, "Secret not found - must be pre-created", "secretName", secretName)
			return false, fmt.Errorf("secret %s not found in namespace %s - must be created before replication",
				secretName, secretNamespace)
		}

		v.log.Error(err, "Failed to get secret", "secretName", secretName)
		return false, fmt.Errorf("error getting secret (%w)", err)
	}

	v.log.Info("Secret exists", "secretName", secretName)

	// Add owner reference
	// if err := v.addOwnerReferenceAndUpdate(secret, v.owner); err != nil {
	// 	v.log.Error(err, "Unable to update secret", "secretName", secretName)
	// 	return true, err
	// }

	v.log.V(1).Info("VolSync secret validated", "secret name", secretName)

	return true, nil
}

// generateVolSyncReplicationSecret generates a new VolSync replication secret with PSK
func (v *VSHandler) generateVolSyncReplicationSecret(secretName string) (*corev1.Secret, error) {
	tlsKey, err := genTLSPreSharedKey(v.log)
	if err != nil {
		v.log.Error(err, "Unable to generate new tls secret for VolSync replication")
		return nil, err
	}

	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      secretName,
			Namespace: v.owner.GetNamespace(),
		},
		StringData: map[string]string{
			"psk.txt": "volsyncmock:" + tlsKey,
		},
	}

	return secret, nil
}

// genTLSPreSharedKey generates a TLS pre-shared key
func genTLSPreSharedKey(log logr.Logger) (string, error) {
	pskData := make([]byte, tlsPSKDataSize)
	if _, err := rand.Read(pskData); err != nil {
		log.Error(err, "error generating tls key")
		return "", err
	}

	return hex.EncodeToString(pskData), nil
}
