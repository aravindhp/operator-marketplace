package catalogsourceconfig_test

import (
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"testing"

	"github.com/ghodss/yaml"
	"github.com/operator-framework/operator-marketplace/pkg/apis/marketplace/v1alpha1"
	"github.com/operator-framework/operator-marketplace/pkg/catalogsourceconfig"
	"github.com/operator-framework/operator-marketplace/pkg/datastore"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

var ds = datastore.New()

func TestBadRootDir(t *testing.T) {
	setupDatastore(t)

	sm, err := ds.ReadSingle("etcd")
	assert.NoError(t, err)

	manifest := catalogsourceconfig.NewManifest(sm, "/badir")
	err = manifest.Create()
	assert.Error(t, err)
}

// TestCreateManifest tests if the manifest directory and files created on disk
// matches the expected layout as per https://github.com/operator-framework/operator-registry#manifest-format
func TestCreateManifest(t *testing.T) {
	setupDatastore(t)

	expectedPackage := "etcd"
	sm, err := ds.ReadSingle(expectedPackage)
	require.NoError(t, err)

	tempDir, err := ioutil.TempDir("", "registry")
	if err != nil {
		log.Fatal(err)
	}

	manifest := catalogsourceconfig.NewManifest(sm, tempDir)
	err = manifest.Create()
	assert.NoError(t, err)
	manifestDir := filepath.Join(tempDir, expectedPackage)

	// Check if /tmp/registryNNN/etcd/ was created
	assert.DirExists(t, manifestDir)

	// Check if /tmp/registryNNN/etcd/etcd.package.yaml was created
	filename := filepath.Join(manifestDir, expectedPackage+".package.yaml")
	assert.FileExists(t, filename)

	// Check if /tmp/registryNNN/etcd/etcd.package.yaml is a valid YAML file
	checkYaml(t, filename)

	// Check if the bundle directories (ex: /tmp/registryNNN/etcd/0.6.1) were
	// created along with the "owned" CRDs. The map has been hard coded on
	// purposes as we don't want to use the same functions used in manifest.go
	// to programtically inspect the manifest
	bundleCRDFilesMap := map[string][]string{
		"0.6.1": {"etcdclusters.etcd.database.coreos.com.crd.yaml"},
		"0.9.0": {"etcdbackups.etcd.database.coreos.com.crd.yaml",
			"etcdclusters.etcd.database.coreos.com.crd.yaml",
			"etcdrestores.etcd.database.coreos.com.crd.yaml"},
		"0.9.2": {"etcdbackups.etcd.database.coreos.com.crd.yaml",
			"etcdclusters.etcd.database.coreos.com.crd.yaml",
			"etcdrestores.etcd.database.coreos.com.crd.yaml"},
	}
	for bundleDir, crdFiles := range bundleCRDFilesMap {
		assert.DirExists(t, filepath.Join(manifestDir, bundleDir))
		for _, crdFile := range crdFiles {
			assert.FileExists(t, filepath.Join(manifestDir, bundleDir, crdFile))
		}
	}

	err = manifest.Delete()
	assert.NoError(t, err)
	_, err = os.Stat(manifestDir)
	assert.True(t, os.IsNotExist(err))

	os.RemoveAll(tempDir)
}

func checkYaml(t *testing.T, filename string) {
	file, err := os.Open(filename)
	assert.NoError(t, err)
	data, err := ioutil.ReadAll(file)
	assert.NoError(t, err)
	_, err = yaml.YAMLToJSON(data)
	assert.NoError(t, err)
}

func helperLoadFromFile(t *testing.T, filename string) *datastore.OperatorMetadata {
	path := filepath.Join("../testdata", filename)

	bytes, err := ioutil.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}

	return &datastore.OperatorMetadata{
		RegistryMetadata: datastore.RegistryMetadata{
			Namespace:  "operators",
			Repository: "redhat",
		},
		RawYAML: bytes,
	}
}

func setupDatastore(t *testing.T) {
	opsrc := &v1alpha1.OperatorSource{
		ObjectMeta: metav1.ObjectMeta{
			UID: types.UID("123456"),
		},
	}

	metadata := []*datastore.OperatorMetadata{
		helperLoadFromFile(t, "rh-operators.yaml"),
	}

	_, err := ds.Write(opsrc, metadata)
	require.NoError(t, err)
}
