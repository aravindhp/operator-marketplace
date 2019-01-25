package catalogsourceconfig

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/ghodss/yaml"
	"github.com/operator-framework/operator-marketplace/pkg/datastore"
)

type manifest struct {
	singleManifest *datastore.SingleOperatorManifest
	registryDir    string
	manifestDir    string
	versionDir     string
}

// Manifest is the interface for creating an operator-registry manifest for an
// operator on disk. The definition of manifest and bundle are per
// https://github.com/operator-framework/operator-registry.
type Manifest interface {
	// Create() is the entrypoint to generate the operator files on disk as per
	// https://github.com/operator-framework/operator-registry#manifest-format.
	// It is up to the caller to call Delete() on any error or to clean up.
	// The function returns on the first encountered error and does not attempt
	// to create a partially valid manifest. For example: if CSV v1 is valid but
	// CSV v2 is invalid, it will return an error and the caller is required to
	// call Delete().
	Create() error

	// Delete() is used to delete the manifest directories and files on error or
	// when the manifest is no longer needed.
	Delete() error
}

// NewManifest returns a new instance of manifest
func NewManifest(singleManifest *datastore.SingleOperatorManifest, registryDir string) Manifest {
	return &manifest{
		singleManifest: singleManifest,
		registryDir:    registryDir,
	}
}

func (b *manifest) Create() (err error) {
	err = b.createManifestDir()
	if err != nil {
		return
	}

	err = b.createPackageYAML()
	if err != nil {
		return
	}

	err = b.createBundles()
	if err != nil {
		return
	}

	return
}

// createBundle creates the bundle for the CSV
func (b *manifest) createBundle(csv *datastore.ClusterServiceVersion) (err error) {
	bundleDir, err := b.createBundleDir(csv)
	if err != nil {
		return
	}
	err = b.createCRDYAMLs(csv, bundleDir)
	if err != nil {
		return
	}
	err = b.createCSVYAML(csv, bundleDir)
	if err != nil {
		return
	}
	return
}

// createBundleDir creates the bundle directory which maps to the CSV version
func (b *manifest) createBundleDir(csv *datastore.ClusterServiceVersion) (string, error) {
	version, err := csv.GetVersion()
	if err != nil {
		return "", err
	}
	if version == "" {
		return "", fmt.Errorf("Unable to create bundle dir as CSV is missing version")
	}

	bundleDir := filepath.Join(b.manifestDir, version)
	return bundleDir, createDir(bundleDir)
}

// createBundles creates bundles for each CSV
func (b *manifest) createBundles() (err error) {
	for _, csv := range b.singleManifest.ClusterServiceVersions {
		b.createBundle(csv)
	}
	return
}

// createCRDYAML creates the CRD YAML in the bundle directory
func (b *manifest) createCRDYAML(crd *datastore.CustomResourceDefinition, bundleDir string) error {
	return createYAML(crd, filepath.Join(bundleDir, crd.GetName()+".crd.yaml"))
}

// createCRDYAMLs creates the CRD YAML files for each CRD listed in the "owned"
// section of the CSV spec
func (b *manifest) createCRDYAMLs(csv *datastore.ClusterServiceVersion, bundleDir string) error {
	ownedCRDKeys, _, err := csv.GetCustomResourceDefintions()
	if err != nil {
		return err
	}

	crdMap := datastore.CustomResourceDefinitionMap{}
	for _, crd := range b.singleManifest.CustomResourceDefinitions {
		crdMap[crd.Key()] = crd
	}

	for _, ownedCRDKey := range ownedCRDKeys {
		crd, found := crdMap[*ownedCRDKey]
		if !found {
			return fmt.Errorf("Owned CRD %s for CSV %s not found", ownedCRDKey, csv.GetName())
		}
		err = b.createCRDYAML(crd, bundleDir)
		if err != nil {
			return err
		}
	}
	return nil
}

// createCSVYAMLs creates the CSV YAML file in the bundle directory
func (b *manifest) createCSVYAML(csv *datastore.ClusterServiceVersion, bundleDir string) error {
	return createYAML(csv, filepath.Join(bundleDir, csv.GetName()+".csv.yaml"))
}

// createManifestDir creates the package directory. Example: registryDir/etcd
func (b *manifest) createManifestDir() error {
	b.manifestDir = filepath.Join(b.registryDir, b.singleManifest.GetPackageID())
	return createDir(b.manifestDir)
}

// createPackage creates the package YAML file. Example: etcd.package.yaml
func (b *manifest) createPackageYAML() error {
	return createYAML(
		b.singleManifest.Package,
		filepath.Join(b.manifestDir, b.singleManifest.GetPackageID()+".package.yaml"),
	)
}

func (b *manifest) Delete() error {
	if b.manifestDir == "" {
		return nil
	}
	return os.RemoveAll(b.manifestDir)
}

func createDir(dir string) error {
	err := os.Mkdir(dir, 0700)
	if err != nil {
		return fmt.Errorf("Error %s creating %s directory", err, dir)
	}
	return nil
}

func createYAML(obj interface{}, file string) error {
	raw, err := yaml.Marshal(obj)
	if err != nil {
		return fmt.Errorf("Error %s marshaling %s into YAML", obj, err)
	}

	err = ioutil.WriteFile(file, raw, 0666)
	if err != nil {
		return fmt.Errorf("Error %s creating %s file", err, file)
	}

	return nil
}
