package e2e

import (
	goctx "context"
	"testing"
	"time"

	"github.com/operator-framework/operator-marketplace/pkg/apis/marketplace/v1alpha1"
	"github.com/operator-framework/operator-sdk/pkg/test"
	"github.com/operator-framework/operator-sdk/pkg/test/e2eutil"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
)

// Global test context that can be shared across subtests.
var ctx *test.TestCtx

func TestCatalogSourceConfig(t *testing.T) {
	initializeFramework(t)

	// Initialize the test context
	ctx = test.NewTestCtx(t)
	defer ctx.Cleanup(t)

	initializeClusterResources(t)

	// run subtests
	t.Run("CatalogSourceConfig-group", func(t *testing.T) {
		t.Run("Create", CatalogSourceConfigCreate)
	})
}

func CatalogSourceConfigCreate(t *testing.T) {
	// Get the global controller runtime client
	dclient := test.Global.DynamicClient

	// Create the namespace where the CatalogSource will be created
	targetNamespace := &corev1.Namespace{
		TypeMeta: metav1.TypeMeta{
			Kind:       "Namespace",
			APIVersion: "v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name: "ns-csc-createtest",
		},
	}
	err := dclient.Create(goctx.TODO(), targetNamespace)
	if err != nil {
		t.Fatal(err)
	}

	ctx.AddFinalizerFn(func() error {
		return dclient.Delete(goctx.TODO(), targetNamespace)
	})

	testNamespace := getTestNamespace(t)
	testCatalogSourceConfigName := "createtest"
	testCatalogSourceConfig := &v1alpha1.CatalogSourceConfig{
		TypeMeta: metav1.TypeMeta{
			Kind:       "CatalogSourceConfig",
			APIVersion: "marketplace.redhat.com/v1alpha1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      "createtest",
			Namespace: testNamespace,
		},
		Spec: v1alpha1.CatalogSourceConfigSpec{
			TargetNamespace: targetNamespace.Name,
		},
	}
	err = dclient.Create(goctx.TODO(), testCatalogSourceConfig)
	if err != nil {
		t.Fatal(err)
	}

	ctx.AddFinalizerFn(func() error {
		return dclient.Delete(goctx.TODO(), testCatalogSourceConfig)
	})

	kclient := test.Global.KubeClient
	expectedConfigMap := v1alpha1.ConfigMapPrefix + testCatalogSourceConfigName
	// Check if the expected ConfigMap was created
	err = WaitForConfigMap(t, kclient, getTestNamespace(t), "marketplace-operator", time.Second*5, time.Minute*30)
	if err != nil {
		t.Fatalf("Expected ConfigMap %s was not created in %s namespace", expectedConfigMap, targetNamespace.Name)
	}
}

func getTestNamespace(t *testing.T) string {
	// Get the namespace where the operator is running
	namespace, err := ctx.GetNamespace()
	if err != nil {
		t.Fatal(err)
	}
	return namespace
}

// initializeClusterResources initializes the required Kubernetes resources required for the Marketplace operator and
// deploys the operator
func initializeClusterResources(t *testing.T) {
	err := ctx.InitializeClusterResources()
	if err != nil {
		t.Fatalf("failed to initialize cluster resources: %v", err)
	}

	// Wait for marketplace-operator to be ready
	err = e2eutil.WaitForDeployment(t, test.Global.KubeClient, getTestNamespace(t), "marketplace-operator", 1,
		time.Second*5, time.Second*30)
	if err != nil {
		t.Fatal(err)
	}
}

// initializeFramework registers the CatalogSourceConfig scheme with the framework's dynamic client
func initializeFramework(t *testing.T) {
	catalogSourceConfigList := &v1alpha1.CatalogSourceConfigList{
		TypeMeta: metav1.TypeMeta{
			Kind:       "CatalogSourceConfig",
			APIVersion: "marketplace.redhat.com/v1alpha1",
		},
	}
	err := test.AddToFrameworkScheme(v1alpha1.AddToScheme, catalogSourceConfigList)
	if err != nil {
		t.Fatalf("failed to add custom resource scheme to framework: %v", err)
	}
}

func WaitForConfigMap(t *testing.T, kubeclient kubernetes.Interface, namespace, name string, retryInterval, timeout time.Duration) error {
	err := wait.Poll(retryInterval, timeout, func() (done bool, err error) {
		_, err = kubeclient.CoreV1().ConfigMaps(namespace).Get(name, metav1.GetOptions{IncludeUninitialized: true})
		if apierrors.IsNotFound(err) {
			if err != nil {
				t.Logf("Waiting for creation of %s ConfigMap\n", name)
				return false, nil
			}
			return false, err
		}
		return true, nil
	})
	if err != nil {
		return err
	}
	t.Logf("ConfigMap %s has been created\n", name)
	return nil
}
