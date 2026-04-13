/*
Copyright 2025 The PDB Operator Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package e2e

import (
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/pdb-operator/pdb-operator/test/utils"
)

// namespace where the project is deployed in
const namespace = "pdb-operator-system"

// serviceAccountName created for the project
const serviceAccountName = "pdb-operator-controller-manager"

// metricsServiceName is the name of the metrics service of the project
const metricsServiceName = "pdb-operator-controller-manager-metrics-service"

// metricsRoleBindingName is the name of the RBAC that will be created to allow get the metrics data
const metricsRoleBindingName = "pdb-operator-metrics-binding"

var _ = Describe("Manager", Ordered, func() {
	var controllerPodName string

	// Before running the tests, set up the environment by creating the namespace,
	// enforce the restricted security policy to the namespace, installing CRDs,
	// and deploying the controller.
	BeforeAll(func() {
		By("creating manager namespace")
		cmd := exec.Command("kubectl", "create", "ns", namespace)
		_, err := utils.Run(cmd)
		Expect(err).NotTo(HaveOccurred(), "Failed to create namespace")

		By("labeling the namespace to enforce the restricted security policy")
		cmd = exec.Command("kubectl", "label", "--overwrite", "ns", namespace,
			"pod-security.kubernetes.io/enforce=restricted")
		_, err = utils.Run(cmd)
		Expect(err).NotTo(HaveOccurred(), "Failed to label namespace with restricted policy")

		By("waiting for cert-manager webhook to be fully ready")
		verifyCertManagerWebhookReady := func(g Gomega) {
			cmd := exec.Command("kubectl", "get", "endpoints", "cert-manager-webhook",
				"-n", "cert-manager", "-o", "jsonpath={.subsets[0].addresses[0].ip}")
			output, err := utils.Run(cmd)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(output).NotTo(BeEmpty(), "cert-manager webhook endpoint not ready")
		}
		Eventually(verifyCertManagerWebhookReady).Should(Succeed())

		By("installing CRDs")
		cmd = exec.Command("make", "install")
		_, err = utils.Run(cmd)
		Expect(err).NotTo(HaveOccurred(), "Failed to install CRDs")

		By("deploying the controller-manager")
		cmd = exec.Command("make", "deploy", fmt.Sprintf("IMG=%s", projectImage))
		_, err = utils.Run(cmd)
		Expect(err).NotTo(HaveOccurred(), "Failed to deploy the controller-manager")

		By("removing the PDBPolicy mutating webhook to avoid connection refused errors")
		cmd = exec.Command("kubectl", "delete", "mutatingwebhookconfiguration",
			"pdb-operator-pdbpolicy-mutating-webhook-configuration",
			"--ignore-not-found")
		_, _ = utils.Run(cmd)
		cmd = exec.Command("kubectl", "delete", "validatingwebhookconfiguration",
			"pdb-operator-pdbpolicy-validating-webhook-configuration",
			"--ignore-not-found")
		_, _ = utils.Run(cmd)
	})

	// After all tests have been executed, clean up by undeploying the controller, uninstalling CRDs,
	// and deleting the namespace.
	AfterAll(func() {
		By("cleaning up the curl pod for metrics")
		cmd := exec.Command("kubectl", "delete", "pod", "curl-metrics", "-n", namespace)
		_, _ = utils.Run(cmd)

		By("undeploying the controller-manager")
		cmd = exec.Command("make", "undeploy")
		_, _ = utils.Run(cmd)

		By("cleaning up test StatefulSets to unblock CRD deletion")
		cmd = exec.Command("kubectl", "delete", "statefulsets", "--all", "-n", "default", "--timeout=30s", "--wait=false")
		_, _ = utils.Run(cmd)
		cmd = exec.Command("kubectl", "delete", "pdbpolicies", "--all", "--all-namespaces", "--timeout=30s", "--wait=false")
		_, _ = utils.Run(cmd)

		By("uninstalling CRDs")
		cmd = exec.Command("make", "uninstall")
		_, _ = utils.Run(cmd)

		By("removing manager namespace")
		cmd = exec.Command("kubectl", "delete", "ns", namespace, "--timeout=60s", "--wait=false")
		_, _ = utils.Run(cmd)
	})

	// After each test, check for failures and collect logs, events,
	// and pod descriptions for debugging.
	AfterEach(func() {
		specReport := CurrentSpecReport()
		if specReport.Failed() {
			By("Fetching controller manager pod logs")
			cmd := exec.Command("kubectl", "logs", controllerPodName, "-n", namespace)
			controllerLogs, err := utils.Run(cmd)
			if err == nil {
				_, _ = fmt.Fprintf(GinkgoWriter, "Controller logs:\n %s", controllerLogs)
			} else {
				_, _ = fmt.Fprintf(GinkgoWriter, "Failed to get Controller logs: %s", err)
			}

			By("Fetching Kubernetes events")
			cmd = exec.Command("kubectl", "get", "events", "-n", namespace, "--sort-by=.lastTimestamp")
			eventsOutput, err := utils.Run(cmd)
			if err == nil {
				_, _ = fmt.Fprintf(GinkgoWriter, "Kubernetes events:\n%s", eventsOutput)
			} else {
				_, _ = fmt.Fprintf(GinkgoWriter, "Failed to get Kubernetes events: %s", err)
			}

			By("Fetching curl-metrics logs")
			cmd = exec.Command("kubectl", "logs", "curl-metrics", "-n", namespace)
			metricsOutput, err := utils.Run(cmd)
			if err == nil {
				_, _ = fmt.Fprintf(GinkgoWriter, "Metrics logs:\n %s", metricsOutput)
			} else {
				_, _ = fmt.Fprintf(GinkgoWriter, "Failed to get curl-metrics logs: %s", err)
			}

			By("Fetching controller manager pod description")
			cmd = exec.Command("kubectl", "describe", "pod", controllerPodName, "-n", namespace)
			podDescription, err := utils.Run(cmd)
			if err == nil {
				fmt.Println("Pod description:\n", podDescription)
			} else {
				fmt.Println("Failed to describe controller pod")
			}
		}
	})

	SetDefaultEventuallyTimeout(2 * time.Minute)
	SetDefaultEventuallyPollingInterval(time.Second)

	Context("Manager", func() {
		It("should run successfully", func() {
			By("validating that the controller-manager pod is running as expected")
			verifyControllerUp := func(g Gomega) {
				// Get the name of the controller-manager pod
				cmd := exec.Command("kubectl", "get",
					"pods", "-l", "control-plane=controller-manager",
					"-o", "go-template={{ range .items }}"+
						"{{ if not .metadata.deletionTimestamp }}"+
						"{{ .metadata.name }}"+
						"{{ \"\\n\" }}{{ end }}{{ end }}",
					"-n", namespace,
				)

				podOutput, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred(), "Failed to retrieve controller-manager pod information")
				podNames := utils.GetNonEmptyLines(podOutput)
				// Deployment runs 2 replicas for high availability with leader election
				g.Expect(podNames).To(HaveLen(2), "expected 2 controller pods running")
				controllerPodName = podNames[0]
				g.Expect(controllerPodName).To(ContainSubstring("controller-manager"))

				// Validate the pod's status
				cmd = exec.Command("kubectl", "get",
					"pods", controllerPodName, "-o", "jsonpath={.status.phase}",
					"-n", namespace,
				)
				output, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(output).To(Equal("Running"), "Incorrect controller-manager pod status")
			}
			Eventually(verifyControllerUp).Should(Succeed())
		})

		It("should ensure the metrics endpoint is serving metrics", func() {
			By("creating a ClusterRoleBinding for the service account to allow access to metrics")
			cmd := exec.Command("kubectl", "create", "clusterrolebinding", metricsRoleBindingName,
				"--clusterrole=pdb-operator-metrics-reader",
				fmt.Sprintf("--serviceaccount=%s:%s", namespace, serviceAccountName),
			)
			_, err := utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred(), "Failed to create ClusterRoleBinding")

			By("validating that the metrics service is available")
			cmd = exec.Command("kubectl", "get", "service", metricsServiceName, "-n", namespace)
			_, err = utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred(), "Metrics service should exist")

			By("getting the service account token")
			token, err := serviceAccountToken()
			Expect(err).NotTo(HaveOccurred())
			Expect(token).NotTo(BeEmpty())

			By("waiting for the metrics endpoint to be ready")
			verifyMetricsEndpointReady := func(g Gomega) {
				cmd := exec.Command("kubectl", "get", "endpoints", metricsServiceName, "-n", namespace)
				output, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(output).To(ContainSubstring("8443"), "Metrics endpoint is not ready")
			}
			Eventually(verifyMetricsEndpointReady).Should(Succeed())

			By("verifying that the controller manager is serving the metrics server")
			verifyMetricsServerStarted := func(g Gomega) {
				cmd := exec.Command("kubectl", "logs", controllerPodName, "-n", namespace)
				output, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(output).To(ContainSubstring("Serving metrics server"),
					"Metrics server not yet started")
			}
			Eventually(verifyMetricsServerStarted).Should(Succeed())

			By("creating the curl-metrics pod to access the metrics endpoint")
			cmd = exec.Command("kubectl", "run", "curl-metrics", "--restart=Never",
				"--namespace", namespace,
				"--image=curlimages/curl:latest",
				"--overrides",
				fmt.Sprintf(`{
					"spec": {
						"containers": [{
							"name": "curl",
							"image": "curlimages/curl:latest",
							"command": ["/bin/sh", "-c"],
							"args": ["curl -v -k -H 'Authorization: Bearer %s' https://%s.%s.svc.cluster.local:8443/metrics"],
							"securityContext": {
								"allowPrivilegeEscalation": false,
								"capabilities": {
									"drop": ["ALL"]
								},
								"runAsNonRoot": true,
								"runAsUser": 1000,
								"seccompProfile": {
									"type": "RuntimeDefault"
								}
							}
						}],
						"serviceAccount": "%s"
					}
				}`, token, metricsServiceName, namespace, serviceAccountName))
			_, err = utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred(), "Failed to create curl-metrics pod")

			By("waiting for the curl-metrics pod to complete.")
			verifyCurlUp := func(g Gomega) {
				cmd := exec.Command("kubectl", "get", "pods", "curl-metrics",
					"-o", "jsonpath={.status.phase}",
					"-n", namespace)
				output, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(output).To(Equal("Succeeded"), "curl pod in wrong status")
			}
			Eventually(verifyCurlUp, 5*time.Minute).Should(Succeed())

			By("getting the metrics by checking curl-metrics logs")
			metricsOutput := getMetricsOutput()
			Expect(metricsOutput).To(ContainSubstring(
				"certwatcher_read_certificate_total",
			))
		})

		// +kubebuilder:scaffold:e2e-webhooks-checks

		// TODO: Customize the e2e test suite with scenarios specific to your project.
		// Consider applying sample/CR(s) and check their status and/or verifying
		// the reconciliation by using the metrics, i.e.:
		// metricsOutput := getMetricsOutput()
		// Expect(metricsOutput).To(ContainSubstring(
		//    fmt.Sprintf(`controller_runtime_reconcile_total{controller="%s",result="success"} 1`,
		//    strings.ToLower(<Kind>),
		// ))
	})

	Context("StatefulSet PDB management", func() {
		const testNamespace = "default"

		// dedent strips leading tabs from YAML heredocs so kubectl can parse them.
		dedent := func(s string) string {
			return strings.ReplaceAll(s, "\t", "")
		}

		// cleanupStatefulSet removes a StatefulSet and its PDB, ignoring not-found errors.
		cleanupStatefulSet := func(name string) {
			cmd := exec.Command("kubectl", "delete", "statefulset", name, "-n", testNamespace,
				"--ignore-not-found", "--wait=false", "--grace-period=0")
			_, _ = utils.Run(cmd)
			cmd = exec.Command("kubectl", "delete", "pdb", name+"-pdb", "-n", testNamespace,
				"--ignore-not-found", "--wait=false")
			_, _ = utils.Run(cmd)
			cmd = exec.Command("kubectl", "delete", "pdbpolicy", name+"-policy", "-n", testNamespace,
				"--ignore-not-found", "--wait=false")
			_, _ = utils.Run(cmd)
		}

		It("should create a PDB when a StatefulSet matches a PDBPolicy", func() {
			const stsName = "e2e-sts-policy"
			cleanupStatefulSet(stsName)
			DeferCleanup(func() { cleanupStatefulSet(stsName) })

			By("creating a PDBPolicy that selects the StatefulSet by label")
			policyYAML := fmt.Sprintf(`
apiVersion: availability.pdboperator.io/v1alpha1
kind: PDBPolicy
metadata:
  name: %s-policy
  namespace: %s
spec:
  availabilityClass: high-availability
  workloadSelector:
    matchLabels:
      app: %s
  priority: 10
`, stsName, testNamespace, stsName)
			cmd := exec.Command("kubectl", "apply", "-f", "-")
			cmd.Stdin = strings.NewReader(dedent(policyYAML))
			_, err := utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred(), "Failed to create PDBPolicy")

			By("creating a StatefulSet with 3 replicas")
			stsYAML := fmt.Sprintf(`
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: %s
  namespace: %s
  annotations:
    pdboperator.io/availability-class: high-availability
  labels:
    app: %s
spec:
  replicas: 3
  selector:
    matchLabels:
      app: %s
  template:
    metadata:
      labels:
        app: %s
    spec:
      containers:
      - name: app
        image: nginx:alpine
`, stsName, testNamespace, stsName, stsName, stsName)
			cmd = exec.Command("kubectl", "apply", "-f", "-")
			cmd.Stdin = strings.NewReader(dedent(stsYAML))
			_, err = utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred(), "Failed to create StatefulSet")

			By("waiting for the PDB to be created")
			verifyPDBCreated := func(g Gomega) {
				cmd := exec.Command("kubectl", "get", "pdb", stsName+"-pdb",
					"-n", testNamespace,
					"-o", "jsonpath={.spec.minAvailable}")
				output, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred(), "PDB should exist")
				g.Expect(output).To(Equal("75%"), "high-availability should set minAvailable to 75%%")
			}
			Eventually(verifyPDBCreated).Should(Succeed())

			By("verifying PDB owner reference points to the StatefulSet")
			cmd = exec.Command("kubectl", "get", "pdb", stsName+"-pdb",
				"-n", testNamespace,
				"-o", "jsonpath={.metadata.ownerReferences[0].kind}")
			output, err := utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred())
			Expect(output).To(Equal("StatefulSet"))
		})

		It("should skip PDB creation for a StatefulSet with fewer than 2 replicas", func() {
			const stsName = "e2e-sts-single"
			cleanupStatefulSet(stsName)
			DeferCleanup(func() { cleanupStatefulSet(stsName) })

			By("creating a StatefulSet with 1 replica")
			stsYAML := fmt.Sprintf(`
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: %s
  namespace: %s
  annotations:
    pdboperator.io/availability-class: standard
  labels:
    app: %s
spec:
  replicas: 1
  selector:
    matchLabels:
      app: %s
  template:
    metadata:
      labels:
        app: %s
    spec:
      containers:
      - name: app
        image: nginx:alpine
`, stsName, testNamespace, stsName, stsName, stsName)
			cmd := exec.Command("kubectl", "apply", "-f", "-")
			cmd.Stdin = strings.NewReader(dedent(stsYAML))
			_, err := utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred(), "Failed to create single-replica StatefulSet")

			By("confirming no PDB is created after a reconciliation window")
			Consistently(func(g Gomega) {
				cmd := exec.Command("kubectl", "get", "pdb", stsName+"-pdb",
					"-n", testNamespace)
				_, err := utils.Run(cmd)
				g.Expect(err).To(HaveOccurred(), "PDB should not exist for single-replica StatefulSet")
			}, 15*time.Second, 3*time.Second).Should(Succeed())
		})

		It("should delete the PDB when the StatefulSet is deleted", func() {
			const stsName = "e2e-sts-delete"
			cleanupStatefulSet(stsName)
			DeferCleanup(func() { cleanupStatefulSet(stsName) })

			By("creating a StatefulSet with availability annotation")
			stsYAML := fmt.Sprintf(`
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: %s
  namespace: %s
  annotations:
    pdboperator.io/availability-class: standard
  labels:
    app: %s
spec:
  replicas: 3
  selector:
    matchLabels:
      app: %s
  template:
    metadata:
      labels:
        app: %s
    spec:
      containers:
      - name: app
        image: nginx:alpine
`, stsName, testNamespace, stsName, stsName, stsName)
			cmd := exec.Command("kubectl", "apply", "-f", "-")
			cmd.Stdin = strings.NewReader(dedent(stsYAML))
			_, err := utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred())

			By("waiting for the PDB to be created")
			Eventually(func(g Gomega) {
				cmd := exec.Command("kubectl", "get", "pdb", stsName+"-pdb", "-n", testNamespace)
				_, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred(), "PDB should exist before deletion test")
			}).Should(Succeed())

			By("deleting the StatefulSet")
			cmd = exec.Command("kubectl", "delete", "statefulset", stsName, "-n", testNamespace)
			_, err = utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred())

			By("waiting for the PDB to be cleaned up")
			Eventually(func(g Gomega) {
				cmd := exec.Command("kubectl", "get", "pdb", stsName+"-pdb", "-n", testNamespace)
				_, err := utils.Run(cmd)
				g.Expect(err).To(HaveOccurred(), "PDB should be deleted after StatefulSet deletion")
			}).Should(Succeed())
		})

		It("should respect strict enforcement - annotation override blocked", func() {
			const stsName = "e2e-sts-strict"
			cleanupStatefulSet(stsName)
			DeferCleanup(func() { cleanupStatefulSet(stsName) })

			By("creating a strict PDBPolicy")
			policyYAML := fmt.Sprintf(`
apiVersion: availability.pdboperator.io/v1alpha1
kind: PDBPolicy
metadata:
  name: %s-policy
  namespace: %s
spec:
  availabilityClass: mission-critical
  enforcement: strict
  workloadSelector:
    matchLabels:
      app: %s
  priority: 100
`, stsName, testNamespace, stsName)
			cmd := exec.Command("kubectl", "apply", "-f", "-")
			cmd.Stdin = strings.NewReader(dedent(policyYAML))
			_, err := utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred())

			By("creating a StatefulSet with a lower annotation override")
			stsYAML := fmt.Sprintf(`
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: %s
  namespace: %s
  annotations:
    pdboperator.io/availability-class: non-critical
  labels:
    app: %s
spec:
  replicas: 3
  selector:
    matchLabels:
      app: %s
  template:
    metadata:
      labels:
        app: %s
    spec:
      containers:
      - name: app
        image: nginx:alpine
`, stsName, testNamespace, stsName, stsName, stsName)
			cmd = exec.Command("kubectl", "apply", "-f", "-")
			cmd.Stdin = strings.NewReader(dedent(stsYAML))
			_, err = utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred())

			By("verifying the PDB uses mission-critical (policy wins over annotation)")
			Eventually(func(g Gomega) {
				cmd := exec.Command("kubectl", "get", "pdb", stsName+"-pdb",
					"-n", testNamespace,
					"-o", "jsonpath={.spec.minAvailable}")
				output, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(output).To(Equal("90%"), "strict policy should override annotation - mission-critical = 90%%")
			}).Should(Succeed())
		})
	})
})

// serviceAccountToken returns a token for the specified service account in the given namespace.
// It uses the Kubernetes TokenRequest API to generate a token by directly sending a request
// and parsing the resulting token from the API response.
func serviceAccountToken() (string, error) {
	const tokenRequestRawString = `{
		"apiVersion": "authentication.k8s.io/v1",
		"kind": "TokenRequest"
	}`

	// Temporary file to store the token request
	secretName := fmt.Sprintf("%s-token-request", serviceAccountName)
	tokenRequestFile := filepath.Join("/tmp", secretName)
	err := os.WriteFile(tokenRequestFile, []byte(tokenRequestRawString), os.FileMode(0o644))
	if err != nil {
		return "", err
	}

	var out string
	verifyTokenCreation := func(g Gomega) {
		// Execute kubectl command to create the token
		cmd := exec.Command("kubectl", "create", "--raw", fmt.Sprintf(
			"/api/v1/namespaces/%s/serviceaccounts/%s/token",
			namespace,
			serviceAccountName,
		), "-f", tokenRequestFile)

		output, err := cmd.CombinedOutput()
		g.Expect(err).NotTo(HaveOccurred())

		// Parse the JSON output to extract the token
		var token tokenRequest
		err = json.Unmarshal(output, &token)
		g.Expect(err).NotTo(HaveOccurred())

		out = token.Status.Token
	}
	Eventually(verifyTokenCreation).Should(Succeed())

	return out, err
}

// getMetricsOutput retrieves and returns the logs from the curl pod used to access the metrics endpoint.
func getMetricsOutput() string {
	By("getting the curl-metrics logs")
	cmd := exec.Command("kubectl", "logs", "curl-metrics", "-n", namespace)
	metricsOutput, err := utils.Run(cmd)
	Expect(err).NotTo(HaveOccurred(), "Failed to retrieve logs from curl pod")
	Expect(metricsOutput).To(ContainSubstring("< HTTP/1.1 200 OK"))
	return metricsOutput
}

// tokenRequest is a simplified representation of the Kubernetes TokenRequest API response,
// containing only the token field that we need to extract.
type tokenRequest struct {
	Status struct {
		Token string `json:"token"`
	} `json:"status"`
}
