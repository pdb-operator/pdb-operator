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

		By("deleting LeaderWorkerSets while the operator can still process their finalizers")
		cmd = exec.Command("kubectl", "delete", "leaderworkersets", "--all", "-n", "default",
			"--ignore-not-found", "--timeout=60s")
		_, _ = utils.Run(cmd)

		By("deleting Workloads while the operator can still process their finalizers")
		cmd = exec.Command("kubectl", "delete", "workloads", "--all", "-n", "default",
			"--ignore-not-found", "--timeout=60s")
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

		It("should delete the PDB when the StatefulSet scales below 2 replicas", func() {
			const stsName = "e2e-sts-scaledown"
			cleanupStatefulSet(stsName)
			DeferCleanup(func() { cleanupStatefulSet(stsName) })
			verifyScaleDownCleansUpPDB("StatefulSet", "statefulset", stsName, testNamespace)
		})

		It("should block a node drain that would violate the StatefulSet PDB", func() {
			const stsName = "e2e-sts-drain"

			// removeDrainResources clears this spec's workload, PDB, Service, and PVCs; idempotent.
			removeDrainResources := func() {
				cleanupStatefulSet(stsName)
				cmd := exec.Command("kubectl", "delete", "service", stsName, "-n", testNamespace,
					"--ignore-not-found", "--wait=false")
				_, _ = utils.Run(cmd)
				cmd = exec.Command("kubectl", "delete", "pvc", "-l", "app="+stsName, "-n", testNamespace,
					"--ignore-not-found", "--wait=false")
				_, _ = utils.Run(cmd)
			}
			removeDrainResources()

			By("getting the node name to drain")
			cmd := exec.Command("kubectl", "get", "nodes",
				"-o", "jsonpath={.items[0].metadata.name}")
			nodeName, err := utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred(), "Failed to get node name")
			nodeName = strings.TrimSpace(nodeName)
			Expect(nodeName).NotTo(BeEmpty(), "node name must not be empty")

			// Cleanup restores the node and removes the workload, service, and PVCs even on failure.
			DeferCleanup(func() {
				By("uncordoning the node")
				// Retry and confirm the node is schedulable so a stuck cordon cannot cascade into later specs.
				verifyNodeSchedulable := func(g Gomega) {
					cmd := exec.Command("kubectl", "uncordon", nodeName)
					_, err := utils.Run(cmd)
					g.Expect(err).NotTo(HaveOccurred(), "Failed to uncordon node")
					cmd = exec.Command("kubectl", "get", "node", nodeName,
						"-o", "jsonpath={.spec.unschedulable}")
					output, err := utils.Run(cmd)
					g.Expect(err).NotTo(HaveOccurred())
					g.Expect(output).To(BeEmpty(), "node should be schedulable after uncordon")
				}
				Eventually(verifyNodeSchedulable).Should(Succeed())
				removeDrainResources()
			})

			By("creating a headless Service for the StatefulSet")
			svcYAML := fmt.Sprintf(`
apiVersion: v1
kind: Service
metadata:
  name: %s
  namespace: %s
  labels:
    app: %s
spec:
  clusterIP: None
  selector:
    app: %s
  ports:
  - name: redis
    port: 6379
`, stsName, testNamespace, stsName, stsName)
			cmd = exec.Command("kubectl", "apply", "-f", "-")
			cmd.Stdin = strings.NewReader(dedent(svcYAML))
			_, err = utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred(), "Failed to create headless Service")

			By("creating a StatefulSet with serviceName, volumeClaimTemplates, and a readiness probe")
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
  serviceName: %s
  podManagementPolicy: Parallel
  replicas: 3
  selector:
    matchLabels:
      app: %s
  template:
    metadata:
      labels:
        app: %s
    spec:
      terminationGracePeriodSeconds: 5
      containers:
      - name: redis
        image: redis:alpine
        ports:
        - name: redis
          containerPort: 6379
        readinessProbe:
          tcpSocket:
            port: 6379
          initialDelaySeconds: 2
          periodSeconds: 3
        volumeMounts:
        - name: data
          mountPath: /data
  volumeClaimTemplates:
  - metadata:
      name: data
      labels:
        app: %s
    spec:
      accessModes: ["ReadWriteOnce"]
      storageClassName: standard
      resources:
        requests:
          storage: 128Mi
`, stsName, testNamespace, stsName, stsName, stsName, stsName, stsName)
			cmd = exec.Command("kubectl", "apply", "-f", "-")
			cmd.Stdin = strings.NewReader(dedent(stsYAML))
			_, err = utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred(), "Failed to create StatefulSet")

			By("waiting for all replicas to be Ready")
			cmd = exec.Command("kubectl", "rollout", "status", "statefulset", stsName,
				"-n", testNamespace, "--timeout=180s")
			_, err = utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred(), "StatefulSet replicas did not become Ready")

			By("waiting for the PDB to report zero allowed disruptions")
			// high-availability = 75% minAvailable; 3 replicas leaves no room for a voluntary eviction.
			Eventually(func(g Gomega) {
				cmd := exec.Command("kubectl", "get", "pdb", stsName+"-pdb",
					"-n", testNamespace,
					"-o", "jsonpath={.status.disruptionsAllowed}")
				output, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred(), "PDB should exist")
				g.Expect(output).To(Equal("0"), "PDB should allow no disruptions at 75% of 3 replicas")
			}).Should(Succeed())

			By("draining the node and asserting the PDB blocks eviction")
			// Scope the drain to this StatefulSet so the single-node kind cluster stays usable.
			cmd = exec.Command("kubectl", "drain", nodeName,
				"--ignore-daemonsets", "--delete-emptydir-data",
				"--pod-selector", "app="+stsName, "--timeout=90s")
			output, err := utils.Run(cmd)
			Expect(err).To(HaveOccurred(), "drain should fail because the PDB blocks eviction")
			Expect(output).To(ContainSubstring("disruption budget"),
				"drain should report a PodDisruptionBudget violation")
		})
	})

	Context("Deployment PDB management", func() {
		const testNamespace = "default"

		// cleanupDeployment removes a Deployment and its PDB, ignoring not-found errors.
		cleanupDeployment := func(name string) {
			cmd := exec.Command("kubectl", "delete", "deployment", name, "-n", testNamespace,
				"--ignore-not-found", "--wait=false", "--grace-period=0")
			_, _ = utils.Run(cmd)
			cmd = exec.Command("kubectl", "delete", "pdb", name+"-pdb", "-n", testNamespace,
				"--ignore-not-found", "--wait=false")
			_, _ = utils.Run(cmd)
		}

		It("should delete the PDB when the Deployment scales below 2 replicas", func() {
			const deployName = "e2e-deploy-scaledown"
			cleanupDeployment(deployName)
			DeferCleanup(func() { cleanupDeployment(deployName) })
			verifyScaleDownCleansUpPDB("Deployment", "deployment", deployName, testNamespace)
		})
	})

	Context("LeaderWorkerSet PDB management", func() {
		const testNamespace = "default"
		const lwsNameLabel = "leaderworkerset.sigs.k8s.io/name"
		const lwsGroupLabel = "leaderworkerset.sigs.k8s.io/group-index"

		// dedent strips leading tabs from YAML heredocs so kubectl can parse them.
		dedent := func(s string) string {
			return strings.ReplaceAll(s, "\t", "")
		}

		// cleanupLWS removes a LeaderWorkerSet, its PDB, and its policy; idempotent.
		// The LWS delete blocks so the operator can process the PDB-cleanup finalizer.
		cleanupLWS := func(name string) {
			cmd := exec.Command("kubectl", "delete", "leaderworkerset", name, "-n", testNamespace,
				"--ignore-not-found", "--timeout=60s")
			_, _ = utils.Run(cmd)
			cmd = exec.Command("kubectl", "delete", "pdb", name+"-pdb", "-n", testNamespace,
				"--ignore-not-found", "--wait=false")
			_, _ = utils.Run(cmd)
			cmd = exec.Command("kubectl", "delete", "pdbpolicy", name+"-policy", "-n", testNamespace,
				"--ignore-not-found", "--wait=false")
			_, _ = utils.Run(cmd)
		}

		// readyStatuses returns the Ready condition status of every pod of the LWS.
		readyStatuses := func(g Gomega, name string) []string {
			cmd := exec.Command("kubectl", "get", "pods",
				"-l", lwsNameLabel+"="+name, "-n", testNamespace,
				"-o", "jsonpath={.items[*].status.conditions[?(@.type==\"Ready\")].status}")
			output, err := utils.Run(cmd)
			g.Expect(err).NotTo(HaveOccurred())
			return strings.Fields(output)
		}

		waitAllPodsReady := func(name string, count int) {
			Eventually(func(g Gomega) {
				statuses := readyStatuses(g, name)
				g.Expect(statuses).To(HaveLen(count))
				for _, s := range statuses {
					g.Expect(s).To(Equal("True"))
				}
			}, 5*time.Minute).Should(Succeed())
		}

		disruptionsAllowed := func(g Gomega, name string) string {
			cmd := exec.Command("kubectl", "get", "pdb", name+"-pdb", "-n", testNamespace,
				"-o", "jsonpath={.status.disruptionsAllowed}")
			output, err := utils.Run(cmd)
			g.Expect(err).NotTo(HaveOccurred())
			return output
		}

		It("should quantize the PDB to whole groups and gate evictions at group granularity", func() {
			const lwsName = "e2e-lws-gang"
			cleanupLWS(lwsName)
			DeferCleanup(func() { cleanupLWS(lwsName) })

			By("creating a mission-critical PDBPolicy selecting the LeaderWorkerSet")
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
`, lwsName, testNamespace, lwsName)
			cmd := exec.Command("kubectl", "apply", "-f", "-")
			cmd.Stdin = strings.NewReader(dedent(policyYAML))
			_, err := utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred(), "Failed to create PDBPolicy")

			By("creating a LeaderWorkerSet with 4 groups of size 2 and slow-ready pods")
			// sleep-then-touch readiness simulates model load so group recovery is observable
			lwsYAML := fmt.Sprintf(`
apiVersion: leaderworkerset.x-k8s.io/v1
kind: LeaderWorkerSet
metadata:
  name: %s
  namespace: %s
  labels:
    app: %s
spec:
  replicas: 4
  leaderWorkerTemplate:
    size: 2
    restartPolicy: RecreateGroupOnPodRestart
    workerTemplate:
      metadata:
        labels:
          app: %s
      spec:
        terminationGracePeriodSeconds: 3
        containers:
        - name: model
          image: busybox:1.37
          command: ["sh", "-c", "sleep 15; touch /tmp/ready; sleep infinity"]
          readinessProbe:
            exec:
              command: ["cat", "/tmp/ready"]
            periodSeconds: 2
          resources:
            requests:
              cpu: 10m
              memory: 16Mi
`, lwsName, testNamespace, lwsName, lwsName)
			cmd = exec.Command("kubectl", "apply", "-f", "-")
			cmd.Stdin = strings.NewReader(dedent(lwsYAML))
			_, err = utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred(), "Failed to create LeaderWorkerSet")

			By("waiting for the group-quantized PDB")
			// mission-critical (90%) over 4 groups: ceil(0.9*4)=4, clamped to 3 groups x 2 pods = 6
			Eventually(func(g Gomega) {
				cmd := exec.Command("kubectl", "get", "pdb", lwsName+"-pdb", "-n", testNamespace,
					"-o", "jsonpath={.spec.minAvailable}")
				output, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred(), "PDB should exist")
				g.Expect(output).To(Equal("6"), "minAvailable should be quantized to 3 groups x 2 pods")
			}).Should(Succeed())

			By("verifying the PDB selects pods by the LWS name label")
			cmd = exec.Command("kubectl", "get", "pdb", lwsName+"-pdb", "-n", testNamespace,
				"-o", "jsonpath={.spec.selector.matchLabels['leaderworkerset\\.sigs\\.k8s\\.io/name']}")
			output, err := utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred())
			Expect(output).To(Equal(lwsName))

			By("verifying the StatefulSet controller created no PDBs for LWS-internal StatefulSets")
			// the leader StatefulSet shares the LWS name, so an unskipped STS would overwrite this PDB
			Consistently(func(g Gomega) {
				cmd := exec.Command("kubectl", "get", "pdb", "-n", testNamespace, "-o", "name")
				output, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred())
				var lwsPDBs []string
				for _, line := range utils.GetNonEmptyLines(output) {
					if strings.Contains(line, lwsName) {
						lwsPDBs = append(lwsPDBs, line)
					}
				}
				g.Expect(lwsPDBs).To(ConsistOf("poddisruptionbudget.policy/" + lwsName + "-pdb"))

				cmd = exec.Command("kubectl", "get", "pdb", lwsName+"-pdb", "-n", testNamespace,
					"-o", "jsonpath={.metadata.ownerReferences[0].kind} {.spec.minAvailable}")
				output, err = utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(output).To(Equal("LeaderWorkerSet 6"))
			}, 15*time.Second, 3*time.Second).Should(Succeed())

			By("waiting for all 8 pods to become Ready")
			waitAllPodsReady(lwsName, 8)

			By("waiting for the PDB to allow exactly one group of disruptions")
			Eventually(func(g Gomega) {
				g.Expect(disruptionsAllowed(g, lwsName)).To(Equal("2"))
			}).Should(Succeed())

			By("evicting every pod of group 0 in one pass")
			cmd = exec.Command("kubectl", "get", "pods",
				"-l", lwsNameLabel+"="+lwsName+","+lwsGroupLabel+"=0",
				"-n", testNamespace, "-o", "jsonpath={.items[*].metadata.name}")
			output, err = utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred())
			groupZeroPods := strings.Fields(output)
			Expect(groupZeroPods).To(HaveLen(2), "group 0 should have leader + 1 worker")
			for _, pod := range groupZeroPods {
				_, err := evictPod(pod)
				Expect(err).NotTo(HaveOccurred(), "eviction of %s should be admitted", pod)
			}

			By("waiting for the budget to be exhausted while group 0 reloads")
			Eventually(func(g Gomega) {
				g.Expect(disruptionsAllowed(g, lwsName)).To(Equal("0"))
			}, 30*time.Second).Should(Succeed())

			By("asserting an eviction from another group is rejected while group 0 reloads")
			cmd = exec.Command("kubectl", "get", "pods",
				"-l", lwsNameLabel+"="+lwsName+","+lwsGroupLabel+"=1",
				"-n", testNamespace, "-o", "jsonpath={.items[0].metadata.name}")
			output, err = utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred())
			groupOnePod := strings.TrimSpace(output)
			Expect(groupOnePod).NotTo(BeEmpty())
			evictOutput, err := evictPod(groupOnePod)
			Expect(err).To(HaveOccurred(), "eviction should be rejected while the budget is exhausted")
			Expect(evictOutput).To(ContainSubstring("disruption budget"))

			By("waiting for group 0 to reload and the budget to recover")
			waitAllPodsReady(lwsName, 8)
			Eventually(func(g Gomega) {
				g.Expect(disruptionsAllowed(g, lwsName)).To(Equal("2"))
			}).Should(Succeed())

			By("evicting the group-1 pod now that the budget has recovered")
			_, err = evictPod(groupOnePod)
			Expect(err).NotTo(HaveOccurred(), "eviction should succeed after recovery")
		})

		It("should skip PDB creation for a single-group LeaderWorkerSet and emit a warning", func() {
			const lwsName = "e2e-lws-single"
			cleanupLWS(lwsName)
			DeferCleanup(func() { cleanupLWS(lwsName) })

			By("creating a single-group LeaderWorkerSet")
			lwsYAML := fmt.Sprintf(`
apiVersion: leaderworkerset.x-k8s.io/v1
kind: LeaderWorkerSet
metadata:
  name: %s
  namespace: %s
  annotations:
    pdboperator.io/availability-class: high-availability
spec:
  replicas: 1
  leaderWorkerTemplate:
    size: 2
    workerTemplate:
      spec:
        terminationGracePeriodSeconds: 3
        containers:
        - name: app
          image: busybox:1.37
          command: ["sh", "-c", "sleep infinity"]
          resources:
            requests:
              cpu: 10m
              memory: 16Mi
`, lwsName, testNamespace)
			cmd := exec.Command("kubectl", "apply", "-f", "-")
			cmd.Stdin = strings.NewReader(dedent(lwsYAML))
			_, err := utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred(), "Failed to create single-group LeaderWorkerSet")

			By("waiting for the LeaderWorkerSetSkipped warning event")
			Eventually(func(g Gomega) {
				cmd := exec.Command("kubectl", "get", "events", "-n", testNamespace)
				output, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred())
				found := false
				for _, line := range utils.GetNonEmptyLines(output) {
					if strings.Contains(line, "LeaderWorkerSetSkipped") && strings.Contains(line, lwsName) {
						found = true
					}
				}
				g.Expect(found).To(BeTrue(), "expected a LeaderWorkerSetSkipped event for %s", lwsName)
			}).Should(Succeed())

			By("confirming no PDB is created for the single group")
			Consistently(func(g Gomega) {
				cmd := exec.Command("kubectl", "get", "pdb", lwsName+"-pdb", "-n", testNamespace)
				_, err := utils.Run(cmd)
				g.Expect(err).To(HaveOccurred(), "PDB should not exist for a single-group LeaderWorkerSet")
			}, 15*time.Second, 3*time.Second).Should(Succeed())
		})
	})

	Context("Workload API gang PDB management", func() {
		const testNamespace = "default"

		// skipUnlessWorkloadAPIServed skips the spec on clusters without scheduling.k8s.io/v1beta1.
		skipUnlessWorkloadAPIServed := func() {
			cmd := exec.Command("kubectl", "api-resources", "--api-group=scheduling.k8s.io", "-o", "name")
			output, err := utils.Run(cmd)
			if err != nil || !strings.Contains(output, "workloads.scheduling.k8s.io") {
				Skip("Workload API (scheduling.k8s.io/v1beta1) not served on this cluster")
			}
		}

		// cleanupWorkload removes a fixture's pods, pod groups, Workload, PDB, and policy; idempotent.
		// The Workload delete blocks so the operator can process the PDB-cleanup finalizer.
		cleanupWorkload := func(name string) {
			cmd := exec.Command("kubectl", "delete", "pods", "-l", "app="+name, "-n", testNamespace,
				"--ignore-not-found", "--wait=false", "--grace-period=0")
			_, _ = utils.Run(cmd)
			cmd = exec.Command("kubectl", "delete", "workload", name, "-n", testNamespace,
				"--ignore-not-found", "--timeout=60s")
			_, _ = utils.Run(cmd)
			cmd = exec.Command("kubectl", "delete", "podgroups", "-l", "app="+name, "-n", testNamespace,
				"--ignore-not-found", "--wait=false")
			_, _ = utils.Run(cmd)
			cmd = exec.Command("kubectl", "delete", "pdb", name+"-pdb", "-n", testNamespace,
				"--ignore-not-found", "--wait=false")
			_, _ = utils.Run(cmd)
			cmd = exec.Command("kubectl", "delete", "pdbpolicy", name+"-policy", "-n", testNamespace,
				"--ignore-not-found", "--wait=false")
			_, _ = utils.Run(cmd)
		}

		// gangWorkloadYAML renders a gang Workload plus its pod groups and pause pods.
		gangWorkloadYAML := func(name string, groups, size int, annotationClass string) string {
			var b strings.Builder
			annotations := ""
			if annotationClass != "" {
				annotations = fmt.Sprintf("\n  annotations:\n    pdboperator.io/availability-class: %s", annotationClass)
			}
			fmt.Fprintf(&b, `apiVersion: scheduling.k8s.io/v1beta1
kind: Workload
metadata:
  name: %s
  namespace: %s
  labels:
    app: %s%s
spec:
  podGroupTemplates:
  - name: workers
    schedulingPolicy:
      gang:
        minCount: %d
    disruptionMode:
      all: {}
`, name, testNamespace, name, annotations, size)
			for g := 0; g < groups; g++ {
				pgName := fmt.Sprintf("%s-workers-%d", name, g)
				fmt.Fprintf(&b, `---
apiVersion: scheduling.k8s.io/v1beta1
kind: PodGroup
metadata:
  name: %s
  namespace: %s
  labels:
    app: %s
spec:
  workloadRef:
    workloadName: %s
    templateName: workers
  schedulingPolicy:
    gang:
      minCount: %d
  disruptionMode:
    all: {}
`, pgName, testNamespace, name, name, size)
				for p := 0; p < size; p++ {
					fmt.Fprintf(&b, `---
apiVersion: v1
kind: Pod
metadata:
  name: %s-%d
  namespace: %s
  labels:
    app: %s
spec:
  schedulingGroup:
    podGroupName: %s
  containers:
  - name: app
    image: registry.k8s.io/pause:3.10
    resources:
      requests:
        cpu: 10m
        memory: 16Mi
`, pgName, p, testNamespace, name, pgName)
				}
			}
			return b.String()
		}

		disruptionsAllowed := func(g Gomega, name string) string {
			cmd := exec.Command("kubectl", "get", "pdb", name+"-pdb", "-n", testNamespace,
				"-o", "jsonpath={.status.disruptionsAllowed}")
			output, err := utils.Run(cmd)
			g.Expect(err).NotTo(HaveOccurred())
			return output
		}

		It("should quantize the PDB to whole pod groups and gate evictions at group granularity", func() {
			skipUnlessWorkloadAPIServed()
			const wName = "e2e-wapi-gang"
			cleanupWorkload(wName)
			DeferCleanup(func() { cleanupWorkload(wName) })

			By("creating a mission-critical PDBPolicy selecting the Workload")
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
`, wName, testNamespace, wName)
			cmd := exec.Command("kubectl", "apply", "-f", "-")
			cmd.Stdin = strings.NewReader(strings.ReplaceAll(policyYAML, "\t", ""))
			_, err := utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred(), "Failed to create PDBPolicy")

			By("creating a gang Workload with 4 pod groups of 2 pause pods")
			cmd = exec.Command("kubectl", "apply", "-f", "-")
			cmd.Stdin = strings.NewReader(gangWorkloadYAML(wName, 4, 2, ""))
			_, err = utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred(), "Failed to create Workload fixture")

			By("waiting for the group-quantized PDB derived from the pod labels")
			// mission-critical (90%) over 4 groups: ceil(0.9*4)=4, clamped to 3 groups x 2 pods = 6
			Eventually(func(g Gomega) {
				cmd := exec.Command("kubectl", "get", "pdb", wName+"-pdb", "-n", testNamespace,
					"-o", "jsonpath={.spec.minAvailable} {.spec.selector.matchLabels.app} "+
						"{.metadata.ownerReferences[0].kind}")
				output, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred(), "PDB should exist")
				g.Expect(output).To(Equal("6 "+wName+" Workload"),
					"minAvailable should be quantized to 3 groups x 2 pods, selector derived from pod labels")
			}).Should(Succeed())

			By("waiting for the PDB to allow exactly one group of disruptions")
			Eventually(func(g Gomega) {
				g.Expect(disruptionsAllowed(g, wName)).To(Equal("2"))
			}).Should(Succeed())

			By("evicting both pods of group 0 in one pass")
			for _, pod := range []string{wName + "-workers-0-0", wName + "-workers-0-1"} {
				_, err := evictPod(pod)
				Expect(err).NotTo(HaveOccurred(), "eviction of %s should be admitted", pod)
			}

			By("waiting for the budget to be exhausted")
			Eventually(func(g Gomega) {
				g.Expect(disruptionsAllowed(g, wName)).To(Equal("0"))
			}, 30*time.Second).Should(Succeed())

			By("asserting an eviction from another group is rejected")
			evictOutput, err := evictPod(wName + "-workers-1-0")
			Expect(err).To(HaveOccurred(), "eviction should be rejected while the budget is exhausted")
			Expect(evictOutput).To(ContainSubstring("disruption budget"))
		})

		It("should skip PDB creation for a single-group Workload and emit a warning", func() {
			skipUnlessWorkloadAPIServed()
			const wName = "e2e-wapi-single"
			cleanupWorkload(wName)
			DeferCleanup(func() { cleanupWorkload(wName) })

			By("creating a single-group gang Workload")
			cmd := exec.Command("kubectl", "apply", "-f", "-")
			cmd.Stdin = strings.NewReader(gangWorkloadYAML(wName, 1, 2, "mission-critical"))
			_, err := utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred(), "Failed to create single-group Workload fixture")

			By("waiting for the single-group WorkloadSkipped warning event")
			Eventually(func(g Gomega) {
				cmd := exec.Command("kubectl", "get", "events", "-n", testNamespace)
				output, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred())
				found := false
				for _, line := range utils.GetNonEmptyLines(output) {
					if strings.Contains(line, "WorkloadSkipped") && strings.Contains(line, wName) &&
						strings.Contains(line, "restarts as a unit") {
						found = true
					}
				}
				g.Expect(found).To(BeTrue(), "expected a single-group WorkloadSkipped event for %s", wName)
			}).Should(Succeed())

			By("confirming no PDB is created for the single group")
			Consistently(func(g Gomega) {
				cmd := exec.Command("kubectl", "get", "pdb", wName+"-pdb", "-n", testNamespace)
				_, err := utils.Run(cmd)
				g.Expect(err).To(HaveOccurred(), "PDB should not exist for a single-group Workload")
			}, 15*time.Second, 3*time.Second).Should(Succeed())
		})
	})
})

// evictPod issues a policy/v1 Eviction for a pod in the default test namespace.
func evictPod(podName string) (string, error) {
	eviction := fmt.Sprintf(
		`{"apiVersion":"policy/v1","kind":"Eviction","metadata":{"name":%q,"namespace":"default"}}`,
		podName)
	cmd := exec.Command("kubectl", "create", "--raw",
		fmt.Sprintf("/api/v1/namespaces/default/pods/%s/eviction", podName),
		"-f", "-")
	cmd.Stdin = strings.NewReader(eviction)
	return utils.Run(cmd)
}

// verifyScaleDownCleansUpPDB creates a 3-replica workload of the given kind, waits for its
// PDB, scales it to 1, and asserts the orphaned PDB is cleaned up. kind is the API Kind
// (StatefulSet/Deployment); kindArg is the kubectl resource arg (statefulset/deployment).
func verifyScaleDownCleansUpPDB(kind, kindArg, name, namespace string) {
	By(fmt.Sprintf("creating a %s with 3 replicas", kind))
	workloadYAML := fmt.Sprintf(`
apiVersion: apps/v1
kind: %s
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
`, kind, name, namespace, name, name, name)
	cmd := exec.Command("kubectl", "apply", "-f", "-")
	cmd.Stdin = strings.NewReader(strings.ReplaceAll(workloadYAML, "\t", ""))
	_, err := utils.Run(cmd)
	Expect(err).NotTo(HaveOccurred())

	By("waiting for the PDB to be created")
	Eventually(func(g Gomega) {
		cmd := exec.Command("kubectl", "get", "pdb", name+"-pdb", "-n", namespace)
		_, err := utils.Run(cmd)
		g.Expect(err).NotTo(HaveOccurred(), "PDB should exist before scale-down")
	}).Should(Succeed())

	By(fmt.Sprintf("scaling the %s down to 1 replica", kind))
	cmd = exec.Command("kubectl", "scale", kindArg, name, "-n", namespace, "--replicas=1")
	_, err = utils.Run(cmd)
	Expect(err).NotTo(HaveOccurred())

	By("waiting for the orphaned PDB to be cleaned up")
	Eventually(func(g Gomega) {
		cmd := exec.Command("kubectl", "get", "pdb", name+"-pdb", "-n", namespace)
		_, err := utils.Run(cmd)
		g.Expect(err).To(HaveOccurred(), "PDB should be deleted after scaling below 2 replicas")
	}).Should(Succeed())
}

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
