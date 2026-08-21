# Dedicated node pool (decision q20)

ad-scheduler only manages a dedicated pool of nodes. A node joins the pool by
carrying **both**:

- the membership **label** `scheduler.arenadata.io/name=ad-scheduler` — the
  source of truth for cluster capacity (a label-filtered informer, M1); and
- the exclusivity **taint** `scheduler.arenadata.io/dedicated=ad-scheduler:NoSchedule`
  (`NoSchedule`, not `NoExecute` — CNI/CSI/kube-proxy DaemonSets stay).

Label without taint leaks exclusivity (other pods land); taint without label
means `capacity.Filter` rejects the node anyway. Both are required.

```sh
# label + taint a set of worker nodes (idempotent)
kubectl label nodes -l node-role.kubernetes.io/worker='' \
  scheduler.arenadata.io/name=ad-scheduler --overwrite
kubectl taint nodes -l node-role.kubernetes.io/worker='' \
  scheduler.arenadata.io/dedicated=ad-scheduler:NoSchedule --overwrite

# verify the pool
kubectl get nodes -l scheduler.arenadata.io/name=ad-scheduler \
  -o custom-columns=NODE:.metadata.name,TAINTS:.spec.taints
```

Pods land on the pool only if the submitter (or an in-tree MutatingAdmissionPolicy,
M6) sets `schedulerName: ad-scheduler` + a matching `nodeSelector` +
`toleration`.

The owner of the label/taint is a node-labeler controller or the provisioner —
never tenant-reachable RBAC.
