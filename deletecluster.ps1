#This script deletes a cluster. You need to provide the cluster name and the associated subscription.

Remove-AzureHDInsightCluster -Name "cluster_name" -Subscription "subscription_name"