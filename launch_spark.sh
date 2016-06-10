aws emr create-cluster --name iPythonNotebookEMRDave \
--release-label emr-4.5.0 \
--use-default-roles \
--instance-groups InstanceGroupType=MASTER,InstanceCount=1,InstanceType=m3.xlarge,BidPrice=0.25 InstanceGroupType=CORE,InstanceCount="${1:-5}",InstanceType=m3.xlarge,BidPrice=0.25 \
--ec2-attributes KeyName=knife \
--applications Name=Hadoop Name=Spark --configurations https://s3.amazonaws.com/***/sparkConfig.json \
--bootstrap-action Path="s3://***/bootstrap/installboto.sh"