for bucket in $(aws s3api list-buckets --query "Buckets[].Name" --output text); do
  aws s3 rb s3://$bucket --force
done