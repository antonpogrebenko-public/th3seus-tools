import boto3
from botocore.exceptions import ClientError

# Configuration
TARGET_REGION = 'us-east-1'  # Change this
BUCKET_TO_KEEP = 'shook.marketplace'  # Change this


def empty_bucket(s3_client, bucket_name):
	"""Empty all objects and versions from a bucket"""
	try:
		# Delete all object versions and delete markers
		paginator = s3_client.get_paginator('list_object_versions')
		for page in paginator.paginate(Bucket=bucket_name):
			objects_to_delete = []

			# Collect versions
			for version in page.get('Versions', []):
				objects_to_delete.append({
					'Key': version['Key'],
					'VersionId': version['VersionId']
				})

			# Collect delete markers
			for marker in page.get('DeleteMarkers', []):
				objects_to_delete.append({
					'Key': marker['Key'],
					'VersionId': marker['VersionId']
				})

			# Delete in batches
			if objects_to_delete:
				s3_client.delete_objects(
					Bucket=bucket_name,
					Delete={'Objects': objects_to_delete}
				)

		print(f"✓ Emptied bucket: {bucket_name}")
		return True
	except ClientError as e:
		print(f"✗ Error emptying {bucket_name}: {e}")
		return False


def main():
	s3_client = boto3.client('s3', region_name=TARGET_REGION)

	# Get all buckets
	response = s3_client.list_buckets()

	# Filter buckets in target region
	buckets_to_process = []
	for bucket in response['Buckets']:
		bucket_name = bucket['Name']

		# Skip the bucket to keep
		if bucket_name == BUCKET_TO_KEEP:
			print(f"⊘ Skipping: {bucket_name} (preserved)")
			continue

		# Check bucket region
		try:
			location = s3_client.get_bucket_location(Bucket=bucket_name)
			bucket_region = location['LocationConstraint'] or 'us-east-1'

			if bucket_region == TARGET_REGION:
				buckets_to_process.append(bucket_name)
		except ClientError as e:
			print(f"⚠ Could not check region for {bucket_name}: {e}")

	# Confirmation
	print(f"\n{'=' * 60}")
	print(f"Found {len(buckets_to_process)} bucket(s) in {TARGET_REGION} to delete:")
	for bucket in buckets_to_process:
		print(f"  - {bucket}")
	print(f"{'=' * 60}\n")

	confirm = input("Type 'DELETE' to proceed: ")
	if confirm != 'DELETE':
		print("Aborted.")
		return

	# Process each bucket
	for bucket_name in buckets_to_process:
		print(f"\nProcessing: {bucket_name}")

		# Empty the bucket
		if empty_bucket(s3_client, bucket_name):
			# Delete the bucket
			try:
				s3_client.delete_bucket(Bucket=bucket_name)
				print(f"✓ Deleted bucket: {bucket_name}")
			except ClientError as e:
				print(f"✗ Error deleting {bucket_name}: {e}")

	print("\n✓ Operation complete!")


if __name__ == '__main__':
	print("Jet")