"""Empty and delete named S3 buckets.

**Read this before changing the entrypoint.**

This script previously selected its targets by calling `list_buckets()` and
deleting *every* bucket in the region except one hardcoded name — and that name
(`shook.marketplace`) belongs to a different project entirely. Against this
account it would have destroyed all twenty `th3seus-*` buckets —
`th3seus-artifacts`, `th3seus-terrain`, `th3seus-assets`, `th3seus-uploads`,
`th3seus-hitl-releases`, both `th3seus-aero-*` buckets, the web asset buckets
and their playground twins — plus the unrelated `rfcalc-*`, `onboard-server-*`,
`sst-*` and CDK bootstrap buckets, behind a single `input("Type 'DELETE'")`.

It never ran: the entrypoint was `print("Jet")`, so `main()` was unreachable.
That is the only reason the account still has its data, and it is not a control.

The selection is now an explicit allowlist that is empty by default. There is no
enumeration: a bucket that is not named in `BUCKETS_TO_DELETE` cannot be
touched, whatever the region contains. Fill it in deliberately, run it, and
empty it again afterwards.
"""

import boto3
from botocore.exceptions import ClientError

# Region the named buckets live in.
TARGET_REGION = "us-east-1"

# Buckets to empty and delete. **Named explicitly, one per line.**
#
# Empty by default and meant to be left that way between uses. Never replace
# this with a listing call: the whole point is that a typo or a stale filter
# cannot reach a bucket nobody wrote down.
BUCKETS_TO_DELETE: tuple[str, ...] = ()

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
	if not BUCKETS_TO_DELETE:
		print("BUCKETS_TO_DELETE is empty — nothing to do.")
		print("Name the buckets explicitly in this file before running it.")
		return

	s3_client = boto3.client("s3", region_name=TARGET_REGION)

	# Verify each named bucket exists and is where we think it is, before
	# asking for confirmation — so the list the operator confirms is the list
	# that will actually be acted on.
	targets = []
	for bucket_name in BUCKETS_TO_DELETE:
		try:
			location = s3_client.get_bucket_location(Bucket=bucket_name)
			bucket_region = location["LocationConstraint"] or "us-east-1"
		except ClientError as e:
			print(f"⚠ Skipping {bucket_name}: {e}")
			continue
		if bucket_region != TARGET_REGION:
			print(f"⊘ Skipping {bucket_name}: in {bucket_region}, not {TARGET_REGION}")
			continue
		targets.append(bucket_name)

	if not targets:
		print("No named bucket is reachable in the target region. Nothing to do.")
		return

	print(f"\n{'=' * 60}")
	print(f"About to EMPTY AND DELETE {len(targets)} bucket(s) in {TARGET_REGION}:")
	for bucket in targets:
		print(f"  - {bucket}")
	print(f"{'=' * 60}\n")
	print("This cannot be undone. Versions and delete markers go too.")

	# The bucket names, not a fixed word: typing "DELETE" is muscle memory, and
	# this operation deserves the operator having read the list.
	expected = ",".join(targets)
	confirm = input(f"Type the bucket names to proceed ({expected}): ")
	if confirm.strip() != expected:
		print("Aborted.")
		return

	for bucket_name in targets:
		print(f"\nProcessing: {bucket_name}")
		if empty_bucket(s3_client, bucket_name):
			try:
				s3_client.delete_bucket(Bucket=bucket_name)
				print(f"✓ Deleted bucket: {bucket_name}")
			except ClientError as e:
				print(f"✗ Error deleting {bucket_name}: {e}")

	print("\n✓ Operation complete!")


if __name__ == "__main__":
	main()
