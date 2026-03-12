import boto3
import time

from botocore.exceptions import WaiterError, ClientError
from redshift_etl.scripts.config_helper import get_config, get_config_path


# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------

def save_config(config, config_path):
    """Write the current config state back to the config file."""
    with open(config_path, 'w') as f:
        config.write(f)


# ---------------------------------------------------------------------------
# Redshift Cluster
# ---------------------------------------------------------------------------

def delete_redshift_cluster(redshift, config, config_path):
    """Delete the Redshift cluster and clear HOST in dwh.cfg.

    Idempotent – if the cluster does not exist, this is a no-op.
    Skips creating a final snapshot to allow a clean teardown.
    """
    cluster_id = config.get('CLUSTER', 'CLUSTER_IDENTIFIER')

    try:
        redshift.delete_cluster(
            ClusterIdentifier=cluster_id,
            SkipFinalClusterSnapshot=True,
        )
        print(f"Redshift cluster '{cluster_id}' deletion initiated.")
    except ClientError as e:
        if e.response['Error']['Code'] == 'ClusterNotFound':
            print(f"Redshift cluster '{cluster_id}' does not exist. Skipping.")
            config.set('CLUSTER', 'HOST', '')
            save_config(config, config_path)
            return
        if e.response['Error']['Code'] == 'InvalidClusterState':
            print(f"Cluster '{cluster_id}' is already being deleted. Waiting...")
        else:
            raise

    # --- Wait for the cluster to be fully deleted ---
    print(f"Waiting for cluster '{cluster_id}' to be deleted "
          f"(this may take several minutes)...")
    try:
        redshift.get_waiter('cluster_deleted').wait(
            ClusterIdentifier=cluster_id,
            WaiterConfig={'Delay': 30, 'MaxAttempts': 60},
        )
        print(f"Redshift cluster '{cluster_id}' has been deleted.")
    except WaiterError as e:
        print(f"Timed out waiting for cluster '{cluster_id}' deletion: {e}")
        raise

    config.set('CLUSTER', 'HOST', '')
    save_config(config, config_path)
    print("Cleared HOST in dwh.cfg.")


# ---------------------------------------------------------------------------
# Security Group
# ---------------------------------------------------------------------------

def delete_security_group(ec2, config, config_path):
    """Delete the Redshift security group and clear SG_ID in dwh.cfg.

    Idempotent – if the security group does not exist, this is a no-op.
    """
    sg_id = config.get('SECURITY_GROUP', 'SG_ID')

    if not sg_id:
        print("No Security Group ID in dwh.cfg. Skipping.")
        return

    try:
        ec2.delete_security_group(GroupId=sg_id)
        print(f"Security Group '{sg_id}' deleted.")
    except ClientError as e:
        if e.response['Error']['Code'] == 'InvalidGroup.NotFound':
            print(f"Security Group '{sg_id}' does not exist. Skipping.")
        else:
            raise

    config.set('SECURITY_GROUP', 'SG_ID', '')
    save_config(config, config_path)
    print("Cleared SG_ID in dwh.cfg.")


# ---------------------------------------------------------------------------
# IAM Role
# ---------------------------------------------------------------------------

def delete_iam_role(iam, config, config_path):
    """Detach policies from and delete the IAM role, then clear ROLE_ARN.

    Idempotent – if the role does not exist, this is a no-op.
    All attached managed policies are detached before deletion (required by IAM).
    """
    role_name = config.get('IAM_ROLE', 'ROLE_NAME')

    # --- Check if the role exists ---
    try:
        iam.get_role(RoleName=role_name)
    except ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchEntity':
            print(f"IAM Role '{role_name}' does not exist. Skipping.")
            config.set('IAM_ROLE', 'ROLE_ARN', '')
            save_config(config, config_path)
            return
        raise

    # --- Detach all managed policies before deletion ---
    attached = iam.list_attached_role_policies(RoleName=role_name)
    for policy in attached['AttachedPolicies']:
        iam.detach_role_policy(RoleName=role_name, PolicyArn=policy['PolicyArn'])
        print(f"Detached policy '{policy['PolicyName']}' from role '{role_name}'.")

    # --- Delete the role ---
    iam.delete_role(RoleName=role_name)
    print(f"IAM Role '{role_name}' deleted.")

    # --- Confirm deletion (no built-in "role_deleted" waiter) ---
    for _ in range(12):
        try:
            iam.get_role(RoleName=role_name)
            time.sleep(5)
        except ClientError as e:
            if e.response['Error']['Code'] == 'NoSuchEntity':
                print(f"IAM Role '{role_name}' deletion confirmed.")
                break
            raise

    config.set('IAM_ROLE', 'ROLE_ARN', '')
    save_config(config, config_path)
    print("Cleared ROLE_ARN in dwh.cfg.")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    config = get_config()
    config_path = get_config_path()

    KEY = config.get('AWS', 'KEY')
    SECRET = config.get('AWS', 'SECRET')

    # --- boto3 clients ---
    iam = boto3.client(
        'iam',
        region_name='us-west-2',
        aws_access_key_id=KEY,
        aws_secret_access_key=SECRET,
    )

    ec2 = boto3.client(
        'ec2',
        region_name='us-west-2',
        aws_access_key_id=KEY,
        aws_secret_access_key=SECRET,
    )

    redshift = boto3.client(
        'redshift',
        region_name='us-west-2',
        aws_access_key_id=KEY,
        aws_secret_access_key=SECRET,
    )

    # --- Tear down resources (reverse order of creation) ---
    delete_redshift_cluster(redshift, config, config_path)
    delete_security_group(ec2, config, config_path)
    delete_iam_role(iam, config, config_path)

    print("\n--- Teardown complete ---")


if __name__ == "__main__":
    main()
