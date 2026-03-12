import boto3
import json

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
# IAM Role
# ---------------------------------------------------------------------------

def create_iam_role(iam, config, config_path):
    """Create an IAM role for Redshift with S3 read-only access.

    Idempotent – if the role already exists it is reused.
    After the role is available the ARN is saved to dwh.cfg.

    Returns:
        str: The IAM Role ARN.
    """
    role_name = config.get('IAM_ROLE', 'ROLE_NAME')

    assume_role_policy = json.dumps({
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": {"Service": "redshift.amazonaws.com"},
                "Action": "sts:AssumeRole"
            }
        ]
    })

    # --- Create the role (skip if it already exists) ---
    try:
        iam.create_role(
            Path='/',
            RoleName=role_name,
            Description="Allows Redshift clusters to call AWS services on your behalf.",
            AssumeRolePolicyDocument=assume_role_policy,
        )
        print(f"IAM Role '{role_name}' created successfully.")
    except ClientError as e:
        if e.response['Error']['Code'] == 'EntityAlreadyExists':
            print(f"IAM Role '{role_name}' already exists. Reusing.")
        else:
            raise

    # --- Wait until the role is available ---
    try:
        iam.get_waiter('role_exists').wait(
            RoleName=role_name,
            WaiterConfig={'Delay': 5, 'MaxAttempts': 12},
        )
        print(f"IAM Role '{role_name}' is now available.")
    except WaiterError as e:
        print(f"Timed out waiting for IAM Role '{role_name}': {e}")
        raise

    # --- Attach S3 read-only policy (idempotent – re-attaching is a no-op) ---
    iam.attach_role_policy(
        RoleName=role_name,
        PolicyArn='arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess',
    )
    print(f"Policy 'AmazonS3ReadOnlyAccess' attached to role '{role_name}'.")

    # --- Retrieve the ARN and persist to config ---
    role_arn = iam.get_role(RoleName=role_name)['Role']['Arn']
    config.set('IAM_ROLE', 'ROLE_ARN', role_arn)
    save_config(config, config_path)
    print(f"Saved IAM Role ARN to dwh.cfg: {role_arn}")

    return role_arn


# ---------------------------------------------------------------------------
# Security Group
# ---------------------------------------------------------------------------

def create_security_group(ec2, config, config_path):
    """Create a VPC security group for Redshift in the default VPC.

    Idempotent – if the security group already exists it is reused.
    Opens TCP port 5439 (Redshift) from 0.0.0.0/0.
    The security group ID is saved to dwh.cfg.

    Returns:
        str: The Security Group ID.
    """
    sg_name = config.get('SECURITY_GROUP', 'SG_NAME')
    db_port = int(config.get('CLUSTER', 'DB_PORT'))

    # --- Find the default VPC ---
    vpcs = ec2.describe_vpcs(Filters=[{'Name': 'isDefault', 'Values': ['true']}])
    if not vpcs['Vpcs']:
        raise RuntimeError("No default VPC found in us-west-2.")
    vpc_id = vpcs['Vpcs'][0]['VpcId']
    print(f"Using default VPC: {vpc_id}")

    # --- Create the security group (skip if it already exists) ---
    try:
        response = ec2.create_security_group(
            GroupName=sg_name,
            Description='Authorize Redshift cluster access',
            VpcId=vpc_id,
        )
        sg_id = response['GroupId']
        print(f"Security Group '{sg_name}' created: {sg_id}")
    except ClientError as e:
        if e.response['Error']['Code'] == 'InvalidGroup.Duplicate':
            existing = ec2.describe_security_groups(
                Filters=[
                    {'Name': 'group-name', 'Values': [sg_name]},
                    {'Name': 'vpc-id', 'Values': [vpc_id]},
                ]
            )
            sg_id = existing['SecurityGroups'][0]['GroupId']
            print(f"Security Group '{sg_name}' already exists: {sg_id}. Reusing.")
        else:
            raise

    # --- Authorize inbound TCP on Redshift port (skip if rule already exists) ---
    try:
        ec2.authorize_security_group_ingress(
            GroupId=sg_id,
            IpPermissions=[
                {
                    'IpProtocol': 'tcp',
                    'FromPort': db_port,
                    'ToPort': db_port,
                    'IpRanges': [{'CidrIp': '0.0.0.0/0', 'Description': 'Redshift access'}],
                }
            ],
        )
        print(f"Ingress rule added: TCP {db_port} from 0.0.0.0/0")
    except ClientError as e:
        if e.response['Error']['Code'] == 'InvalidPermission.Duplicate':
            print(f"Ingress rule for TCP {db_port} already exists. Skipping.")
        else:
            raise

    # --- Persist to config ---
    config.set('SECURITY_GROUP', 'SG_ID', sg_id)
    save_config(config, config_path)
    print(f"Saved Security Group ID to dwh.cfg: {sg_id}")

    return sg_id


# ---------------------------------------------------------------------------
# Redshift Cluster
# ---------------------------------------------------------------------------

def create_redshift_cluster(redshift, config, config_path, role_arn, sg_id):
    """Create a Redshift cluster using parameters from dwh.cfg.

    Idempotent – if the cluster already exists the endpoint is read from it
    instead of creating a new cluster.  Uses a boto3 waiter to block until
    the cluster reaches the ``available`` state, then saves the endpoint
    address to dwh.cfg.

    Returns:
        str: The cluster endpoint address.
    """
    cluster_id = config.get('CLUSTER', 'CLUSTER_IDENTIFIER')
    cluster_type = config.get('CLUSTER', 'CLUSTER_TYPE')
    node_type = config.get('CLUSTER', 'NODE_TYPE')
    num_nodes = int(config.get('CLUSTER', 'NUM_NODES'))
    db_name = config.get('CLUSTER', 'DB_NAME')
    db_user = config.get('CLUSTER', 'DB_USER')
    db_password = config.get('CLUSTER', 'DB_PASSWORD')
    db_port = int(config.get('CLUSTER', 'DB_PORT'))

    # --- Check if the cluster already exists ---
    cluster_exists = False
    try:
        desc = redshift.describe_clusters(ClusterIdentifier=cluster_id)
        cluster = desc['Clusters'][0]
        cluster_exists = True
        status = cluster['ClusterStatus']
        print(f"Redshift cluster '{cluster_id}' already exists (status: {status}).")

        if status == 'available':
            endpoint = cluster['Endpoint']['Address']
            config.set('CLUSTER', 'HOST', endpoint)
            save_config(config, config_path)
            print(f"Cluster endpoint already available: {endpoint}")
            return endpoint
        # If status is 'creating' or similar, fall through to the waiter.
    except ClientError as e:
        if e.response['Error']['Code'] == 'ClusterNotFound':
            pass  # Will create below.
        else:
            raise

    # --- Create the cluster if it does not exist ---
    if not cluster_exists:
        create_kwargs = dict(
            ClusterIdentifier=cluster_id,
            ClusterType=cluster_type,
            NodeType=node_type,
            DBName=db_name,
            MasterUsername=db_user,
            MasterUserPassword=db_password,
            Port=db_port,
            IamRoles=[role_arn],
            VpcSecurityGroupIds=[sg_id],
            PubliclyAccessible=True,
        )
        if cluster_type == 'multi-node':
            create_kwargs['NumberOfNodes'] = num_nodes

        redshift.create_cluster(**create_kwargs)
        print(f"Redshift cluster '{cluster_id}' creation initiated.")

    # --- Wait for the cluster to become available ---
    print(f"Waiting for cluster '{cluster_id}' to become available "
          f"(this may take several minutes)...")
    try:
        redshift.get_waiter('cluster_available').wait(
            ClusterIdentifier=cluster_id,
            WaiterConfig={'Delay': 30, 'MaxAttempts': 60},
        )
        print(f"Redshift cluster '{cluster_id}' is now available.")
    except WaiterError as e:
        print(f"Timed out waiting for Redshift cluster '{cluster_id}': {e}")
        raise

    # --- Retrieve endpoint and persist to config ---
    desc = redshift.describe_clusters(ClusterIdentifier=cluster_id)
    endpoint = desc['Clusters'][0]['Endpoint']['Address']
    config.set('CLUSTER', 'HOST', endpoint)
    save_config(config, config_path)
    print(f"Saved cluster endpoint to dwh.cfg: {endpoint}")

    return endpoint


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

    # --- Provision resources ---
    role_arn = create_iam_role(iam, config, config_path)
    sg_id = create_security_group(ec2, config, config_path)
    endpoint = create_redshift_cluster(redshift, config, config_path, role_arn, sg_id)

    print("\n--- Provisioning complete ---")
    print(f"  IAM Role ARN : {role_arn}")
    print(f"  Security Grp : {sg_id}")
    print(f"  Cluster Host : {endpoint}")


if __name__ == "__main__":
    main()