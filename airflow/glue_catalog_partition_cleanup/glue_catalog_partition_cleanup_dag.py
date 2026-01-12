import boto3
import logging
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Any

from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule

from airflow import DAG
from lib.helpers.utils import get_environment_run
from glue_catalog_partition_cleanup.tables_config import TABLES_CONFIG


DAG_ID = 'glue_catalog_partition_cleanup'

################################### Env config ##############################################
env = get_environment_run()

RETENTION_DAYS_BUFFER = 30  ## buffer days to add to the retention days to be on the safe side

if env == 'production':
    SLACK_CHANNEL = '#airflow'
else:
    SLACK_CHANNEL = '#airflow_dev'

##########################################################################################

default_args = {
    'owner': 'igalem',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=15),
    'aws_conn_id': 'aws_igalem',
}


def get_tables_config_to_process(**context) -> List[Dict[str, Any]]:
    """
    Validate and return the list of tables to process.
    """
    def validate_table_config(config: Dict[str, Any]) -> bool:
        """Validate that a table config has all required fields."""
        required_fields = ['database', 'table']
        for field in required_fields:
            if field not in config:
                raise ValueError(f"Table config missing required field: {field}")
        return True
    
    processed_configs = []
    
    for config in TABLES_CONFIG:
        validate_table_config(config)
        processed_configs.append(config)
    
    return processed_configs


def get_table_details(**context) -> List[Dict[str, Any]]:
    """
    Get partition keys from Glue table definitions for all configured tables.
    
    Returns:
        List of dictionaries with table info and their partition keys
    """
    ti = context['task_instance']
    
    # Pull validated table configs from previous task
    table_configs = ti.xcom_pull(task_ids='get_tables_configs')
    
    if not table_configs:
        logging.warning("No table configurations found from validate_table_configs task")
        return []
    
    glue_client = boto3.client('glue')
    tables_with_partition_keys = []
    
    for config in table_configs:
        database = config['database']
        table = config['table']
        full_table_name = f"{database}.{table}"
        
        logging.info(f"Getting partition keys for table: {full_table_name}")
        
        try:
            table_response = glue_client.get_table(
                DatabaseName=database,
                Name=table
            )
            
            s3_location = table_response['Table']['StorageDescriptor']['Location']
            s3_parts = s3_location.split('/')
            s3_bucket = s3_parts[2]
            s3_prefix = '/'.join(s3_parts[3:])  # Extract only the prefix path without bucket
            partition_keys = table_response['Table'].get('PartitionKeys', [])
            
            table_info = {
                'database': database,
                'table': table,
                'full_table_name': full_table_name,
                's3_bucket': s3_bucket,
                's3_prefix': s3_prefix,
                'partition_keys': partition_keys
            }
            
            tables_with_partition_keys.append(table_info)
            
            logging.info(f"Found {len(partition_keys)} partition keys for {full_table_name}")
            for key in partition_keys:
                logging.info(f"  - {key['Name']} ({key['Type']})")
                
        except Exception as e:
            logging.error(f"Error getting partition keys for {full_table_name}: {e}")
            raise
    
    return tables_with_partition_keys


def list_old_partitions(**context) -> List[Dict[str, Any]]:
    """
    List old/stale partitions from configured tables based on retention policy.
    
    Returns:
        List of dictionaries with table info and their stale partitions
    """
    ti = context['task_instance']
    
    # Pull table info with retention days from previous task
    tables_info = ti.xcom_pull(task_ids='get_max_retention_for_table')
    
    if not tables_info:
        logging.warning("No table information found from get_max_retention_for_table task")
        return []
    
    glue_client = boto3.client('glue')
    tables_with_stale_partitions = []
    
    for table_info in tables_info:
        database = table_info['database']
        table = table_info['table']
        full_table_name = table_info['full_table_name']
        partition_keys = table_info['partition_keys']
        retention_days = table_info['retention_days']
        
        logging.info(f"Listing partitions for table: {full_table_name}")
        
        try:
            # Calculate stale date based on retention days
            stale_date_calculated = datetime.now(timezone.utc) - timedelta(days=retention_days)
            stale_date = stale_date_calculated.strftime('%Y-%m-%d')
            
            logging.info(f"Stale date for {full_table_name}: {stale_date} (retention days: {retention_days})")
            
            # Get all partitions for the table
            paginator = glue_client.get_paginator('get_partitions')
            response_iterator = paginator.paginate(
                DatabaseName=database,
                TableName=table
            )
            
            stale_partitions = []
            total_partitions = 0
            
            for page in response_iterator:
                for partition in page['Partitions']:
                    total_partitions += 1
                    partition_values = partition['Values']
                    
                    # Extract date from partition based on partition key structure
                    partition_date = extract_partition_date(partition_keys, partition_values)
                    
                    if partition_date and partition_date < stale_date:
                        stale_partitions.append({
                            'values': partition_values
                        })
            
            logging.info(f"Found {len(stale_partitions)} stale partitions out of {total_partitions} total for {full_table_name}")
            
            table_info['stale_partitions'] = stale_partitions
            table_info['total_partitions'] = total_partitions
            table_info['stale_date'] = stale_date
            
            tables_with_stale_partitions.append(table_info)
            
        except Exception as e:
            logging.error(f"Error listing partitions for {full_table_name}: {e}")
            raise
    
    return tables_with_stale_partitions


def extract_partition_date(partition_keys: List[Dict], partition_values: List[str]) -> str:
    """
    Extract date string from partition values based on partition key structure.
    """
    partition_date = ''
    
    # Check if partition keys contain year, month, and day
    for i, key_info in enumerate(partition_keys):
        key_name = key_info['Name']
        
        if i < len(partition_values):
            value = partition_values[i]
            
            if key_name == 'year':
                partition_date += f'{value}'
            elif key_name in ['month', 'day', 'hour', 'minute', 'second']:
                partition_date += f'-{value.zfill(2)}'  # Pad with zero if needed
            elif key_name in ['date', 'dt', 'partition_date', 'impression_date']:  # Other common date partition names
                partition_date = value
                break
    
    return partition_date


def delete_stale_partitions(**context) -> Dict[str, Any]:
    ti = context['task_instance']
    
    tables_info = ti.xcom_pull(task_ids='list_old_partitions')
    
    if not tables_info:
        logging.warning("No table information found from list_old_partitions task")
        return {'deleted_count': 0, 'error_count': 0, 'tables_processed': 0}
    
    glue_client = boto3.client('glue')
    
    total_deleted = 0
    total_errors = 0
    tables_processed = 0
    
    for table_info in tables_info:
        database = table_info['database']
        table = table_info['table']
        full_table_name = table_info['full_table_name']
        stale_partitions = table_info.get('stale_partitions', [])
        
        tables_processed += 1
        
        logging.info(f"Deleting {len(stale_partitions)} stale partitions for table: {full_table_name}")
        
        for partition_info in stale_partitions:
            try:
                partition_values = partition_info['values']
                
                response = glue_client.delete_partition(
                    DatabaseName=database,
                    TableName=table,
                    PartitionValues=partition_values
                )
                
                total_deleted += 1
                logging.info(f"Deleted partition {partition_values} from {full_table_name}")
                
            except Exception as e:
                total_errors += 1
                logging.error(f"Error deleting partition {partition_values} from {full_table_name}: {e}")
    
    summary = {
        'deleted_count': total_deleted,
        'error_count': total_errors,
        'tables_processed': tables_processed
    }
    
    logging.info(f"Partition cleanup summary: {summary}")
    
    return summary


def get_max_retention_for_table(**context) -> List[Dict[str, Any]]:
    """
    Get maximum retention days from S3 lifecycle rules for each table.
    
    Retention priority (in production):
    1. S3 lifecycle rules for the table's prefix (if found)
    2. Manual retention_days from table config (if no S3 rules and if specified)
    3. Error/abort if neither is available
    """
    ti = context['task_instance']
    
    tables_info = ti.xcom_pull(task_ids='get_table_details')
    
    # pull the original table configs to get manual retention if specified
    table_configs = ti.xcom_pull(task_ids='validate_table_configs')
    
    if not tables_info:
        logging.warning("No table information found from get_table_details task")
        return []
    
    manual_retention_lookup = {}
    if table_configs:
        for config in table_configs:
            key = f"{config['database']}.{config['table']}"
            if 'retention_days' in config:
                manual_retention_lookup[key] = config['retention_days']
    
    tables_with_retention = []
    
    for table_info in tables_info:
        full_table_name = table_info['full_table_name']
        s3_bucket = table_info['s3_bucket']
        s3_prefix = table_info['s3_prefix']
        
        logging.info(f"Getting lifecycle rules for table: {full_table_name} (bucket: {s3_bucket}, prefix: {s3_prefix})")
        
        try:
            if env == 'production':
                lifecycle_rules = get_lifecycle_rules_for_prefix(bucket_name=s3_bucket, prefix=s3_prefix)
                
                retentions = []
                for rule in lifecycle_rules:
                    expiration = rule.get('Expiration', {})
                    if 'Days' in expiration:
                        retentions.append(expiration['Days'])
                
                valid_retentions = [r for r in retentions if r is not None]
                
                if valid_retentions:
                    # lifecycle rules exist
                    max_retention = max(valid_retentions)
                    logging.info(f"Found max retention of {max_retention} days from S3 lifecycle rules for {full_table_name}")
                else:
                    # lifecycle rules not found. check for manual retention
                    if full_table_name in manual_retention_lookup:
                        max_retention = manual_retention_lookup[full_table_name]
                        logging.info(f"No S3 lifecycle rules found. Using manual retention of {max_retention} days for {full_table_name}")
                    else:
                        error_msg = (f"No valid retention configuration found for {full_table_name}. "
                                f"No S3 lifecycle rules exist for prefix '{s3_prefix}' and no manual "
                                f"'retention_days' specified in table config. Cannot proceed. "
                                f"Please add S3 lifecycle rules or specify 'retention_days' in the table configuration.")
                        logging.error(error_msg)
                        raise ValueError(error_msg)
            else:
                # for development environment only (deletation will not be performed otherwise)
                max_retention = RETENTION_DAYS_BUFFER
                logging.info(f"Using default retention for development environment: {RETENTION_DAYS_BUFFER} days for {full_table_name}")
            
            # Add retention_days to table_info
            table_info_with_retention = table_info.copy()
            table_info_with_retention['retention_days'] = max_retention + RETENTION_DAYS_BUFFER
            
            tables_with_retention.append(table_info_with_retention)
            
        except Exception as e:
            error_msg = f"Error processing retention for {full_table_name}: {e}"
            logging.error(error_msg)
            raise ValueError(error_msg) from e
    
    return tables_with_retention


def get_lifecycle_rules_for_prefix(bucket_name, prefix):
    """
    Returns all lifecycle rules in a bucket that apply to the given prefix.
    """
    s3 = boto3.client('s3')
    try:
        response = s3.get_bucket_lifecycle_configuration(Bucket=bucket_name)
    except s3.exceptions.ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchLifecycleConfiguration':
            print(f"No lifecycle configuration found for bucket '{bucket_name}'.")
            return []
        else:
            raise
    
    matched_rules = []
    for rule in response.get('Rules', []):
        # Some rules use 'Filter' while older ones use 'Prefix'
        rule_prefix = ''
        if 'Prefix' in rule:
            # Old-style rule with direct Prefix
            rule_prefix = rule['Prefix']
        elif 'Filter' in rule:
            filter_obj = rule['Filter']
            if 'Prefix' in filter_obj:
                # Filter with direct Prefix
                rule_prefix = filter_obj['Prefix']
            elif 'And' in filter_obj and 'Prefix' in filter_obj['And']:
                # Filter with And containing Prefix (e.g., when combined with Tags)
                rule_prefix = filter_obj['And']['Prefix']

        if rule_prefix.startswith(prefix) and 'granica-' not in rule.get('ID', ''):
            logging.info(f"Matched rule: {rule}")
            matched_rules.append(rule)

    return matched_rules



with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    description='Clean up tables stale partitions from Glue catalog based on S3 lifecycle rules retention days',
    schedule_interval="00 04 * * *",
    max_active_runs=1,
    start_date=datetime(2024, 12, 1),
    catchup=False,
    tags=['glue', 'partition-cleanup', 'maintenance']
) as dag:

    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end', trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)

    # Step 1: Validate table configurations
    get_tables_configs = PythonOperator(
        task_id='get_tables_configs',
        python_callable=get_tables_config_to_process,
        do_xcom_push=True
    )
    
    get_table_details_task = PythonOperator(
        task_id='get_table_details',
        python_callable=get_table_details,
        do_xcom_push=True
    )
    
    get_max_retention_for_table_task = PythonOperator(
        task_id='get_max_retention_for_table',
        python_callable=get_max_retention_for_table,
        do_xcom_push=True
    )
    
    list_old_partitions_task = PythonOperator(
        task_id='list_old_partitions',
        python_callable=list_old_partitions,
        do_xcom_push=True
    )
    
    if env == 'production':
        delete_stale_partitions_task = PythonOperator(
            task_id='delete_stale_partitions',
            python_callable=delete_stale_partitions,
            do_xcom_push=True
        )
    else:
        # dummy task for 'developmet'
        delete_stale_partitions_task = EmptyOperator(
            task_id='dummy_delete_stale_partitions',
        )
    

    start >> get_tables_configs >> get_table_details_task >> get_max_retention_for_table_task >> list_old_partitions_task >> delete_stale_partitions_task >> end
