import great_expectations as ge
import sys

partition_date = sys.argv[1]

context = ge.data_context.DataContext()

input_data_path = f'/home/bartosz/workspace/python-playground/great_expectations_test/partition/{partition_date}'
batch = context.get_batch(batch_kwargs={'datasource': 'spark_df', 'reader_method': 'json', 'path': input_data_path},
                          expectation_suite_name="logs.warning",
                          batch_parameters={'partition': partition_date})

results = context.run_validation_operator(
    'action_list_operator',
    assets_to_validate=[batch],
    run_id=f'logs_{partition_date}'
)

print(f'Got result {results}')
