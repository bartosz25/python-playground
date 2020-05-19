import json
def check_data_validation_results(**context):
    partition_date = context['execution_date']

    input_data_path = f'/home/bartosz/workspace/python-playground/great_expectations_test/' \
                      f'great_expectations/uncommitted/validations/logs/warning/logs_{partition_date}/validation_result.json'
    with open(input_data_path) as validation_result_file:
      validation_result = json.load(validation_result_file)

      if not validation_result['success']:
          raise RuntimeError(f'Validation failed for batch {partition_date}')


check_data_validation_results(execution_date='20200505T0300')