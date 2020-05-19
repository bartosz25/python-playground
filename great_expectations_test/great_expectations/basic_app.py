import great_expectations as ge
from great_expectations.core import ExpectationSuite
from pyspark.sql import SparkSession

#explorer = ge.data_context.ExplorerDataContext()
#print(f'{explorer}')

expectation_suite = ExpectationSuite(
    expectation_suite_name='json_test_expectations',
    expectations=[
        {
            'expectation_type': 'expect_column_values_to_not_be_null',
            'kwargs': {
                'column': 'source'
            }
        },
        # I'm introducing the check on a not existing column on purpose
        # to see what types of errors are returned
        {
            'expectation_type': 'expect_column_to_exist',
            'kwargs': {
                'column': 'website'
            }
        }
    ]
)

spark = SparkSession.builder.master("local[2]")\
    .appName("JSON loader").getOrCreate()
json_dataset = spark.read.json('/home/bartosz/workspace/python-playground/great_expectations_test/input_test.json',
                               lineSep='\n')

context = ge.data_context.DataContext()
batch_kwargs = {'datasource': 'spark_df', 'dataset': json_dataset}

#batch_kwargs = context.build_batch_kwargs('spark_df', 'default')
#print(f'Got batch_kwargs={batch_kwargs}')
batch = context.get_batch({'datasource': 'spark_df', 'reader_method': 'json',
                           'path': '/home/bartosz/workspace/python-playground/great_expectations_test/input_test.json'},
                          expectation_suite,
                          batch_parameters={'description': "I'm testing the batch locally",
                                            'partition': '2020-04-12T03:30:00'})
# BK: As above, the operator's name must be defined in the `great_expectations.yml` file

results = context.run_validation_operator(
    'action_list_operator', assets_to_validate=[batch], run_id='test_run'
)

print(f'Got result {results}')

context.open_data_docs()
