from great_expectations.data_context.store import TupleFilesystemStoreBackend


class IdempotentFileNameStoreBackend(TupleFilesystemStoreBackend):
    def __init__(self, base_directory,
                 filepath_template=None,
                 filepath_prefix=None,
                 filepath_suffix=None,
                 forbidden_substrings=None,
                 platform_specific_separator=True,
                 root_directory=None,
                 fixed_length_key=False):
        super().__init__(base_directory=base_directory,
                         filepath_template=filepath_template, filepath_prefix=filepath_prefix,
                         filepath_suffix=filepath_suffix, forbidden_substrings=forbidden_substrings,
                         platform_specific_separator=platform_specific_separator,
                         root_directory=root_directory, fixed_length_key=fixed_length_key)

    def _set(self, key, value, **kwargs):
        key_items = list(key)
        key_items[len(key)-1] = 'validation_result'

        print(f'Setting {tuple(key_items)}={value}')
        super()._set(tuple(key_items), value, **kwargs)
