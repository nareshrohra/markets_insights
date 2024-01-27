import os


class EnvironmentSettings:
    Paths = {
        'DataBaseDir': "../../_data",
        'RawDataDir': "raw",
        'ProcessedDataDir': "processed",
        'HistoricalDataDir': "historical",
        'MonthlySuffix': "monthly",
        'AnnualSuffix': "annual",
        'BhavDataDir': "bhavcopy",
        'NseDerivativesDataDir': "nse_derivatives",
        'NseIndicesDataDir': "nse_indices",
        'ManualDataPath': "../manual_data"
    }
    Development = {
        "InstrumentationLevel": 1 | 2 | 4
    }

class Environment:
    def setup(cache_data_base_path: str = None):
        if cache_data_base_path is not None:
            EnvironmentSettings.Paths['DataBaseDir'] = cache_data_base_path

        Environment._setup_cache_dirs()
    
    def _setup_cache_dirs():
        env_paths = EnvironmentSettings.Paths
        
        cur_path = f"{env_paths['DataBaseDir']}"
        if not os.path.exists(cur_path):
            os.mkdir(cur_path)

        for folder in ['RawDataDir', 'ProcessedDataDir']:
            cur_path = f"{env_paths['DataBaseDir']}/{env_paths[folder]}"
            if not os.path.exists(cur_path):
                os.mkdir(cur_path)
        
        for folder in ['BhavDataDir', 'NseIndicesDataDir', 'NseDerivativesDataDir']:
            cur_path = f"{env_paths['DataBaseDir']}/{env_paths['RawDataDir']}/{env_paths[folder]}"
            if not os.path.exists(cur_path):
                os.mkdir(cur_path)

        cur_path = f"{env_paths['DataBaseDir']}/{env_paths['ProcessedDataDir']}/{env_paths['HistoricalDataDir']}"
        if not os.path.exists(cur_path):
            os.mkdir(cur_path)

        